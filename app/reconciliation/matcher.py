"""7ステップ入金消込マッチングエンジン"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from unicodedata import normalize

logger = logging.getLogger(__name__)


# マッチタイプ定義（信頼度の高い順）
MATCH_EXACT = "exact"             # Step1: 完全一致（金額 + 取引先名）
MATCH_INVOICE_NO = "invoice_no"   # Step3: 金額一致 + 請求書番号
MATCH_KEYWORD = "keyword"         # Step2: 金額一致 + キーワード
MATCH_TAX_DIFF = "tax_diff"       # Step4: 金額±消費税 + 取引先
MATCH_COMBINED = "combined"       # Step5: 複数入金→1請求書
MATCH_SPLIT = "split"             # Step6: 1入金→複数請求書
MATCH_MANUAL = "manual_candidate" # Step7: 手動選択候補
MATCH_NONE = "unmatched"          # マッチ不可

# 自動承認可能なマッチタイプ（信頼度高）
AUTO_APPROVABLE = {MATCH_EXACT, MATCH_INVOICE_NO, MATCH_KEYWORD}
# Slack確認が必要なマッチタイプ
NEEDS_CONFIRM = {MATCH_TAX_DIFF, MATCH_COMBINED, MATCH_SPLIT}


@dataclass
class MatchResult:
    txn: dict
    match_type: str
    confidence: float          # 0.0〜1.0
    matched_invoices: list[dict] = field(default_factory=list)
    notes: str = ""
    keyword_matched: str = ""  # Step2で一致したキーワード

    # ─── ショートカットプロパティ ─────────────────────────────────
    @property
    def txn_id(self) -> str:
        return str(self.txn.get("id", ""))

    @property
    def amount(self) -> int:
        return int(self.txn.get("amount", 0))

    @property
    def memo(self) -> str:
        return self.txn.get("description", "") or self.txn.get("memo", "") or ""

    @property
    def txn_date(self) -> str:
        return self.txn.get("date", "")

    @property
    def is_auto_approvable(self) -> bool:
        return self.match_type in AUTO_APPROVABLE and self.confidence >= 0.85

    @property
    def needs_slack_confirm(self) -> bool:
        return self.match_type in NEEDS_CONFIRM or (
            self.match_type in AUTO_APPROVABLE and self.confidence < 0.85
        )

    @property
    def primary_invoice(self) -> Optional[dict]:
        return self.matched_invoices[0] if self.matched_invoices else None


class MatchingEngine:
    def __init__(self, name_mapping: list[dict] = None):
        """
        Args:
            name_mapping: Sheetsから取得した取引先→キーワードマッピング
                          [{"freee_partner_name": "...", "memo_keywords": "kw1,kw2"}, ...]
        """
        self.name_mapping = name_mapping or []
        # partner_name → keywords リストに変換
        self._kw_map: dict[str, list[str]] = {}
        for row in self.name_mapping:
            name = row.get("freee_partner_name", "").strip()
            kws_raw = row.get("memo_keywords", "")
            if name and kws_raw:
                kws = [k.strip() for k in kws_raw.split(",") if k.strip()]
                self._kw_map[name] = kws

    # ─── メインエントリポイント ───────────────────────────────────────

    def match_all(self, wallet_txns: list[dict], invoices: list[dict]) -> list[MatchResult]:
        """全入金に対してマッチングを実行し、MatchResultリストを返す"""
        results: list[MatchResult] = []
        used_invoice_ids: set[str] = set()   # 重複マッチ防止

        # Step 1〜4,6: 1対1マッチング
        for txn in wallet_txns:
            available = [i for i in invoices if str(i.get("id")) not in used_invoice_ids]
            result = self._match_one(txn, available)
            results.append(result)

            if result.match_type not in (MATCH_MANUAL, MATCH_NONE):
                for inv in result.matched_invoices:
                    used_invoice_ids.add(str(inv.get("id")))

        # Step 5: 合算マッチ（複数の未マッチ入金 → 1請求書）
        unmatched_results = [r for r in results if r.match_type == MATCH_NONE]
        remaining_invoices = [i for i in invoices if str(i.get("id")) not in used_invoice_ids]
        if len(unmatched_results) >= 2:
            combined = self._combined_match(unmatched_results, remaining_invoices)
            if combined:
                # 合算マッチしたものを results の中身を差し替え
                combined_ids = {r.txn_id for r in combined}
                results = [r if r.txn_id not in combined_ids else next(c for c in combined if c.txn_id == r.txn_id)
                           for r in results]
                for r in combined:
                    for inv in r.matched_invoices:
                        used_invoice_ids.add(str(inv.get("id")))

        logger.info(
            f"Matching done: "
            f"exact={sum(1 for r in results if r.match_type==MATCH_EXACT)}, "
            f"keyword={sum(1 for r in results if r.match_type==MATCH_KEYWORD)}, "
            f"invoice_no={sum(1 for r in results if r.match_type==MATCH_INVOICE_NO)}, "
            f"tax_diff={sum(1 for r in results if r.match_type==MATCH_TAX_DIFF)}, "
            f"combined={sum(1 for r in results if r.match_type==MATCH_COMBINED)}, "
            f"split={sum(1 for r in results if r.match_type==MATCH_SPLIT)}, "
            f"manual={sum(1 for r in results if r.match_type==MATCH_MANUAL)}, "
            f"unmatched={sum(1 for r in results if r.match_type==MATCH_NONE)}"
        )
        return results

    # ─── Step 1〜4, 6 ─────────────────────────────────────────────────

    def _match_one(self, txn: dict, invoices: list[dict]) -> MatchResult:
        """1入金に対してStep1〜4,6を順に試みる"""
        amount = int(txn.get("amount", 0))
        memo = self._normalize(txn.get("description", "") or txn.get("memo", ""))

        # Step 1: 完全一致（金額一致 + 取引先名がメモに含まれる）
        for inv in invoices:
            if self._amount_eq(amount, inv) and self._partner_in_memo(inv, memo):
                return MatchResult(txn=txn, match_type=MATCH_EXACT, confidence=1.0,
                                   matched_invoices=[inv], notes="金額・取引先名が完全一致")

        # Step 2: 金額一致 + name_mappingキーワードマッチ
        for inv in invoices:
            if self._amount_eq(amount, inv):
                kw = self._keyword_match(inv, memo)
                if kw:
                    return MatchResult(txn=txn, match_type=MATCH_KEYWORD, confidence=0.92,
                                       matched_invoices=[inv], keyword_matched=kw,
                                       notes=f"金額一致 + キーワード「{kw}」一致")

        # Step 3: 金額一致 + 請求書番号がメモに含まれる
        for inv in invoices:
            if self._amount_eq(amount, inv) and self._invoice_no_in_memo(inv, memo):
                return MatchResult(txn=txn, match_type=MATCH_INVOICE_NO, confidence=0.95,
                                   matched_invoices=[inv], notes="金額一致 + 請求書番号マッチ")

        # Step 4: 金額±消費税の差異 + 取引先マッチ
        for inv in invoices:
            if self._amount_tax_tolerant(amount, inv) and self._partner_in_memo(inv, memo):
                inv_amt = self._inv_amount(inv)
                return MatchResult(txn=txn, match_type=MATCH_TAX_DIFF, confidence=0.78,
                                   matched_invoices=[inv],
                                   notes=f"金額近似（消費税差異）+ 取引先一致: 入金={amount:,}, 請求={inv_amt:,}")

        # Step 6: 分割マッチ（1入金 = 複数請求書の合計）
        split = self._split_match(txn, invoices, amount, memo)
        if split:
            return split

        # Step 7: 手動マッチ候補（スコアでTop3）
        candidates = self._score_candidates(txn, invoices, amount, memo)
        if candidates:
            top3 = sorted(candidates, key=lambda x: x[1], reverse=True)[:3]
            return MatchResult(txn=txn, match_type=MATCH_MANUAL, confidence=top3[0][1],
                               matched_invoices=[c[0] for c in top3],
                               notes="自動マッチ不可 — 手動選択が必要")

        return MatchResult(txn=txn, match_type=MATCH_NONE, confidence=0.0,
                           matched_invoices=[], notes="マッチング候補なし")

    # ─── Step 5: 合算マッチ ──────────────────────────────────────────

    def _combined_match(
        self,
        unmatched: list[MatchResult],
        invoices: list[dict],
    ) -> list[MatchResult]:
        """複数の未マッチ入金の合計 = 1請求書金額になるか確認"""
        updated: list[MatchResult] = []
        used_txn_ids: set[str] = set()

        for inv in invoices:
            inv_amt = self._inv_amount(inv)
            if inv_amt <= 0:
                continue

            # 2入金の組み合わせ
            for i, r1 in enumerate(unmatched):
                if r1.txn_id in used_txn_ids:
                    continue
                for r2 in unmatched[i + 1:]:
                    if r2.txn_id in used_txn_ids:
                        continue
                    if r1.amount + r2.amount == inv_amt:
                        note = f"合算消込: {r1.amount:,}+{r2.amount:,}={inv_amt:,}"
                        r1.match_type = MATCH_COMBINED
                        r1.confidence = 0.80
                        r1.matched_invoices = [inv]
                        r1.notes = note + " (1/2)"
                        r2.match_type = MATCH_COMBINED
                        r2.confidence = 0.80
                        r2.matched_invoices = [inv]
                        r2.notes = note + " (2/2)"
                        updated.extend([r1, r2])
                        used_txn_ids.update([r1.txn_id, r2.txn_id])
                        break
                else:
                    continue
                break

        return updated

    # ─── ユーティリティ ──────────────────────────────────────────────

    @staticmethod
    def _normalize(text: str) -> str:
        """全角→半角、カナ正規化"""
        return normalize("NFKC", text or "")

    def _inv_amount(self, inv: dict) -> int:
        return int(inv.get("total_amount", 0) or inv.get("amount", 0) or 0)

    def _amount_eq(self, amount: int, inv: dict) -> bool:
        return amount == self._inv_amount(inv)

    def _amount_tax_tolerant(self, amount: int, inv: dict) -> bool:
        """金額が消費税差異（8%/10%）の範囲内かどうか"""
        inv_amt = self._inv_amount(inv)
        if inv_amt == 0:
            return False
        ratio = amount / inv_amt
        return 0.88 <= ratio <= 1.12

    def _partner_in_memo(self, inv: dict, memo: str) -> bool:
        """請求書の取引先名がメモ欄に含まれるか（部分一致）"""
        partner = self._normalize(
            inv.get("partner_name", "") or inv.get("partner_display_name", "") or ""
        )
        if not partner or not memo:
            return False
        # 名前を単語に分割して個別マッチ（株式会社などを除外）
        tokens = re.split(r"[\s　（）()株式会社有限会社合同会社]", partner)
        tokens = [t for t in tokens if len(t) >= 2]
        return any(t in memo for t in tokens)

    def _keyword_match(self, inv: dict, memo: str) -> Optional[str]:
        """name_mappingのキーワードがメモに含まれるか"""
        partner = inv.get("partner_name", "") or ""
        keywords = self._kw_map.get(partner, [])
        for kw in keywords:
            if kw and self._normalize(kw) in memo:
                return kw
        return None

    def _invoice_no_in_memo(self, inv: dict, memo: str) -> bool:
        """請求書番号がメモ欄に含まれるか"""
        inv_no = inv.get("invoice_number", "") or inv.get("title", "") or ""
        return bool(inv_no) and self._normalize(inv_no) in memo

    def _split_match(
        self,
        txn: dict,
        invoices: list[dict],
        total: int,
        memo: str,
    ) -> Optional[MatchResult]:
        """1入金 = 複数請求書の合計かチェック（2件まで）"""
        for i, inv1 in enumerate(invoices):
            for inv2 in invoices[i + 1:]:
                if self._inv_amount(inv1) + self._inv_amount(inv2) == total:
                    return MatchResult(
                        txn=txn,
                        match_type=MATCH_SPLIT,
                        confidence=0.82,
                        matched_invoices=[inv1, inv2],
                        notes=(
                            f"分割消込: {self._inv_amount(inv1):,}+{self._inv_amount(inv2):,}"
                            f"={total:,}"
                        )
                    )
        return None

    def _score_candidates(
        self,
        txn: dict,
        invoices: list[dict],
        amount: int,
        memo: str,
    ) -> list[tuple[dict, float]]:
        """マッチ失敗時のスコアリングでTop候補を返す"""
        candidates = []
        for inv in invoices:
            inv_amt = self._inv_amount(inv)
            score = 0.0

            # 金額の近さ（比率）
            if inv_amt > 0 and amount > 0:
                ratio = min(amount, inv_amt) / max(amount, inv_amt)
                score += ratio * 0.55

            # 取引先名のメモ部分一致
            if self._partner_in_memo(inv, memo):
                score += 0.35

            # キーワード一致
            if self._keyword_match(inv, memo):
                score += 0.20

            if score >= 0.3:
                candidates.append((inv, round(score, 3)))

        return candidates
