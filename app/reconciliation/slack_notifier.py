"""Slack Block Kit 通知クライアント（消込用）"""

import os
import json
import logging
from typing import Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from .matcher import MatchResult, MATCH_MANUAL, MATCH_NONE, MATCH_COMBINED, MATCH_SPLIT

logger = logging.getLogger(__name__)

# action_id プレフィックス（/webhooks/slack でルーティングに使用）
PREFIX = "recon_"


def _action_value(data: dict) -> str:
    """action の value に格納する JSON（255文字制限のため必要最小限）"""
    return json.dumps(data, ensure_ascii=False)


class ReconciliationSlackNotifier:
    def __init__(self):
        self.client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN", ""))
        self.channel = os.environ.get(
            "RECONCILIATION_CHANNEL_ID",
            os.environ.get("SLACK_INVOICE_CHANNEL_ID", "C0ANE67AU2X"),
        )

    # ─── 内部ユーティリティ ───────────────────────────────────────────

    def _post(self, blocks: list, text: str = "消込通知") -> Optional[str]:
        """Slackにブロックを投稿し、message_ts を返す"""
        try:
            resp = self.client.chat_postMessage(
                channel=self.channel,
                text=text,
                blocks=blocks,
            )
            return resp["ts"]
        except SlackApiError as e:
            logger.error(f"Slack post error: {e}", exc_info=True)
            return None

    def _update(self, ts: str, blocks: list, text: str = "消込通知"):
        """既存メッセージを更新"""
        try:
            self.client.chat_update(
                channel=self.channel,
                ts=ts,
                text=text,
                blocks=blocks,
            )
        except SlackApiError as e:
            logger.error(f"Slack update error: {e}", exc_info=True)

    @staticmethod
    def _divider():
        return {"type": "divider"}

    @staticmethod
    def _section(text: str) -> dict:
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    @staticmethod
    def _header(text: str) -> dict:
        return {"type": "header", "text": {"type": "plain_text", "text": text, "emoji": True}}

    @staticmethod
    def _context(text: str) -> dict:
        return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}

    # ─── 公開API ─────────────────────────────────────────────────────

    def post_start(self, target_month: str, txn_count: int, invoice_count: int) -> Optional[str]:
        """消込バッチ開始通知"""
        blocks = [
            self._header("🔄 入金消込処理を開始しました"),
            self._section(
                f"*対象月:* {target_month}\n"
                f"*入金明細:* {txn_count} 件\n"
                f"*未回収請求書:* {invoice_count} 件"
            ),
            self._context("マッチング結果は順次通知されます"),
        ]
        return self._post(blocks, text=f"消込処理開始: {target_month}")

    def post_auto_match(self, result: MatchResult) -> Optional[str]:
        """自動マッチング結果の承認依頼（承認/却下ボタン付き）"""
        inv = result.primary_invoice
        if not inv:
            return None

        confidence_pct = int(result.confidence * 100)
        inv_url = f"https://secure.freee.co.jp/invoices/{inv.get('id', '')}"
        match_label = _match_type_label(result.match_type)
        warning = "⚠️ " if result.match_type in (MATCH_SPLIT,) else ""

        # action value（txn_id と invoice_idを記録）
        action_base = {
            "txn_id": result.txn_id,
            "invoice_id": str(inv.get("id", "")),
            "invoice_no": inv.get("invoice_number", ""),
            "amount": result.amount,
            "match_type": result.match_type,
        }
        if result.match_type == MATCH_SPLIT and len(result.matched_invoices) > 1:
            action_base["invoice_id2"] = str(result.matched_invoices[1].get("id", ""))

        blocks = [
            self._header(f"{warning}入金消込 — {match_label}（信頼度 {confidence_pct}%）"),
            self._section(
                f"*入金日:* {result.txn_date}　｜　*金額:* ¥{result.amount:,}\n"
                f"*摘要:* {result.memo or '（なし）'}"
            ),
            self._divider(),
            self._section(
                f"*請求書:* <{inv_url}|{inv.get('invoice_number', inv.get('id', ''))}>\n"
                f"*取引先:* {inv.get('partner_name', '—')}\n"
                f"*請求金額:* ¥{int(inv.get('total_amount', 0)):,}　｜　*発行日:* {inv.get('issue_date', '—')}"
            ),
        ]

        # 分割消込の場合は2件目も表示
        if result.match_type == MATCH_SPLIT and len(result.matched_invoices) > 1:
            inv2 = result.matched_invoices[1]
            inv2_url = f"https://secure.freee.co.jp/invoices/{inv2.get('id', '')}"
            blocks.append(self._section(
                f"*請求書2:* <{inv2_url}|{inv2.get('invoice_number', inv2.get('id', ''))}>\n"
                f"*取引先:* {inv2.get('partner_name', '—')}\n"
                f"*請求金額:* ¥{int(inv2.get('total_amount', 0)):,}"
            ))

        blocks.append(self._context(f"マッチ理由: {result.notes}"))
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ 承認・消込実行"},
                    "style": "primary",
                    "action_id": f"{PREFIX}approve",
                    "value": _action_value({**action_base, "action": "approve"}),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ 却下"},
                    "style": "danger",
                    "action_id": f"{PREFIX}reject",
                    "value": _action_value({**action_base, "action": "reject"}),
                },
            ],
        })

        return self._post(blocks, text=f"消込確認: ¥{result.amount:,} ({match_label})")

    def post_manual_match(self, result: MatchResult) -> Optional[str]:
        """手動マッチング選択依頼（候補リスト + 選択ボタン）"""
        blocks = [
            self._header("🔍 入金消込 — 手動選択が必要です"),
            self._section(
                f"*入金日:* {result.txn_date}　｜　*金額:* ¥{result.amount:,}\n"
                f"*摘要:* {result.memo or '（なし）'}"
            ),
            self._divider(),
            self._section("*マッチング候補（自動判定不可）:*"),
        ]

        action_elements = []
        for i, inv in enumerate(result.matched_invoices[:3], 1):
            inv_url = f"https://secure.freee.co.jp/invoices/{inv.get('id', '')}"
            partner = inv.get("partner_name", "—")
            inv_amt = int(inv.get("total_amount", 0))
            inv_no = inv.get("invoice_number", str(inv.get("id", "")))

            blocks.append(self._section(
                f"*候補{i}:* <{inv_url}|{inv_no}> — {partner} — ¥{inv_amt:,}"
            ))
            action_elements.append({
                "type": "button",
                "text": {"type": "plain_text", "text": f"候補{i}を選択"},
                "action_id": f"{PREFIX}manual_{i}",
                "value": _action_value({
                    "txn_id": result.txn_id,
                    "invoice_id": str(inv.get("id", "")),
                    "invoice_no": inv_no,
                    "amount": result.amount,
                    "match_type": "manual",
                    "action": "manual",
                }),
            })

        action_elements.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "⏭️ スキップ"},
            "action_id": f"{PREFIX}skip",
            "value": _action_value({
                "txn_id": result.txn_id,
                "amount": result.amount,
                "action": "skip",
            }),
        })

        blocks.append({"type": "actions", "elements": action_elements})
        return self._post(blocks, text=f"手動消込選択: ¥{result.amount:,}")

    def post_unmatched(self, result: MatchResult) -> Optional[str]:
        """未マッチ通知（情報のみ）"""
        freee_url = (
            f"https://secure.freee.co.jp/wallet_txns?"
            f"company_id={os.environ.get('FREEE_COMPANY_ID', '10397910')}"
        )
        blocks = [
            self._header("⚠️ 入金消込 — 候補なし"),
            self._section(
                f"*入金日:* {result.txn_date}　｜　*金額:* ¥{result.amount:,}\n"
                f"*摘要:* {result.memo or '（なし）'}\n\n"
                f"自動・手動いずれのマッチング候補も見つかりませんでした。\n"
                f"<{freee_url}|freeeで手動確認してください>。"
            ),
            {
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⏭️ スキップ（確認済み）"},
                    "action_id": f"{PREFIX}skip",
                    "value": _action_value({
                        "txn_id": result.txn_id,
                        "amount": result.amount,
                        "action": "skip",
                    }),
                }],
            },
        ]
        return self._post(blocks, text=f"消込候補なし: ¥{result.amount:,}")

    def post_summary(
        self,
        target_month: str,
        total: int,
        auto_done: int,
        pending: int,
        unmatched: int,
        error: int = 0,
    ) -> Optional[str]:
        """消込処理完了サマリー"""
        blocks = [
            self._header("✅ 入金消込処理が完了しました"),
            self._section(
                f"*対象月:* {target_month}\n"
                f"*処理対象:* {total} 件\n"
                f"*自動消込完了:* {auto_done} 件\n"
                f"*承認待ち:* {pending} 件\n"
                f"*未マッチ:* {unmatched} 件"
                + (f"\n*エラー:* {error} 件" if error else "")
            ),
        ]
        if pending > 0:
            blocks.append(self._context(f"⬆️ 上記の承認依頼メッセージを確認して承認してください"))
        return self._post(blocks, text=f"消込完了サマリー: {target_month}")

    def post_error(self, message: str, detail: str = "") -> Optional[str]:
        """エラー通知"""
        blocks = [
            self._header("🚨 消込処理エラー"),
            self._section(f"*エラー:* {message}"),
        ]
        if detail:
            blocks.append(self._context(detail[:300]))
        return self._post(blocks, text=f"消込エラー: {message}")

    # ─── メッセージ更新（ボタン押下後） ────────────────────────────

    def update_approved(self, ts: str, result_text: str, user: str, freee_result: dict):
        """承認後にメッセージを更新"""
        freee_ok = freee_result.get("success", False)
        freee_note = "freee消込完了 ✅" if freee_ok else f"freee消込失敗 ⚠️: {freee_result.get('reason', '')[:60]}"
        self._update(ts, [
            self._header("✅ 消込承認済み"),
            self._section(result_text),
            self._context(f"承認者: {user} | {freee_note}"),
        ], text="消込承認済み")

    def update_rejected(self, ts: str, txn_info: str, user: str):
        """却下後にメッセージを更新"""
        self._update(ts, [
            self._header("❌ 消込却下"),
            self._section(txn_info),
            self._context(f"却下者: {user} — freeeで手動確認してください"),
        ], text="消込却下")

    def update_manual_selected(self, ts: str, txn_info: str, inv_info: str, user: str, freee_result: dict):
        """手動選択後にメッセージを更新"""
        freee_ok = freee_result.get("success", False)
        freee_note = "freee消込完了 ✅" if freee_ok else f"freee消込失敗 ⚠️: {freee_result.get('reason', '')[:60]}"
        self._update(ts, [
            self._header("✅ 手動消込完了"),
            self._section(f"{txn_info}\n→ {inv_info}"),
            self._context(f"担当: {user} | {freee_note}"),
        ], text="手動消込完了")

    def update_skipped(self, ts: str, txn_info: str, user: str):
        """スキップ後にメッセージを更新"""
        self._update(ts, [
            self._header("⏭️ スキップ済み"),
            self._section(txn_info),
            self._context(f"スキップ者: {user}"),
        ], text="スキップ済み")


def _match_type_label(match_type: str) -> str:
    return {
        "exact": "完全一致",
        "invoice_no": "請求書番号一致",
        "keyword": "キーワード一致",
        "tax_diff": "金額近似（消費税差異）",
        "combined": "合算消込",
        "split": "分割消込",
        "manual_candidate": "手動選択",
        "manual": "手動選択済み",
        "unmatched": "未マッチ",
    }.get(match_type, match_type)

# build: 2026-03-26b
