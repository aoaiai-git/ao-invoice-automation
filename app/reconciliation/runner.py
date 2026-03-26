"""入金消込バッチ — メインオーケストレーター

スケジュール:
  毎月1日 08:05 JST (0 23 L1 * *) — 前月の消込処理
  毎月3日 08:05 JST (5 23 3 * *)  — 前月の残余消込（再実行）
"""

import os
import logging
import uuid
from datetime import date, timedelta
from calendar import monthrange

from .sheets_client import ReconciliationSheetsClient
from .freee_client import FreeeReconcileClient
from .matcher import MatchingEngine, MATCH_MANUAL, MATCH_NONE
from .slack_notifier import ReconciliationSlackNotifier

logger = logging.getLogger(__name__)


def _prev_month_range(today: date = None) -> tuple[str, str, str]:
    """前月の開始日・終了日・ラベルを返す"""
    today = today or date.today()
    first_of_current = today.replace(day=1)
    last_of_prev = first_of_current - timedelta(days=1)
    first_of_prev = last_of_prev.replace(day=1)
    _, last_day = monthrange(last_of_prev.year, last_of_prev.month)
    end = last_of_prev.replace(day=last_day)
    label = first_of_prev.strftime("%Y年%m月")
    return first_of_prev.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), label


async def run_reconciliation(
    start_date: str = None,
    end_date: str = None,
    dry_run: bool = False,
) -> dict:
    """消込処理メイン

    Args:
        start_date: 対象開始日 (YYYY-MM-DD)。省略時は前月1日
        end_date:   対象終了日 (YYYY-MM-DD)。省略時は前月末
        dry_run:    True の場合、Slack通知のみ行い freee消込は実行しない

    Returns:
        {"run_id": ..., "total": ..., "auto_done": ..., "pending": ..., "unmatched": ...}
    """
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"=== 消込バッチ開始: run_id={run_id}, dry_run={dry_run} ===")

    # ─── 日付範囲の決定 ─────────────────────────────────────────────
    if not start_date or not end_date:
        start_date, end_date, target_label = _prev_month_range()
    else:
        target_label = f"{start_date}〜{end_date}"

    logger.info(f"対象期間: {start_date}〜{end_date}")

    # ─── クライアント初期化 ─────────────────────────────────────────
    sheets = ReconciliationSheetsClient()
    freee = FreeeReconcileClient()
    notifier = ReconciliationSlackNotifier()

    stats = {"run_id": run_id, "total": 0, "auto_done": 0, "pending": 0, "unmatched": 0, "error": 0}

    try:
        # ─── シート確認・初期化 ────────────────────────────────────
        sheets.ensure_sheets_exist()

        # ─── データ取得 ─────────────────────────────────────────────
        logger.info("freeeから入金データを取得中...")
        wallet_txns = freee.get_wallet_txns(start_date, end_date, entry_side="income")

        logger.info("freeeから未回収請求書を取得中...")
        invoices = freee.get_unpaid_invoices()

        logger.info("消込マスタをロード中...")
        name_mapping = sheets.get_name_mapping()

        stats["total"] = len(wallet_txns)
        logger.info(f"入金 {len(wallet_txns)} 件 / 未回収請求書 {len(invoices)} 件 / マスタ {len(name_mapping)} 件")

        if not wallet_txns:
            logger.info("入金データが0件のため終了")
            return stats

        # ─── 開始通知 ───────────────────────────────────────────────
        notifier.post_start(target_label, len(wallet_txns), len(invoices))

        # ─── マッチング ─────────────────────────────────────────────
        engine = MatchingEngine(name_mapping)
        results = engine.match_all(wallet_txns, invoices)

        # ─── 結果ごとに処理 ─────────────────────────────────────────
        for result in results:
            try:
                await _process_result(
                    result=result,
                    run_id=run_id,
                    sheets=sheets,
                    freee=freee,
                    notifier=notifier,
                    dry_run=dry_run,
                    stats=stats,
                )
            except Exception as e:
                logger.error(f"処理エラー txn_id={result.txn_id}: {e}", exc_info=True)
                stats["error"] += 1
                notifier.post_error(f"txn_id={result.txn_id} の処理中にエラー", str(e))

        # ─── サマリー通知 ────────────────────────────────────────────
        notifier.post_summary(
            target_label,
            total=stats["total"],
            auto_done=stats["auto_done"],
            pending=stats["pending"],
            unmatched=stats["unmatched"],
            error=stats["error"],
        )

        logger.info(f"=== 消込バッチ完了: {stats} ===")
        return stats

    except Exception as e:
        logger.error(f"消込バッチで致命的エラー: {e}", exc_info=True)
        try:
            notifier.post_error(f"消込バッチで致命的エラー: {e}")
        except Exception:
            pass
        stats["error"] += 1
        return stats


async def _process_result(
    result,
    run_id: str,
    sheets: ReconciliationSheetsClient,
    freee: FreeeReconcileClient,
    notifier: ReconciliationSlackNotifier,
    dry_run: bool,
    stats: dict,
):
    """1件の MatchResult を処理する"""
    inv = result.primary_invoice
    inv_id = str(inv.get("id", "")) if inv else ""
    inv_no = (inv.get("invoice_number", "") if inv else "") or ""
    inv_amt = str(int(inv.get("total_amount", 0)) if inv else 0)

    # Google Sheetsに記録
    record = {
        "run_id": run_id,
        "txn_id": result.txn_id,
        "txn_date": result.txn_date,
        "amount": str(result.amount),
        "memo": result.memo,
        "partner_name": inv.get("partner_name", "") if inv else "",
        "invoice_id": inv_id,
        "invoice_no": inv_no,
        "invoice_amount": inv_amt,
        "match_type": result.match_type,
        "confidence": str(result.confidence),
        "status": "pending_approval",
        "processed_at": "",
        "processed_by": "system",
        "slack_ts": "",
        "notes": result.notes,
    }

    if result.match_type == MATCH_NONE:
        # ─── 未マッチ ──────────────────────────────────────────────
        record["status"] = "unmatched"
        sheets.append_record(record)
        notifier.post_unmatched(result)
        stats["unmatched"] += 1

    elif result.match_type == MATCH_MANUAL:
        # ─── 手動マッチ候補 ────────────────────────────────────────
        record["status"] = "pending_manual"
        sheets.append_record(record)
        ts = notifier.post_manual_match(result)
        if ts:
            sheets.update_record_status(result.txn_id, "pending_manual", slack_ts=ts)
        stats["pending"] += 1

    elif result.is_auto_approvable and not dry_run:
        # ─── 信頼度高 → 自動承認・freee消込実行 ──────────────────
        freee_result = freee.execute_reconciliation(
            invoice_id=int(inv_id) if inv_id else 0,
            amount=result.amount,
            txn_date=result.txn_date,
        )
        record["status"] = "auto_approved"
        record["notes"] += f" | freee: {'OK' if freee_result.get('success') else 'FAILED'}"
        sheets.append_record(record)

        # Slackに自動承認済みとして通知（ボタンなし）
        ts = _post_auto_done(notifier, result, freee_result)
        stats["auto_done"] += 1

    else:
        # ─── 中程度の信頼度 → Slackで人間が承認 ──────────────────
        record["status"] = "pending_approval"
        sheets.append_record(record)
        ts = notifier.post_auto_match(result)
        if ts:
            sheets.update_record_status(result.txn_id, "pending_approval", slack_ts=ts)
        stats["pending"] += 1


def _post_auto_done(notifier, result, freee_result: dict) -> str | None:
    """自動消込完了をSlackに通知（承認ボタンなし）"""
    inv = result.primary_invoice
    freee_ok = freee_result.get("success", False)
    from .slack_notifier import _match_type_label
    try:
        ts = notifier.client.chat_postMessage(
            channel=notifier.channel,
            text=f"消込自動完了: ¥{result.amount:,}",
            blocks=[
                notifier._header(f"✅ 自動消込完了 — {_match_type_label(result.match_type)}"),
                notifier._section(
                    f"*入金日:* {result.txn_date}　｜　*金額:* ¥{result.amount:,}\n"
                    f"*摘要:* {result.memo or '（なし）'}\n"
                    f"*請求書:* {inv.get('invoice_number', inv.get('id', '—')) if inv else '—'} "
                    f"— {inv.get('partner_name', '—') if inv else '—'}"
                ),
                notifier._context(
                    f"{'freee消込完了 ✅' if freee_ok else '⚠️ freee消込失敗 — 手動確認要'} | "
                    f"信頼度: {int(result.confidence * 100)}%"
                ),
            ]
        )
        return ts["ts"]
    except Exception as e:
        logger.warning(f"auto_done post failed: {e}")
        return None
