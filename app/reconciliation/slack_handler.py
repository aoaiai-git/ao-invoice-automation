"""消込Slackインタラクションハンドラー

action_id プレフィックス "recon_" のボタン押下を処理する。
app/main.py の /webhooks/slack から呼び出される。
"""

import json
import logging
from typing import Optional

from .sheets_client import ReconciliationSheetsClient
from .freee_client import FreeeReconcileClient
from .slack_notifier import ReconciliationSlackNotifier

logger = logging.getLogger(__name__)


async def handle_reconciliation_action(
    action_id: str,
    value_data: dict,
    slack_payload: dict,
    user_name: str,
):
    """消込関連のSlackボタン操作を処理

    Args:
        action_id:     "recon_approve" / "recon_reject" / "recon_manual_N" / "recon_skip"
        value_data:    action の value フィールドを JSON デコードしたもの
        slack_payload: Slack 全体 payload
        user_name:     操作したユーザー名
    """
    txn_id = value_data.get("txn_id", "")
    invoice_id_str = value_data.get("invoice_id", "")
    invoice_no = value_data.get("invoice_no", "")
    amount = int(value_data.get("amount", 0))
    action = value_data.get("action", "")

    # 元メッセージの ts を取得（更新用）
    message_ts: Optional[str] = slack_payload.get("message", {}).get("ts")
    channel_id: Optional[str] = slack_payload.get("channel", {}).get("id")

    sheets = ReconciliationSheetsClient()
    freee = FreeeReconcileClient()
    notifier = ReconciliationSlackNotifier()
    if channel_id:
        notifier.channel = channel_id  # 応答は同じチャンネルへ

    logger.info(f"Reconciliation action: {action_id}, txn_id={txn_id}, by={user_name}")

    if action_id == "recon_approve":
        await _handle_approve(txn_id, invoice_id_str, invoice_no, amount, value_data,
  #                            user_name, message_ts, sheets, freee, notifier)

    elif action_id == "recon_reject":
        await _handle_reject(txn_id, amount, user_name, message_ts, sheets, notifier)

    elif action_id in ("recon_manual_1", "recon_manual_2", "recon_manual_3"):
        await _handle_manual(txn_id, invoice_id_str, invoice_no, amount, value_data,
                              user_name, message_ts, sheets, freee, notifier)

    elif action_id == "recon_skip":
        await _handle_skip(txn_id, amount, user_name, message_ts, sheets, notifier)

    else:
        logger.warning(f"Unknown reconciliation action_id: {action_id}")


# ─── 承認 ────────────────────────────────────────────────────────────

async def _handle_approve(
    txn_id: str,
    invoice_id_str: str,
    invoice_no: str,
    amount: int,
    value_data: dict,
    user_name: str,
    ts: Optional[str],
    sheets: ReconciliationSheetsClient,
    freee: FreeeReconcileClient,
    notifier: ReconciliationSlackNotifier,
):
    """承認ボタン押下: freee消込実行 + Sheets更新 + Slackメッセージ更新"""
    try:
        # 消込記録からtxn日付を取得
        record, _ = sheets.get_record_by_txn_id(txn_id)
        txn_date = record.get("txn_date", "") if record else ""

        # freee消込実行
        freee_result = {"success": False, "reason": "invoice_id不明"}
        if invoice_id_str:
            freee_result = freee.execute_reconciliation(
                invoice_id=int(invoice_id_str),
                amount=amount,
                txn_date=txn_date,
            )
            # 分割消込の2件目
            invoice_id2 = value_data.get("invoice_id2", "")
            if invoice_id2 and freee_result.get("success"):
                freee_result2 = freee.execute_reconciliation(
                    invoice_id=int(invoice_id2),
                    amount=amount,  # NOTE: 合算の場合は各金額を計算する必要あり
                    txn_date=txn_date,
                )
                if not freee_result2.get("success"):
                    freee_result["success"] = False
                    freee_result["reason"] = f"2件目消込失敗: {freee_result2.get('reason', '')}"

        # Sheets更新
        notes = f"freee: {'OK' if freee_result.get('success') else 'FAILED: ' + freee_result.get('reason', '')[:60]}"
        sheets.update_record_status(txn_id, "approved", processed_by=user_name, notes=notes)

        # Slackメッセージ更新
        if ts:
            result_text = (
                f"*入金:* ¥{amount:,}　→　*請求書:* {invoice_no or invoice_id_str or '—'}"
            )
            notifier.update_approved(ts, result_text, user_name, freee_result)
        logger.info(f"Approved: txn_id={txn_id}, freee={freee_result.get('success')}")

    except Exception as e:
        logger.error(f"Approval error txn_id={txn_id}: {e}", exc_info=True)
        if ts:
            notifier._update(ts, [
                notifier._header("❌ 承認処理中にエラーが発生しました"),
                notifier._section(f"txn_id={txn_id}\nエラー: {str(e)[:100]}"),
            ])


# ─── 却下 ────────────────────────────────────────────────────────────

async def _handle_reject(
    txn_id: str,
    amount: int,
    user_name: str,
    ts: Optional[str],
    sheets: ReconciliationSheetsClient,
    notifier: ReconciliationSlackNotifier,
):
    """却下ボタン押下: Sheets更新 + Slackメッセージ更新"""
    try:
        sheets.update_record_status(txn_id, "rejected", processed_by=user_name,
                                    notes="Slackで却下")
        if ts:
            notifier.update_rejected(ts, f"*入金金額:* ¥{amount:,}", user_name)
        logger.info(f"Rejected: txn_id={txn_id}")
    except Exception as e:
        logger.error(f"Rejection error txn_id={txn_id}: {e}", exc_info=True)


# ─── 手動選択 ─────────────────────────────────────────────────────────

async def _handle_manual(
    txn_id: str,
    invoice_id_str: str,
    invoice_no: str,
    amount: int,
    value_data: dict,
    user_name: str,
    ts: Optional[str],
    sheets: ReconciliationSheetsClient,
    freee: FreeeReconcileClient,
    notifier: ReconciliationSlackNotifier,
):
    """手動選択ボタン押下: freee消込実行 + Sheets更新 + Slackメッセージ更新"""
    try:
        # 消込記録からtxn日付を取得
        record, _ = sheets.get_record_by_txn_id(txn_id)
        txn_date = record.get("txn_date", "") if record else ""

        # Sheetsの請求書情報を更新
        sheets.update_invoice_for_record(
            txn_id=txn_id,
            invoice_id=invoice_id_str,
            invoice_no=invoice_no,
            invoice_amount=str(amount),
        )

        # freee消込実行
        freee_result = {"success": False, "reason": "invoice_id不明"}
        if invoice_id_str:
            freee_result = freee.execute_reconciliation(
                invoice_id=int(invoice_id_str),
                amount=amount,
                txn_date=txn_date,
            )

        # ステータス更新
        notes = f"手動選択 | freee: {'OK' if freee_result.get('success') else 'FAILED: ' + freee_result.get('reason', '')[:60]}"
        sheets.update_record_status(txn_id, "manually_approved", processed_by=user_name, notes=notes)

        # Slackメッセージ更新
        if ts:
            inv_info = f"請求書: {invoice_no or invoice_id_str or '—'}"
            txn_info = f"*入金:* ¥{amount:,}"
            notifier.update_manual_selected(ts, txn_info, inv_info, user_name, freee_result)

        logger.info(f"Manual: txn_id={txn_id}, invoice={invoice_id_str}, freee={freee_result.get('success')}")
    except Exception as e:
        logger.error(f"Manual selection error txn_id={txn_id}: {e}", exc_info=True)


# ─── スキップ ─────────────────────────────────────────────────────────

async def _handle_skip(
    txn_id: str,
    amount: int,
    user_name: str,
    ts: Optional[str],
    sheets: ReconciliationSheetsClient,
    notifier: ReconciliationSlackNotifier,
):
    """スキップボタン押下: Sheets更新 + Slackメッセージ更新"""
    try:
        sheets.update_record_status(txn_id, "skipped", processed_by=user_name, notes="Slackでスキップ")
        if ts:
            notifier.update_skipped(ts, f"*入金金額:* ¥{amount:,}", user_name)
        logger.info(f"Skipped: txn_id={txn_id}")
    except Exception as e:
        logger.error(f"Skip error txn_id={txn_id}: {e}", exc_info=True)
