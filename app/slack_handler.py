"""Slackハンドラー"""

import os
import json
import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackHandler:
    def __init__(self):
        self.client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN", ""))
        self.channel_id = os.environ.get("SLACK_INVOICE_CHANNEL_ID", "C0ANE67AU2X")

    async def post_invoice_approval(self, invoice_data: dict):
        """Slackに請求書承認依頼を投稿"""
        vendor = invoice_data.get("vendor_name", "不明")
        amount = invoice_data.get("amount_incl_tax", 0)
        currency = invoice_data.get("currency", "JPY")
        invoice_date = invoice_data.get("invoice_date", "不明")
        account = invoice_data.get("suggested_account", "雑費")
        description = invoice_data.get("description", "")
        confidence = invoice_data.get("confidence", "low")
        notes = invoice_data.get("notes", "")
        subject = invoice_data.get("subject", "")
        invoice_number = invoice_data.get("invoice_number", "")

        # 確信度アイコン
        conf_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(confidence, "⚪")

        # 金額フォーマット
        if currency == "JPY":
            amount_str = f"¥{amount:,}"
        else:
            amount_str = f"{amount:,.2f} {currency}"

        # 承認ボタンに渡すデータ（PDF込み）
        button_value = json.dumps({
            "msg_id": invoice_data.get("msg_id"),
            "subject": subject,
            "sender": invoice_data.get("sender"),
            "pdf_filename": invoice_data.get("pdf_filename"),
            "pdf_data_b64": invoice_data.get("pdf_data_b64"),
            "vendor_name": vendor,
            "invoice_number": invoice_number,
            "invoice_date": invoice_date,
            "due_date": invoice_data.get("due_date", ""),
            "amount_excl_tax": invoice_data.get("amount_excl_tax", 0),
            "tax_amount": invoice_data.get("tax_amount", 0),
            "amount_incl_tax": amount,
            "currency": currency,
            "description": description,
            "suggested_account": account,
            "suggested_account_id": invoice_data.get("suggested_account_id", 675785162),
        })

        # Slackの制限：ボタンvalueは2000文字まで
        # PDF dataが大きい場合はtruncate
        if len(button_value) > 1990:
            truncated = invoice_data.copy()
            truncated["pdf_data_b64"] = ""
            button_value = json.dumps({
                "msg_id": invoice_data.get("msg_id"),
                "subject": subject,
                "sender": invoice_data.get("sender"),
                "pdf_filename": invoice_data.get("pdf_filename"),
                "pdf_data_b64": "",
                "vendor_name": vendor,
                "invoice_number": invoice_number,
                "invoice_date": invoice_date,
                "due_date": invoice_data.get("due_date", ""),
                "amount_excl_tax": invoice_data.get("amount_excl_tax", 0),
                "tax_amount": invoice_data.get("tax_amount", 0),
                "amount_incl_tax": amount,
                "currency": currency,
                "description": description,
                "suggested_account": account,
                "suggested_account_id": invoice_data.get("suggested_account_id", 675785162),
            })

        # PDF注記
        pdf_note = ""
        if invoice_data.get("pdf_filename"):
            pdf_note = f"\n📎 {invoice_data['pdf_filename']}"
        if not invoice_data.get("pdf_data_b64"):
            pdf_note += "\n⚠️ PDFデータなし"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📄 請求書承認依頼"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*請求元*\n{vendor}"},
                    {"type": "mrkdwn", "text": f"*金額*\n{amount_str}"},
                    {"type": "mrkdwn", "text": f"*請求日*\n{invoice_date}"},
                    {"type": "mrkdwn", "text": f"*請求書番号*\n{invoice_number or '（記載なし）'}"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*提案勘定科目*\n{account}"},
                    {"type": "mrkdwn", "text": f"*AI確信度*\n{conf_icon} {confidence}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*件名*: {subject}{pdf_note}"}
            },
        ]

        if description:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*内容*: {description[:200]}"}
            })

        if notes:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"📝 *備考*: {notes[:300]}"}
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ 承認してfreee登録"},
                    "style": "primary",
                    "action_id": "approve_invoice",
                    "value": button_value[:1990],
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ 却下"},
                    "style": "danger",
                    "action_id": "reject_invoice",
                    "value": json.dumps({"msg_id": invoice_data.get("msg_id"), "subject": subject}),
                },
            ]
        })

        try:
            resp = self.client.chat_postMessage(
                channel=self.channel_id,
                text=f"請求書承認依頼: {vendor} {amount_str}",
                blocks=blocks,
            )
            logger.info(f"Slack message posted: ts={resp['ts']}")
            return resp["ts"]
        except SlackApiError as e:
            logger.error(f"Slack post error: {e}")
            raise

    async def update_invoice_message(self, channel: str, ts: str, status: str, user_name: str, drive_url: str = None, freee_result: dict = None):
        """承認/却下後にSlackメッセージを更新"""
        if status == "approved":
            status_text = "✅ 承認済み・freee登録完了"
            result_parts = [f"承認者: {user_name}"]
            if drive_url:
                result_parts.append(f"<{drive_url}|📁 Googleドライブで開く>")
            if freee_result:
                deal_id = freee_result.get("id", "")
                if deal_id:
                    result_parts.append(f"freee取引ID: {deal_id}")
            result_text = "\n".join(result_parts)
        else:
            status_text = "❌ 却下済み"
            result_text = f"却下者: {user_name}"

        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{status_text}*\n{result_text}"}
            }
        ]
        try:
            self.client.chat_update(
                channel=channel,
                ts=ts,
                text=status_text,
                blocks=blocks,
            )
            logger.info(f"Slack message updated: ts={ts}, status={status}")
        except SlackApiError as e:
            logger.error(f"Slack update error: {e}")
            raise

    async def post_completion_reply(self, channel: str, ts: str, vendor_name: str = None, drive_url: str = None, freee_result: dict = None):
        """承認完了後にスレッドへ完了通知を返信"""
        lines = ["freeeに登録して請求書をGoogleドライブに格納しました。"]
        if vendor_name:
            lines.append(f"取引先: {vendor_name}")
        if drive_url:
            lines.append(f"<{drive_url}|📁 Googleドライブで開く>")
        if freee_result:
            deal_id = freee_result.get("id", "")
            if deal_id:
                lines.append(f"💼 freee取引ID: {deal_id}")

        text = "\n".join(lines)
        try:
            self.client.chat_postMessage(
                channel=channel,
                thread_ts=ts,
                text=text,
            )
            logger.info(f"Posted completion thread reply: ts={ts}")
        except SlackApiError as e:
            logger.error(f"Slack thread reply error: {e}")
            raise
