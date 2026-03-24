"""Slack ハンドラー"""

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
        # PDF dataが大きい場合は別途処理
        if len(button_value) > 1800:
            # PDF dataを除外してボタンvalueを作成
            button_value_no_pdf = json.dumps({
                 k : v for k, v in json.loads(button_value).items()
                if k != "pdf_data_b64"
            })
            approve_value = button_value_no_pdf
            reject_value = button_value_no_pdf
            pdf_note = "\n⚠️ PDFサイズが大きないためで、承認後の自動アップロードは刉動で確ってください。"
        else:
            approve_value = button_value
            reject_value = json.dumps({
                "msg_id": invoice_data.get("msg_id"),
                "subject": subject,
            })
            pdf_note = ""

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📄 欢褏請求書 - 承認依頼"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"+請求元*\n{vendor}"},
                    {"type": "mrkdwn", "text": f"*金額*\n{famount_str}"},
                    {"type": "mrkdwn", "text": f"*诋求日*\n{invoice_date}"},
                    {"type": "mrkdwn", "text": f"*請求書番号*\ntinvoice_number or '（記載なし）'}"},
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
                "text": {"type": "mrkdwn", "text": f"*摘要*\n{description}"}
            },
        ]

        if notes or pdf_note:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*備考*\n{notes}{pdf_note}"}
            })

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*$��名*\n{subject}"}
        })

        blocks.append({"type": "divider"})

        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ 承認・freee登録"},
                    "style": "primary",
                    "action_id": "approve_invoice",
                    "value": approve_value[:2000]
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ 却下"},
                    "style": "danger",
                    "action_id": "reject_invoice",
                    "value": (reject_value if len(reject_value) <= 2000 else
                             json.dumps({"msg_id": invoice_data.get("msg_id")}))
                }
            ]
        })

        try:
            resp = self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=blocks,
                text=f"新規請求書: {vendor} {amount_str}"
            )
            logger.info(f"Posted to Slack: ts={resp['ts']}")
        except SlackApiError as e:
            logger.error(f"Slack post error: {e}")
            raise

    async def update_invoice_message(
        self,
        channel: str,
        ts: str,
        status: str,
        user_name: str,
        drive_url: str = None,
        freee_result: dict = None
    ):
        """承認/却下後にSlackメッセージを更新"""
        if status == "approved":
            header = "✅ 承認済み"
            color = "good"
            detail_parts = [f"承認者: {user_name}"]
            if drive_url:
                detail_parts.append(f"<{drive_url}|Google Driveに保存>")
            if freee_result:
                deal_id = freee_result.get("deal", {}).get("id", "")
                if deal_id:
                    detail_parts.append(f"freee取引ID: {deal_id}")
            detail = " | ".join(detail_parts)
        else:
            header = "❌ 却下"
            color = "danger"
            detail = f"却下者: {user_name}"

        new_blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{header}*\n{detail}"}
            }
        ]

        try:
            self.client.chat_update(
                channel=channel,
                ts=ts,
                blocks=new_blocks,
                text=header
            )
        except SlackApiError as e:
            logger.error(f"Slack update error: {e}")
