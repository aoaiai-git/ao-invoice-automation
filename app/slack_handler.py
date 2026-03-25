"""Slack ãƒãƒ³ãƒ‰ãƒ©ãƒ¼"""

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
        """Slackã«è«‹æ±‚æ›¸æ‰¿èªä¾é ¼ã‚’æŠ•ç¨¿"""
        vendor = invoice_data.get("vendor_name", "ä¸æ˜Ž")
        amount = invoice_data.get("amount_incl_tax", 0)
        currency = invoice_data.get("currency", "JPY")
        invoice_date = invoice_data.get("invoice_date", "ä¸æ˜Ž")
        account = invoice_data.get("suggested_account", "é›‘è²»")
        description = invoice_data.get("description", "")
        confidence = invoice_data.get("confidence", "low")
        notes = invoice_data.get("notes", "")
        subject = invoice_data.get("subject", "")
        invoice_number = invoice_data.get("invoice_number", "")

        # ç¢ºä¿¡åº¦ã‚¢ã‚¤ã‚³ãƒ³
        conf_icon = {"high": "ðŸŸ¢", "medium": "ðŸŸ¡", "low": "ðŸ”´"}.get(confidence, "âšª")

        # é‡‘é¡ãƒ•ã‚©ãƒ¼ãƒžãƒƒãƒˆ
        if currency == "JPY":
            amount_str = f"Â¥{amount:,}"
        else:
            amount_str = f"{amount:,.2f} {currency}"

        # æ‰¿èªãƒœã‚¿ãƒ³ã«æ¸¡ã™ãƒ‡ãƒ¼ã‚¿ï¼ˆPDFè¾¼ã¿ï¼‰
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

        # Slackã®åˆ¶é™ï¼šãƒœã‚¿ãƒ³valueã¯2000æ–‡å­—ã¾ã§
        # PDF dataãŒå¤§ãã„å ´åˆã¯åˆ¥é€”å‡¦ç†
        if len(button_value) > 1800:
            # PDF dataã‚’é™¤å¤–ã—ã¦ãƒœã‚¿ãƒ³valueã‚’ä½œæˆ
            button_value_no_pdf = json.dumps({
                 k : v for k, v in json.loads(button_value).items()
                if k != "pdf_data_b64"
            })
            approve_value = button_value_no_pdf
            reject_value = button_value_no_pdf
            pdf_note = "\nâš ï¸ PDFã‚µã‚¤ã‚ºãŒå¤§ããªã„ãŸã‚ã§ã€æ‰¿èªå¾Œã®è‡ªå‹•ã‚¢ãƒƒãƒ—ãƒ­ãƒ¼ãƒ‰ã¯åˆ‰å‹•ã§ç¢ºã£ã¦ãã ã•ã„ã€‚"
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
                    "text": "ðŸ“„ æ¬¢è¤è«‹æ±‚æ›¸ - æ‰¿èªä¾é ¼"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*è«‹æ±‚å…ƒ*\n{vendor}"},
                    {"type": "mrkdwn", "text": f"*é‡‘é¡*\n{amount_str}"},
                    {"type": "mrkdwn", "text": f"*è¯‹æ±‚æ—¥*\n{invoice_date}"},
                    {"type": "mrkdwn", "text": f"*è«‹æ±‚æ›¸ç•ªå·*\{invoice_number or 'ï¼ˆè¨˜è¼‰ãªã—ï¼‰'}"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*ææ¡ˆå‹˜å®šç§‘ç›®*\n{account}"},
                    {"type": "mrkdwn", "text": f"*AIç¢ºä¿¡åº¦*\n{conf_icon} {confidence}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*æ‘˜è¦*\n{description}"}
            },
        ]

        if notes or pdf_note:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*å‚™è€ƒ*\n{notes}{pdf_note}"}
            })

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*$»¶å*\n{subject}"}
        })

        blocks.append({"type": "divider"})

        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "âœ… æ‰¿èªãƒ»freeeç™»éŒ²"},
                    "style": "primary",
                    "action_id": "approve_invoice",
                    "value": approve_value[:2000]
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "âŒ å´ä¸‹"},
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
                text=f"æ–°è¦è«‹æ±‚æ›¸: {vendor} {amount_str}"
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
        """æ‰¿èª/å´ä¸‹å¾Œã«Slackãƒ¡ãƒƒã‚»ãƒ¼ã‚¸ã‚’æ›´æ–°"""
        if status == "approved":
            header = "âœ… æ‰¿èªæ¸ˆã¿"
            color = "good"
            detail_parts = [f"æ‰¿èªè€…: {user_name}"]
            if drive_url:
                detail_parts.append(f"<{drive_url}|Google Driveã«ä¿å­˜>")
            if freee_result:
                deal_id = freee_result.get("deal", {}).get("id", "")
                if deal_id:
                    detail_parts.append(f"freeeå–å¼•ID: {deal_id}")
            detail = " | ".join(detail_parts)
        else:
            header = "âŒ å´ä¸‹"
            color = "danger"
            detail = f"å´ä¸‹è€…: {user_name}"

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
