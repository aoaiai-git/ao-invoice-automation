"""Gmail APIハンドラー"""

import os
import json
import base64
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# Gmail watchのhistoryId管理（メモリ内。Railway再起動でリセット）
_last_history_id: str = ""


class GmailHandler:
    def __init__(self):
        self._service = None

    def _get_service(self):
        if self._service:
            return self._service
        token_json = os.environ.get("GOOGLE_TOKEN_JSON", "")
        if not token_json:
            raise ValueError("GOOGLE_TOKEN_JSON not set")
        token_data = json.loads(token_json)
        creds = Credentials(token=token_data.get("access_token"), refresh_token=token_data.get("refresh_token"), token_uri="https://oauth2.googleapis.com/token", client_id=token_data.get("client_id"), client_secret=token_data.get("client_secret"), scopes=token_data.get("scopes", ["https://www.googleapis.com/auth/gmail.readonly"]))
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        self._service = build("gmail", "v1", credentials=creds)
        return self._service

    async def get_new_invoice_messages(self, history_id: str) -> list:
        global _last_history_id
        service = self._get_service()
        if not _last_history_id:
            _last_history_id = history_id
            logger.info(f"Initialized historyId: {history_id}")
            return []
        try:
            history_resp = service.users().history().list(userId="me", startHistoryId=_last_history_id, historyTypes=["messageAdded"], labelId="INBOX").execute()
        except Exception as e:
            logger.error(f"Gmail history.list error: {e}")
            _last_history_id = history_id
            return []
        _last_history_id = history_id
        history_items = history_resp.get("history", [])
        logger.info(f"Gmail get_history: found {len(history_items)} history items")
        messages = []
        seen_ids = set()
        for item in history_items:
            for msg_added in item.get("messagesAdded", []):
                msg_id = msg_added.get("message", {}).get("id", "")
                if msg_id and msg_id not in seen_ids:
                    seen_ids.add(msg_id)
                    msg_data = await self._fetch_message_with_pdf(service, msg_id)
                    if msg_data:
                        messages.append(msg_data)
        return messages

    async def _fetch_message_with_pdf(self, service, msg_id: str) -> dict | None:
        try:
            msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
        except Exception as e:
            logger.error(f"Failed to fetch message {msg_id}: {e}")
            return None
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        subject = headers.get("subject", "")
        sender = headers.get("from", "")
        pdf_data, pdf_filename = self._find_pdf_attachment(service, msg)
        if not pdf_data:
            logger.info(f"Skipping message {msg_id}: no PDF attachment (subject: {subject})")
            return None
        return {"id": msg_id, "subject": subject, "sender": sender, "pdf_data": pdf_data, "pdf_filename": pdf_filename}

    def _find_pdf_attachment(self, service, msg: dict):
        payload = msg.get("payload", {})
        parts = payload.get("parts", [])
        if not parts:
            if "application/pdf" in payload.get("mimeType", ""):
                data = payload.get("body", {}).get("data", "")
                att_id = payload.get("body", {}).get("attachmentId", "")
                if att_id: return self._get_attachment(service, msg["id"], att_id), "invoice.pdf"
                elif data: return base64.urlsafe_b64decode(data), "invoice.pdf"
            return None, None
        return self._search_parts_for_pdf(service, msg["id"], parts)

    def _search_parts_for_pdf(self, service, msg_id: str, parts: list):
        for part in parts:
            mime_type = part.get("mimeType", "")
            filename = part.get("filename", "")
            if mime_type == "application/pdf" or (filename and filename.lower().endswith(".pdf")):
                att_id = part.get("body", {}).get("attachmentId", "")
                data = part.get("body", {}).get("data", "")
                if att_id: return self._get_attachment(service, msg_id, att_id), filename or "invoice.pdf"
                elif data: return base64.urlsafe_b64decode(data), filename or "invoice.pdf"
            sub_parts = part.get("parts", [])
            if sub_parts:
                result = self._search_parts_for_pdf(service, msg_id, sub_parts)
                if result[0]: return result
        return None, None

    def _get_attachment(self, service, msg_id: str, att_id: str) -> bytes | None:
        try:
            att = service.users().messages().attachments().get(userId="me", messageId=msg_id, id=att_id).execute()
            return base64.urlsafe_b64decode(att["data"])
        except Exception as e:
            logger.error(f"Failed to get attachment: {e}")
            return None
