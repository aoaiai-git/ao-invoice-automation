"""Google Drive ハンドラー"""

import os
import io
import json
import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)


class DriveHandler:
    def __init__(self):
        self._service = None
        self.root_folder_id = os.environ.get(
            "GOOGLE_DRIVE_INVOICE_FOLDER_ID",
            "1-XCSqtbXpw98sPo6xlxpB8ccwagoe2lZ"
        )

    def _get_service(self):
        if self._service:
            return self._service

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not set")

        sa_info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        self._service = build("drive", "v3", credentials=creds)
        return self._service

    def _get_or_create_month_folder(self, service, invoice_date: str) -> str:
        try:
            year_month = invoice_date[:7]
        except Exception:
            year_month = datetime.now().strftime("%Y-%m")

        query = (
            f"name = '{year_month}' and "
            f"'{self.root_folder_id}' in parents and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"trashed = false"
        )
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        if files:
            return files[0]["id"]

        folder_meta = {"name": year_month, "mimeType": "application/vnd.google-apps.folder", "parents": [self.root_folder_id]}
        folder = service.files().create(body=folder_meta, fields="id").execute()
        logger.info(f"Created month folder: {year_month} -> {folder['id']}")
        return folder["id"]

    async def upload_invoice(self, pdf_bytes: bytes, filename: str, invoice_date: str) -> str:
        service = self._get_service()
        month_folder_id = self._get_or_create_month_folder(service, invoice_date)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not filename.endswith(".pdf"):
            filename += ".pdf"
        safe_filename = f"{ts}_{filename}"
        file_meta = {"name": safe_filename, "parents": [month_folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=False)
        uploaded = service.files().create(body=file_meta, media_body=media, fields="id, webViewLink").execute()
        logger.info(f"Uploaded to Drive: {safe_filename} -> {uploaded.get('id')}")
        return uploaded.get("webViewLink", "")
