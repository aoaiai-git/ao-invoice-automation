"""Google Drive ハンドラー"""
import os
import io
import re
import json
import base64
import logging
from datetime import datetime
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)


class DriveHandler:
    def __init__(self):
        self._service = None
        self._folder_shared = False
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
        sa_email = sa_info.get("client_email", "")

        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        self._service = build("drive", "v3", credentials=creds)

        # 初回のみフォルダ共有を自動設定
        if sa_email and not self._folder_shared:
            self._ensure_folder_shared(sa_email)
            self._folder_shared = True

        return self._service

    def _ensure_folder_shared(self, service_account_email: str) -> bool:
        """
        ユーザーのOAuth認証情報を使って、サービスアカウントに
        ルートフォルダへの書き込みアクセス権を自動付与する。
        GOOGLE_TOKEN_JSON に drive スコープがある場合のみ成功する。
        """
        try:
            token_json = os.environ.get("GOOGLE_TOKEN_JSON", "")
            if not token_json:
                logger.warning("GOOGLE_TOKEN_JSON not set. Cannot auto-share Drive folder.")
                return False

            try:
                token_data = json.loads(token_json)
            except (json.JSONDecodeError, ValueError):
                token_data = json.loads(base64.b64decode(token_json).decode("utf-8"))

            user_creds = Credentials(
                token=token_data.get("token") or token_data.get("access_token"),
                refresh_token=token_data.get("refresh_token"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=token_data.get("client_id"),
                client_secret=token_data.get("client_secret"),
                scopes=token_data.get("scopes", ["https://www.googleapis.com/auth/drive"]),
            )

            if user_creds.expired and user_creds.refresh_token:
                user_creds.refresh(Request())

            user_drive = build("drive", "v3", credentials=user_creds)

            # 既存パーミッションを確認
            perms = user_drive.permissions().list(
                fileId=self.root_folder_id,
                fields="permissions(id,emailAddress,role)",
                supportsAllDrives=True,
            ).execute()
            existing = [p.get("emailAddress", "") for p in perms.get("permissions", [])]

            if service_account_email in existing:
                logger.info(f"Drive folder already shared with {service_account_email}")
                return True

            # サービスアカウントに writer 権限を付与
            user_drive.permissions().create(
                fileId=self.root_folder_id,
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": service_account_email,
                },
                sendNotificationEmail=False,
                supportsAllDrives=True,
            ).execute()
            logger.info(f"Auto-shared Drive folder with {service_account_email}")
            return True

        except Exception as e:
            logger.warning(
                f"Could not auto-share Drive folder: {e}. "
                "Please share the 'GOOGLE_DRIVE_INVOICE_FOLDER_ID' folder manually "
                f"with {service_account_email} (Editor role)."
            )
            return False

    def _sanitize_filename(self, name: str) -> str:
        """ファイル名として使用できない文字を除去"""
        sanitized = re.sub(r'[\\/:*?"<>|]', '_', name)
        sanitized = re.sub(r'[\s_]+', '_', sanitized)
        return sanitized.strip('_').strip() or "invoice"

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

        folder_meta = {
            "name": year_month,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [self.root_folder_id],
        }
        folder = service.files().create(body=folder_meta, fields="id").execute()
        logger.info(f"Created month folder: {year_month} -> {folder['id']}")
        return folder["id"]

    async def upload_invoice(
        self,
        pdf_bytes: bytes,
        filename: str,
        invoice_date: str,
        vendor_name: str = None,
    ) -> str:
        """
        PDFをGoogle Driveにアップロードする。
        保存先: 請求書/{YYYY-MM}/{業者名}.pdf
        vendor_name が指定されている場合はそれをファイル名に使用。
        """
        service = self._get_service()
        month_folder_id = self._get_or_create_month_folder(service, invoice_date)

        if vendor_name:
            safe_name = self._sanitize_filename(vendor_name)
            safe_filename = f"{safe_name}.pdf"
        else:
            if not filename.endswith(".pdf"):
                filename += ".pdf"
            safe_filename = filename

        file_meta = {"name": safe_filename, "parents": [month_folder_id]}
        media = MediaIoBaseUpload(
            io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=False
        )
        uploaded = service.files().create(
            body=file_meta, media_body=media, fields="id, webViewLink"
        ).execute()
        logger.info(f"Uploaded to Drive: {safe_filename} -> {uploaded.get('id')}")
        return uploaded.get("webViewLink", "")
