"""Google Sheets クライアント（消込マスタ・消込記録の管理）"""

import os
import json
import logging
from datetime import datetime
from typing import Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_NAME_MAPPING = "消込マスタ"
SHEET_RECONCILIATION = "消込記録"

# 消込マスタ: 取引先名とメモキーワードのマッピング
MAPPING_COLS = ["freee_partner_name", "memo_keywords", "notes"]

# 消込記録: 各入金に対する処理ログ
RECORD_COLS = [
    "run_id", "txn_id", "txn_date", "amount", "memo", "partner_name",
    "invoice_id", "invoice_no", "invoice_amount", "match_type", "confidence",
    "status", "processed_at", "processed_by", "slack_ts", "notes"
]


class ReconciliationSheetsClient:
    def __init__(self):
        service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not service_account_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

        creds_info = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build("sheets", "v4", credentials=creds)
        self.sheets = service.spreadsheets()
        self.spreadsheet_id = os.environ.get(
            "RECONCILIATION_SPREADSHEET_ID",
            os.environ.get("SPREADSHEET_ID", "")
        )
        if not self.spreadsheet_id:
            raise ValueError("RECONCILIATION_SPREADSHEET_ID is not set")

    # ─── 内部ユーティリティ ───────────────────────────────────────────

    def _get_values(self, sheet_name: str, range_: str = "A:P") -> list:
        result = self.sheets.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!{range_}"
        ).execute()
        return result.get("values", [])

    def _append_row(self, sheet_name: str, values: list):
        self.sheets.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!A:Z",
            valueInputOption="USER_ENTERED",
            body={"values": [values]}
        ).execute()

    def _update_row(self, sheet_name: str, row_idx: int, values: list):
        """row_idx: 1始まり（ヘッダー=1行目）"""
        col_count = len(values)
        end_col = chr(ord("A") + col_count - 1)
        self.sheets.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!A{row_idx}:{end_col}{row_idx}",
            valueInputOption="USER_ENTERED",
            body={"values": [values]}
        ).execute()

    def _rows_to_dicts(self, rows: list) -> list[dict]:
        if len(rows) < 2:
            return []
        headers = rows[0]
        return [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows[1:]]

    # ─── 消込マスタ ───────────────────────────────────────────────────

    def get_name_mapping(self) -> list[dict]:
        """取引先→メモキーワードのマッピング一覧を取得"""
        rows = self._get_values(SHEET_NAME_MAPPING)
        return self._rows_to_dicts(rows)

    def upsert_name_mapping(self, partner_name: str, keywords: str, notes: str = ""):
        """取引先マッピングを追加 or 更新（同名がなければ追加）"""
        rows = self._get_values(SHEET_NAME_MAPPING)
        if not rows:
            self._append_row(SHEET_NAME_MAPPING, MAPPING_COLS)
            rows = [MAPPING_COLS]

        for idx, row in enumerate(rows[1:], start=2):
            if row and row[0] == partner_name:
                self._update_row(SHEET_NAME_MAPPING, idx, [partner_name, keywords, notes])
                return
        self._append_row(SHEET_NAME_MAPPING, [partner_name, keywords, notes])

    # ─── 消込記録 ─────────────────────────────────────────────────────

    def get_record_by_txn_id(self, txn_id: str) -> tuple[Optional[dict], int]:
        """txn_id で記録を検索。(record, row_index) を返す（row_index は1始まり）"""
        rows = self._get_values(SHEET_RECONCILIATION)
        if len(rows) < 2:
            return None, -1
        headers = rows[0]
        for idx, row in enumerate(rows[1:], start=2):
            if len(row) > 1 and str(row[1]) == str(txn_id):   # col B = txn_id
                return dict(zip(headers, row + [""] * max(0, len(headers) - len(row)))), idx
        return None, -1

    def get_pending_records(self) -> list[dict]:
        """承認待ちの消込記録を取得"""
        rows = self._get_values(SHEET_RECONCILIATION)
        records = self._rows_to_dicts(rows)
        return [r for r in records if r.get("status") in ("pending_approval", "pending_manual")]

    def append_record(self, record: dict):
        """消込記録を新規追加"""
        rows = self._get_values(SHEET_RECONCILIATION)
        if not rows:
            self._append_row(SHEET_RECONCILIATION, RECORD_COLS)
        values = [str(record.get(col, "")) for col in RECORD_COLS]
        self._append_row(SHEET_RECONCILIATION, values)

    def update_record_status(
        self,
        txn_id: str,
        status: str,
        processed_by: str = "",
        notes: str = "",
        slack_ts: str = ""
    ):
        """txn_id を key に消込記録のステータスを更新"""
        record, row_idx = self.get_record_by_txn_id(txn_id)
        if not record or row_idx < 0:
            logger.warning(f"Record not found for txn_id={txn_id}")
            return

        record["status"] = status
        record["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if processed_by:
            record["processed_by"] = processed_by
        if notes:
            record["notes"] = notes
        if slack_ts:
            record["slack_ts"] = slack_ts

        values = [str(record.get(col, "")) for col in RECORD_COLS]
        self._update_row(SHEET_RECONCILIATION, row_idx, values)
        logger.info(f"Updated record txn_id={txn_id} → status={status}")

    def update_invoice_for_record(self, txn_id: str, invoice_id: str, invoice_no: str, invoice_amount: str):
        """手動選択時に請求書情報を上書き"""
        record, row_idx = self.get_record_by_txn_id(txn_id)
        if not record or row_idx < 0:
            return
        record["invoice_id"] = invoice_id
        record["invoice_no"] = invoice_no
        record["invoice_amount"] = invoice_amount
        record["match_type"] = "manual"
        values = [str(record.get(col, "")) for col in RECORD_COLS]
        self._update_row(SHEET_RECONCILIATION, row_idx, values)

    # ─── シート初期化 ─────────────────────────────────────────────────

    def ensure_sheets_exist(self):
        """必要なシートを確認・作成する"""
        try:
            meta = self.sheets.get(spreadsheetId=self.spreadsheet_id).execute()
            existing = {s["properties"]["title"] for s in meta.get("sheets", [])}

            reqs = []
            for name in [SHEET_NAME_MAPPING, SHEET_RECONCILIATION]:
                if name not in existing:
                    reqs.append({"addSheet": {"properties": {"title": name}}})

            if reqs:
                self.sheets.batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": reqs}
                ).execute()
                created = [r["addSheet"]["properties"]["title"] for r in reqs]
                logger.info(f"Created sheets: {created}")

            # ヘッダー行を追加（空なら）
            if SHEET_NAME_MAPPING not in existing or not self._get_values(SHEET_NAME_MAPPING):
                self._append_row(SHEET_NAME_MAPPING, MAPPING_COLS)
            if SHEET_RECONCILIATION not in existing or not self._get_values(SHEET_RECONCILIATION):
                self._append_row(SHEET_RECONCILIATION, RECORD_COLS)

            logger.info("Sheets initialized successfully")
        except Exception as e:
            logger.error(f"Failed to ensure sheets: {e}", exc_info=True)
            raise

# build: 2026-03-26
