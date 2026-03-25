"""freee API ハンドラー"""

import os
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FREEE_TOKEN_URL = "https://accounts.secure.freee.co.jp/public_api/token"
FREEE_API_BASE = "https://api.freee.co.jp"


class FreeeHandler:
    def __init__(self):
        self.client_id = os.environ.get("FREEE_CLIENT_ID", "677453071260482")
        self.client_secret = os.environ.get("FREEE_CLIENT_SECRET", "")
        self.refresh_token = os.environ.get("FREEE_REFRESH_TOKEN", "")
        self.company_id = int(os.environ.get("FREEE_COMPANY_ID", "10397910"))
        self._access_token = None

    def _get_access_token(self) -> str:
        """リフレッシュトークンからアクセストークンを取得"""
        if self._access_token:
            return self._access_token

        resp = requests.post(FREEE_TOKEN_URL, data={
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }, timeout=30)

        if resp.status_code != 200:
            raise Exception(f"Failed to get access token: {resp.status_code} {resp.text}")

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        # リフレッシュトークンの更新
        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]
        return self._access_token

    async def check_duplicate(self, invoice_data: dict) -> dict:
        """freeeに同じ請求書が既に登録されていないか確認

        Returns:
            既存の取引dict（重複あり）またはNone（重複なし）
        """
        vendor_name = invoice_data.get("vendor_name", "")
        invoice_number = invoice_data.get("invoice_number", "")
        invoice_date = invoice_data.get("invoice_date", "")
        amount_incl_tax = invoice_data.get("amount_incl_tax", 0)

        # 判定に使える情報がなければスキップ
        if not vendor_name and not invoice_number:
            logger.info("check_duplicate: no vendor_name or invoice_number, skipping")
            return None

        try:
            token = self._get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            params = {
                "company_id": self.company_id,
                "type": "expense",
                "limit": 50,
                "offset": 0,
            }

            # 請求日 ±7日の範囲で検索
            if invoice_date:
                try:
                    dt = datetime.strptime(invoice_date, "%Y-%m-%d")
                    params["start_issue_date"] = (dt - timedelta(days=7)).strftime("%Y-%m-%d")
                    params["end_issue_date"] = (dt + timedelta(days=7)).strftime("%Y-%m-%d")
                except ValueError:
                    pass

            resp = requests.get(
                f"{FREEE_API_BASE}/api/1/deals",
                headers=headers,
                params=params,
                timeout=15
            )

            if resp.status_code != 200:
                logger.warning(f"freee duplicate check failed: {resp.status_code} {resp.text[:200]}")
                return None

            deals = resp.json().get("deals", [])
            logger.info(f"freee duplicate check: found {len(deals)} deals in range")

            for deal in deals:
                # ① 請求書番号で一致（最優先・最確実）
                deal_ref = deal.get("ref_number", "") or ""
                if invoice_number and deal_ref and deal_ref == invoice_number:
                    logger.info(f"Duplicate deal found by invoice_number={invoice_number}: id={deal['id']}")
                    return deal

                # ② 取引先名 + 金額で一致（請求書番号がない場合のフォールバック）
                if not invoice_number and vendor_name:
                    partner_name = deal.get("partner_name", "") or ""
                    if vendor_name and partner_name and (
                        vendor_name in partner_name or partner_name in vendor_name
                    ):
                        for detail in deal.get("details", []):
                            if int(detail.get("amount", 0)) == int(amount_incl_tax):
                                logger.info(
                                    f"Duplicate deal found by vendor+amount: "
                                    f"vendor={vendor_name}, amount={amount_incl_tax}, id={deal['id']}"
                                )
                                return deal

            return None

        except Exception as e:
            logger.error(f"Error checking freee duplicate: {e}", exc_info=True)
            return None  # チェック失敗時は重複なしとして処理継続

    async def create_expense(self, invoice_data: dict) -> dict:
        """freeeに経費（支払い）を登録"""
        access_token = self._get_access_token()

        vendor_name = invoice_data.get("vendor_name", "不明")
        amount_incl_tax = invoice_data.get("amount_incl_tax", 0)
        amount_excl_tax = invoice_data.get("amount_excl_tax", 0)
        tax_amount = invoice_data.get("tax_amount", 0)
        account_item_id = invoice_data.get("suggested_account_id", 675785162)
        description = invoice_data.get("description", "")
        invoice_date = invoice_data.get("invoice_date", "")
        invoice_number = invoice_data.get("invoice_number", "")

        # 日付の正規化
        if not invoice_date:
            invoice_date = datetime.now().strftime("%Y-%m-%d")

        # 摘要の組み立て
        memo = f"{vendor_name}"
        if invoice_number:
            memo += f" 請求書#{invoice_number}"
        if description:
            memo += f" {description}"
        memo = memo[:250]  # freeeの制限

        # 税区分の判定
        if amount_incl_tax > 0 and amount_excl_tax > 0:
            tax_rate = round(tax_amount / amount_excl_tax * 100)
            if tax_rate == 10:
                tax_code = 1011  # 課税仕入 10%
            elif tax_rate == 8:
                tax_code = 1012  # 課税仕入 8%（軽減税率）
            else:
                tax_code = 1012
        else:
            tax_code = 1011  # デフォルト 10%
            amount_excl_tax = int(amount_incl_tax / 1.1)
            tax_amount = amount_incl_tax - amount_excl_tax

        simple_payload = {
            "company_id": self.company_id,
            "issue_date": invoice_date,
            "type": "expense",
            "partner_name": vendor_name,
            "ref_number": invoice_number or None,
            "details": [
                {
                    "account_item_id": account_item_id,
                    "tax_code": tax_code,
                    "amount": amount_incl_tax,
                    "description": memo,
                    "vat": tax_amount,
                }
            ]
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(
                f"{FREEE_API_BASE}/api/1/deals",
                json=simple_payload,
                headers=headers,
                timeout=30
            )

            if resp.status_code in (200, 201):
                result = resp.json()
                deal = result.get("deal", {})
                logger.info(f"freee deal created: id={deal.get('id')}, "
                           f"amount={amount_incl_tax}, account_id={account_item_id}")
                return result
            else:
                logger.error(f"freee API error: {resp.status_code} {resp.text[:500]}")
                raise Exception(f"freee API error: {resp.status_code}")

        except Exception as e:
            logger.error(f"Error creating freee deal: {e}", exc_info=True)
            raise
