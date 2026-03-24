"""freee API ハンドラー"""

import os
import logging
import requests
from datetime import datetime

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
        })

        if resp.status_code != 200:
            raise ValueError(f"freee token refresh failed: {resp.status_code} {resp.text}")

        data = resp.json()
        self._access_token = data["access_token"]

        # 新しいリフレッシュトークンを更新
        new_refresh = data.get("refresh_token")
        if new_refresh and new_refresh != self.refresh_token:
            self.refresh_token = new_refresh
            self._update_railway_env("FREEE_REFRESH_TOKEN", new_refresh)

        return self._access_token

    def _update_railway_env(self, key: str, value: str):
        """Railway環境変数を更新（ao-daily-batchと同じ仕組み）"""
        railway_token = os.environ.get("RAILWAY_API_TOKEN", "")
        project_id = os.environ.get("RAILWAY_PROJECT_ID", "")
        env_id = os.environ.get("RAILWAY_ENVIRONMENT_ID", "")
        service_id = os.environ.get("RAILWAY_SERVICE_ID", "")

        if not all([railway_token, project_id, env_id, service_id]):
            logger.warning("Railway credentials not set, skipping env update")
            return

        query = """
        mutation {
            variableUpsert(input: {
                projectId: "%s",
                environmentId: "%s",
                serviceId: "%s",
                name: "%s",
                value: "%s"
            })
        }
        """ % (project_id, env_id, service_id, key, value)

        try:
            resp = requests.post(
                "https://backboard.railway.com/graphql/v2",
                json={"query": query},
                headers={"Authorization": f"Bearer {railway_token}",
                         "Content-Type": "application/json"},
                timeout=10
            )
            logger.info(f"Railway env update {key}: {resp.status_code}")
        except Exception as e:
            logger.error(f"Railway env update failed: {e}")

    async def create_expense(self, invoice_data: dict) -> dict:
        """freeeに支出取引を登録"""
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

        deal_payload = {
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
            ],
            "payments": [
                {
                    "from_walletable_type": "bank_account",
                    "from_walletable_id": None,  # 未払金として登録
                    "amount": amount_incl_tax,
                    "date": invoice_date,
                }
            ]
        }

        # 未払金として登録（支払口座を指定しない場合）
        # - deal_paymentは省略してシンプルな取引として登録
        simple_payload = {
            "company_id": self.company_id,
            "issue_date": invoice_date,
            "type": "expense",
            "partner_name": vendor_name,
            "ref_number": invoice_number if invoice_number else None,
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
                logger.error(f"freee deal creation failed: {resp.status_code} {resp.text}")
                return {"error": resp.text, "status_code": resp.status_code}

        except Exception as e:
            logger.error(f"freee API call failed: {e}", exc_info=True)
            return {"error": str(e)}
