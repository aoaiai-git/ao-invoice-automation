"""freee 消込用APIクライアント（入金明細・売上請求書・消込実行）"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

FREEE_TOKEN_URL = "https://accounts.secure.freee.co.jp/public_api/token"
FREEE_API_BASE = "https://api.freee.co.jp"


class FreeeReconcileClient:
    def __init__(self):
        self.client_id = os.environ.get("FREEE_CLIENT_ID", "677453071260482")
        self.client_secret = os.environ.get("FREEE_CLIENT_SECRET", "")
        self.refresh_token = os.environ.get("FREEE_REFRESH_TOKEN", "")
        self.company_id = int(os.environ.get("FREEE_COMPANY_ID", "10397910"))
        self._access_token: str | None = None

    # ─── OAuth2 ──────────────────────────────────────────────────────

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
            raise Exception(f"Token refresh failed: {resp.status_code} {resp.text[:200]}")

        token_data = resp.json()
        self._access_token = token_data["access_token"]

        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]
            os.environ["FREEE_REFRESH_TOKEN"] = token_data["refresh_token"]
            self._update_railway_refresh_token(token_data["refresh_token"])

        return self._access_token

    def _update_railway_refresh_token(self, new_token: str):
        """Railway環境変数のリフレッシュトークンを自動更新"""
        try:
            api_token = os.environ.get("RAILWAY_API_TOKEN")
            project_id = os.environ.get("RAILWAY_PROJECT_ID")
            env_id = os.environ.get("RAILWAY_ENVIRONMENT_ID")
            service_id = os.environ.get("RAILWAY_SERVICE_ID")
            if not all([api_token, project_id, env_id, service_id]):
                return

            resp = requests.post(
                "https://backboard.railway.com/graphql/v2",
                headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
                json={
                    "query": "mutation($v: VariableUpsertInput!) { variableUpsert(input: $v) }",
                    "variables": {"v": {
                        "projectId": project_id,
                        "environmentId": env_id,
                        "serviceId": service_id,
                        "name": "FREEE_REFRESH_TOKEN",
                        "value": new_token
                    }}
                },
                timeout=15
            )
            if resp.status_code == 200:
                logger.info("Railway FREEE_REFRESH_TOKEN updated")
            else:
                logger.warning(f"Railway token update failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Failed to update Railway token: {e}")

    # ─── APIユーティリティ ─────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        resp = requests.get(
            f"{FREEE_API_BASE}{path}",
            headers=self._headers(),
            params=params or {},
            timeout=30,
        )
        if resp.status_code != 200:
            raise Exception(f"GET {path} failed: {resp.status_code} {resp.text[:300]}")
        return resp.json()

    def _post(self, path: str, payload: dict) -> dict:
        resp = requests.post(
            f"{FREEE_API_BASE}{path}",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            raise Exception(f"POST {path} failed: {resp.status_code} {resp.text[:300]}")
        return resp.json()

    def _paginate(self, path: str, key: str, base_params: dict, limit: int = 100) -> list:
        """ページネーション対応の一覧取得"""
        all_items = []
        offset = 0
        while True:
            params = {**base_params, "offset": offset, "limit": limit}
            data = self._get(path, params)
            items = data.get(key, [])
            all_items.extend(items)
            if len(items) < limit:
                break
            offset += limit
        return all_items

    # ─── 口座明細（入金）─────────────────────────────────────────────

    def get_wallet_txns(
        self,
        start_date: str,
        end_date: str,
        entry_side: str = "income",
    ) -> list[dict]:
        """口座明細（入金）を取得

        Args:
            start_date: "2024-02-01" 形式
            end_date:   "2024-02-29" 形式
            entry_side: "income"（入金）or "expense"（出金）
        """
        items = self._paginate(
            "/api/1/wallet_txns",
            "wallet_txns",
            {
                "company_id": self.company_id,
                "start_date": start_date,
                "end_date": end_date,
                "entry_side": entry_side,
            }
        )
        logger.info(f"Got {len(items)} wallet_txns ({start_date}〜{end_date}, {entry_side})")
        return items

    # ─── 売上請求書 ─────────────────────────────────────────────────

    def get_unpaid_invoices(
        self,
        start_issue_date: str = None,
        end_issue_date: str = None,
    ) -> list[dict]:
        """未回収の請求書（送付済み + 未決済）を取得"""
        params = {
            "company_id": self.company_id,
            "invoice_status": "submitted",
            "payment_status": "unsettled",
        }
        if start_issue_date:
            params["start_issue_date"] = start_issue_date
        if end_issue_date:
            params["end_issue_date"] = end_issue_date

        items = self._paginate("/api/1/invoices", "invoices", params)
        logger.info(f"Got {len(items)} unpaid invoices")
        return items

    def get_invoice(self, invoice_id: int) -> dict:
        """請求書1件を取得"""
        data = self._get(f"/api/1/invoices/{invoice_id}", {"company_id": self.company_id})
        return data.get("invoice", {})

    # ─── 取引先 ───────────────────────────────────────────────────────

    def get_partners(self) -> list[dict]:
        """取引先一覧を取得"""
        data = self._get("/api/1/partners", {"company_id": self.company_id})
        return data.get("partners", [])

    # ─── 口座一覧 ────────────────────────────────────────────────────

    def get_walletables(self) -> list[dict]:
        """口座一覧を取得（walletable_id 解決用）"""
        data = self._get("/api/1/walletables", {"company_id": self.company_id})
        return data.get("walletables", [])

    # ─── 消込実行 ────────────────────────────────────────────────────

    def execute_reconciliation(
        self,
        invoice_id: int,
        amount: int,
        txn_date: str,
        walletable_id: int = None,
        walletable_type: str = "bank_account",
    ) -> dict:
        """請求書に対して入金消込を実行

        freee API: 請求書のdeal_idに対してpaymentを追加し、
        invoice を settled 状態にする。

        Returns:
            {"success": True, "deal_id": ..., "invoice_id": ...}
            or {"success": False, "reason": ..., "invoice_id": ...}
        """
        try:
            # 請求書のdeal_idを取得
            invoice = self.get_invoice(invoice_id)
            deal_id = invoice.get("deal_id")

            if not deal_id:
                logger.warning(f"Invoice {invoice_id} has no deal_id — cannot auto-reconcile in freee")
                return {"success": False, "reason": "no_deal_id", "invoice_id": invoice_id}

            # dealにpaymentを追加（入金消込）
            payload = {
                "company_id": self.company_id,
                "date": txn_date,
                "amount": amount,
                "from_walletable_type": walletable_type,
            }
            if walletable_id:
                payload["from_walletable_id"] = walletable_id

            token = self._get_access_token()
            resp = requests.post(
                f"{FREEE_API_BASE}/api/1/deals/{deal_id}/payments",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=30,
            )

            if resp.status_code in (200, 201):
                logger.info(f"Reconciled: invoice={invoice_id}, deal={deal_id}, amount={amount}")
                return {"success": True, "deal_id": deal_id, "invoice_id": invoice_id}
            else:
                logger.error(f"Reconciliation failed: {resp.status_code} {resp.text[:200]}")
                return {"success": False, "reason": resp.text[:200], "invoice_id": invoice_id}

        except Exception as e:
            logger.error(f"execute_reconciliation error: {e}", exc_info=True)
            return {"success": False, "reason": str(e), "invoice_id": invoice_id}
