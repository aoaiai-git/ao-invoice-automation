"""AK請求書処理のfreee操作: AK支払い登録 & Aidiot請求書作成"""
import os
import logging
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

FREEE_API_BASE = "https://api.freee.co.jp"
FREEE_TOKEN_URL = "https://accounts.secure.freee.co.jp/public_api/token"
FREEE_COMPANY_ID = int(os.environ.get("FREEE_COMPANY_ID", "10397910"))
FREEE_AK_PARTNER_ID = int(os.environ.get("FREEE_AK_PARTNER_ID", "0"))
FREEE_AIDIOT_PARTNER_ID = int(os.environ.get("FREEE_AIDIOT_PARTNER_ID", "0"))
ACCOUNT_ITEM_ID_OUTSOURCING = 675785125  # 外注費


def _get_access_token():
    resp = requests.post(
        FREEE_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ.get("FREEE_CLIENT_ID", "677453071260482"),
            "client_secret": os.environ.get("FREEE_CLIENT_SECRET", ""),
            "refresh_token": os.environ.get("FREEE_REFRESH_TOKEN", ""),
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _freee_headers():
    token = _get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Api-Version": "2020-06-15",
    }


def _get_due_date(billing_month):
    """Due date: last day of next month"""
    try:
        year, month = map(int, billing_month.split("-"))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        if month == 12:
            last = datetime(year, month, 31)
        else:
            last = datetime(year, month + 1, 1) - timedelta(days=1)
        return last.strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def register_ak_payment_sync(billing_month, amount_excl_tax, amount_incl_tax):
    """支払い登録 (expense deal) — Returns freee deal ID"""
    headers = _freee_headers()
    issue_date = f"{billing_month}-01"
    due_date = _get_due_date(billing_month)
    payload = {
        "company_id": FREEE_COMPANY_ID,
        "issue_date": issue_date,
        "due_date": due_date,
        "type": "expense",
        "partner_id": FREEE_AK_PARTNER_ID,
        "details": [
            {
                "account_item_id": ACCOUNT_ITEM_ID_OUTSOURCING,
                "tax_code": 5,
                "amount": amount_incl_tax,
                "description": f"AK業務委託費（{billing_month}）",
            }
        ],
    }
    resp = requests.post(f"{FREEE_API_BASE}/api/1/deals", headers=headers, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"freee AK payment registration failed: {resp.status_code} {resp.text}")
    deal_id = resp.json().get("deal", {}).get("id")
    logger.info(f"AK payment registered in freee: deal_id={deal_id}")
    return deal_id


def create_aidiot_invoice_for_ak_sync(billing_month, amount_excl_tax, amount_incl_tax):
    """売上請求書作成 — Returns freee invoice ID"""
    headers = _freee_headers()
    issue_date = datetime.now().strftime("%Y-%m-%d")
    due_date = _get_due_date(billing_month)
    payload = {
        "company_id": FREEE_COMPANY_ID,
        "issue_date": issue_date,
        "due_date": due_date,
        "invoice_status": "issued",
        "partner_id": FREEE_AIDIOT_PARTNER_ID,
        "invoice_contents": [
            {
                "order": 0,
                "type": "normal",
                "qty": 1,
                "unit_price": amount_excl_tax,
                "vat_percent": 10,
                "description": f"AKシステム開発業務委託費（{billing_month}）",
                "tax_code": 5,
            }
        ],
    }
    resp = requests.post(f"{FREEE_API_BASE}/api/1/invoices", headers=headers, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"freee Aidiot invoice creation failed: {resp.status_code} {resp.text}")
    invoice_id = resp.json().get("invoice", {}).get("id")
    logger.info(f"Aidiot invoice created in freee: invoice_id={invoice_id}")
    return invoice_id
