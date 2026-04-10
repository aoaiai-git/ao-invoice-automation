"""
アイディオット向け freee 売上請求書作成
"""
import os
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import List

logger = logging.getLogger(__name__)

FREEE_TOKEN_URL = "https://accounts.secure.freee.co.jp/public_api/token"
FREEE_API_BASE = "https://api.freee.co.jp"
JST = timezone(timedelta(hours=9))

IDIOTT_COMPANY_NAME = "株式会社アイディオット"
MANAGEMENT_FEE_PER_PERSON = 5000


def _get_access_token() -> str:
    """freee アクセストークン取得"""
    resp = requests.post(FREEE_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "client_id": os.environ.get("FREEE_CLIENT_ID", "677453071260482"),
        "client_secret": os.environ.get("FREEE_CLIENT_SECRET", ""),
        "refresh_token": os.environ.get("FREEE_REFRESH_TOKEN", ""),
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def create_idiott_invoice_sync(
    billing_month: str,
    invoices: List[dict],
    management_fee_excl: int,
    grand_total_incl: int,
) -> int:
    """freeeに売上請求書を作成して invoice_id を返す"""
    access_token = _get_access_token()
    company_id = int(os.environ.get("FREEE_COMPANY_ID", "10397910"))
    partner_id = int(os.environ.get("FREEE_IDIOTT_PARTNER_ID", "0"))

    # 日付: 請求月翌月1日が発行日、翌月末が支払期日
    year, month = billing_month.split("-")
    y, m = int(year), int(month)
    next_m = m + 1 if m < 12 else 1
    next_y = y if m < 12 else y + 1
    issue_date = f"{next_y}-{next_m:02d}-01"

    # 支払期日: 翌々月1日の前日（翌月末）
    due_m = next_m + 1 if next_m < 12 else 1
    due_y = next_y if next_m < 12 else next_y + 1
    due_date = f"{due_y}-{due_m:02d}-01"

    # 明細: 各業務委託者の立替費用
    invoice_contents = []
    for idx, inv in enumerate(invoices, 1):
        amount_excl = int(inv.get("amount_excl_tax", 0) or 0)
        invoice_contents.append({
            "order": idx,
            "type": "normal",
            "qty": 1,
            "unit_price": amount_excl,
            "vat_percent": 10,
            "description": f"業務委託費（{inv['contractor_name']} / {billing_month}）",
            "tax_code": 5,  # 課税売上10%
        })

    # 明細: 管理手数料
    invoice_contents.append({
        "order": len(invoice_contents) + 1,
        "type": "normal",
        "qty": len(invoices),
        "unit_price": MANAGEMENT_FEE_PER_PERSON,
        "vat_percent": 10,
        "description": f"業務委託管理手数料（{billing_month}）",
        "tax_code": 5,
    })

    payload = {
        "company_id": company_id,
        "issue_date": issue_date,
        "due_date": due_date,
        "invoice_status": "issued",
        "message": f"{billing_month}分 業務委託費（立替）および管理手数料のご請求です。",
        "invoice_contents": invoice_contents,
    }
    # partner_id が設定されている場合のみセット
    if partner_id > 0:
        payload["partner_id"] = partner_id
    else:
        payload["partner_display_name"] = IDIOTT_COMPANY_NAME

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(
        f"{FREEE_API_BASE}/api/1/invoices",
        json=payload,
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise Exception(
            f"freee invoice creation failed: {resp.status_code} - {resp.text[:300]}"
        )

    invoice = resp.json().get("invoice", {})
    invoice_id = invoice.get("id")
    logger.info(f"freee invoice created: ID={invoice_id}, �{grand_total_incl:,}")
    return invoice_id
