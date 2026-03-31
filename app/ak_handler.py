"""AKエーケーからの請求書処理 + Aidiot向け請求書計算システム内統合"""
import os
import logging
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

AK_EMAIL = os.getenv("AK_EMAIL", "")
SLACK_AK_CHANNEL_ID = os.getenv("SLACK_AK_CHANNEL_ID", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
AIDIOT_HOURLY_RATE = int(os.getenv("AIDIOT_HOURLY_RATE", "0"))

def is_ak_sender(sender: str) -> bool:
    if not AK_EMAIL:
        return False
    return AK_EMAIL.lower() in sender.lower()

def get_billing_month() -> str:
    now = datetime.now()
    if now.month == 1:
        return f"{now.year - 1}-12"
    return f"{now.year}-{now.month - 1:02d}"

async def _sb_get(table, params=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase not configured")
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"Supabase GET {table} failed: {resp.status_code} {resp.text}")
        return []

async def _sb_upsert(table, data):
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase not configured")
        return None
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=data, timeout=10)
        if resp.status_code in (200, 201):
            rows = resp.json()
            return rows[0] if rows else None
        logger.error(f"Supabase UPSERT {table} failed: {resp.status_code} {resp.text}")
        return None

async def _sb_patch(table, params, data):
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase not configured")
        return False
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.patch(url, headers=headers, params=params, json=data, timeout=10)
        if resp.status_code in (200, 204):
            return True
        logger.error(f"Supabase PATCH {table} failed: {resp.status_code} {resp.text}")
        return False

def _build_approval_blocks(billing_month, actual_hours, ak_invoice_excl_tax, ak_invoice_incl_tax, aidiot_subtotal, aidiot_tax, aidiot_total, vendor_name, record_id):
    return [
        {"type": "header", "text": {"type": "plain_text", "text": "\ud83d\udcc4 AK\u8acb\u6c42\u66f8 \u53d7\u4fe1 \u2014 \u78ba\u8a8d\u30fb\u627f\u8a8d"}},
        {"type": "section", "fields": [{"type": "mrkdwn", "text": f"*\u8acb\u6c42\u5143:*\n{vendor_name}"}, {"type": "mrkdwn", "text": f"*\u8acb\u6c42\u5bfe\u8c61\u6708:*\n{billing_month}"}]},
        {"type": "section", "fields": [{"type": "mrkdwn", "text": f"*\u5b9f\u7a3c\u50cd\u6642\u9593:*\n{actual_hours} \u6642\u9593"}, {"type": "mrkdwn", "text": f"*Aidiot\u6642\u7d66\u5358\u4fa1:*\n\u00a5{AIDIOT_HOURLY_RATE:,}/\u6642\u9593"}]},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*\u25a0 AK \u2192 AO \u652f\u6255\u3044\uff08\u4ed5\u5165\u308c\uff09*"}, "fields": [{"type": "mrkdwn", "text": f"*\u7a0e\u629c:* \u00a5{ak_invoice_excl_tax:,}"}, {"type": "mrkdwn", "text": f"*\u7a0e\u8fbc:* \u00a5{ak_invoice_incl_tax:,}"}]},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*\u25a0 AO \u2192 Aidiot \u8acb\u6c42\uff08\u58f2\u4e0a\uff09*"}, "fields": [{"type": "mrkdwn", "text": f"*\u5c0f\u8a08:* \u00a5{aidiot_subtotal:,}"}, {"type": "mrkdwn", "text": f"*\u6d88\u8cbb\u7a0e(10%):* \u00a5{aidiot_tax:,}"}, {"type": "mrkdwn", "text": f"*\u5408\u8a08:* \u00a5{aidiot_total:,}"}]},
        {"type": "divider"},
        {"type": "actions", "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "\u2705 \u627f\u8a8d\u30fbfreee\u767b\u9332"}, "style": "primary", "action_id": "ak_approve", "value": record_id, "confirm": {"title": {"type": "plain_text", "text": "\u627f\u8a8d\u78ba\u8a8d"}, "text": {"type": "mrkdwn", "text": f"AK\u652f\u6255\u3044 (\u00a5{ak_invoice_incl_tax:,}) \u3092freee\u306b\u767b\u9332\u3057\u3001Aidiot\u8acb\u6c42\u66f8 (\u00a5{aidiot_total:,}) \u3092\u4f5c\u6210\u3057\u307e\u3059\u3002"}, "confirm": {"type": "plain_text", "text": "\u5b9f\u884c"}, "deny": {"type": "plain_text", "text": "\u30ad\u30e3\u30f3\u30bb\u30eb"}}},
            {"type": "button", "text": {"type": "plain_text", "text": "\u274c \u5374\u4e0b"}, "style": "danger", "action_id": "ak_reject", "value": record_id}
        ]}
    ]

async def process_ak_invoice(slack_client, analysis, pdf_data, drive_handler=None):
    billing_month = get_billing_month()
    vendor_name = analysis.get("vendor_name", "AK")
    actual_hours = float(analysis.get("actual_hours") or 0)
    ak_invoice_excl_tax = int(analysis.get("amount_excl_tax") or 0)
    ak_invoice_incl_tax = int(analysis.get("amount_incl_tax") or 0)
    aidiot_subtotal = int(actual_hours * AIDIOT_HOURLY_RATE // 10) * 10
    aidiot_tax = int(aidiot_subtotal * 0.1 // 10) * 10
    aidiot_total = aidiot_subtotal + aidiot_tax
    logger.info(f"AK invoice: billing_month={billing_month}, actual_hours={actual_hours}, ak_excl={ak_invoice_excl_tax}, ak_incl={ak_invoice_incl_tax}, aidiot_subtotal={aidiot_subtotal}, aidiot_total={aidiot_total}")
    pdf_drive_url = None
    if drive_handler and pdf_data:
        try:
            invoice_date = analysis.get("invoice_date", datetime.now().strftime("%Y-%m-%d"))
            pdf_drive_url = await drive_handler.upload_invoice(pdf_data, f"AK_{billing_month}.pdf", invoice_date, vendor_name=vendor_name)
            logger.info(f"AK PDF uploaded to Drive: {pdf_drive_url}")
        except Exception as e:
            logger.error(f"Failed to upload AK PDF to Drive: {e}")
    record = await _sb_upsert("ak_aidiot_billing", {"billing_month": billing_month, "ak_actual_hours": actual_hours, "ak_invoice_excl_tax": ak_invoice_excl_tax, "ak_invoice_incl_tax": ak_invoice_incl_tax, "aidiot_subtotal": aidiot_subtotal, "aidiot_tax": aidiot_tax, "aidiot_total": aidiot_total, "status": "received", "pdf_drive_url": pdf_drive_url})
    record_id = record["id"] if record else billing_month
    channel_id = SLACK_AK_CHANNEL_ID
    if not channel_id:
        logger.error("SLACK_AK_CHANNEL_ID is not set")
        return
    blocks = _build_approval_blocks(billing_month=billing_month, actual_hours=actual_hours, ak_invoice_excl_tax=ak_invoice_excl_tax, ak_invoice_incl_tax=ak_invoice_incl_tax, aidiot_subtotal=aidiot_subtotal, aidiot_tax=aidiot_tax, aidiot_total=aidiot_total, vendor_name=vendor_name, record_id=record_id)
    resp = slack_client.chat_postMessage(channel=channel_id, text=f"AK\u8acb\u6c42\u66f8\u53d7\u4fe1: {billing_month} / Aidiot\u8acb\u6c42\u984d \u00a5{aidiot_total:,}", blocks=blocks)
    message_ts = resp.get("ts", "")
    if record and message_ts:
        await _sb_patch("ak_aidiot_billing", {"id": f"eq.{record['id']}"}, {"slack_message_ts": message_ts})
    logger.info(f"AK invoice posted to Slack: ts={message_ts}")

async def handle_ak_approve(slack_client, channel, message_ts, record_id, user_id, user_name):
    from . import ak_freee
    rows = await _sb_get("ak_aidiot_billing", {"id": f"eq.{record_id}"})
    if not rows:
        logger.error(f"ak_aidiot_billing record not found: {record_id}")
        try:
            slack_client.chat_postEphemeral(channel=channel, user=user_id, text="\u26a0\ufe0f \u53f0\u5e33\u30ec\u30b3\u30fc\u30c9\u304c\u898b\u3064\u304b\u308a\u307e\u305b\u3093\u3067\u3057\u305f\u3002")
        except Exception:
            pass
        return
    record = rows[0]
    billing_month = record["billing_month"]
    ak_invoice_excl_tax = record["ak_invoice_excl_tax"] or 0
    ak_invoice_incl_tax = record["ak_invoice_incl_tax"] or 0
    aidiot_subtotal = record["aidiot_subtotal"] or 0
    aidiot_total = record["aidiot_total"] or 0
    try:
        ak_deal_id = ak_freee.register_ak_payment_sync(billing_month=billing_month, amount_excl_tax=ak_invoice_excl_tax, amount_incl_tax=ak_invoice_incl_tax)
        logger.info(f"AK freee deal created: deal_id={ak_deal_id}")
        aidiot_invoice_id = ak_freee.create_aidiot_invoice_for_ak_sync(billing_month=billing_month, amount_excl_tax=aidiot_subtotal, amount_incl_tax=aidiot_total)
        logger.info(f"Aidiot invoice created: invoice_id={aidiot_invoice_id}")
        await _sb_patch("ak_aidiot_billing", {"id": f"eq.{record_id}"}, {"ak_freee_deal_id": ak_deal_id, "aidiot_freee_invoice_id": aidiot_invoice_id, "status": "approved"})
        slack_client.chat_update(channel=channel, ts=message_ts, text=f"\u2705 AK\u8acb\u6c42\u66f8\u627f\u8a8d\u6e08\u307f ({billing_month}) \u2014 freee\u767b\u9332\u5b8c\u4e86 by {user_name}", blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": f"\u2705 *\u627f\u8a8d\u30fbfreee\u767b\u9332\u5b8c\u4e86* by {user_name}\n\u8acb\u6c42\u5bfe\u8c61\u6708: {billing_month}\nAK\u652f\u6255\u3044 (freee deal ID: {ak_deal_id}) \u00a5{ak_invoice_incl_tax:,}\nAidiot\u8acb\u6c42\u66f8 (freee invoice ID: {aidiot_invoice_id}) \u00a5{aidiot_total:,}"}}])
    except Exception as e:
        logger.error(f"AK approve failed: {e}", exc_info=True)
        try:
            slack_client.chat_postEphemeral(channel=channel, user=user_id, text=f"\u26a0\ufe0f freee\u767b\u9332\u4e2d\u306b\u30a8\u30e9\u30fc\u304c\u767a\u751f\u3057\u307e\u3057\u305f: {e}")
        except Exception:
            pass

async def handle_ak_reject(slack_client, channel, message_ts, record_id, user_name):
    await _sb_patch("ak_aidiot_billing", {"id": f"eq.{record_id}"}, {"status": "rejected"})
    try:
        slack_client.chat_update(channel=channel, ts=message_ts, text=f"\u274c AK\u8acb\u6c42\u66f8\u5374\u4e0b by {user_name}", blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": f"\u274c *\u5374\u4e0b* by {user_name}"}}])
    except Exception as e:
        logger.error(f"Failed to update Slack message on rejection: {e}")
    logger.info(f"AK invoice rejected: record_id={record_id} by {user_name}")
