"""
ã¢ã¤ãã£ãªããæ¥­åå§è¨è«æ±æ¸ãã­ã¼
- æ¥­åå§è¨èãããªæ¸PDFåé  â Supabaseè¨é² â Slackéç¥
- å¨å¡åæã®ããã£ãããè«æ±æ¸ä½æããã¿ã³è¡¨ç¤º
- è«æ±æ¸ãã¬ãã¥ã¼ï¼ç¿ç°ããè¨ç»å¤æ¯è¼ã»è­¦åä»ãï¼
- ãFreeeç»é²ããã¿ã³ â Freeeã«å£²ä¸è«æ±æ¸ç»é²
"""
import os
import logging
import httpx
from datetime import datetime, timezone, timedelta
from typing import Optional, List

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
SLACK_IDIOTT_CHANNEL_ID = os.getenv("SLACK_IDIOTT_CHANNEL_ID", "C0APDEC3KE1")

JST = timezone(timedelta(hours=9))
MANAGEMENT_FEE_PER_PERSON = 5000  # ç¨æ 5,000å/äºº
AO_COMPANY_NAME = "ä¸è¬ç¤¾å£æ³äººã¢ã½ã·ã¨ã¼ã·ã§ã³ãªãã£ã¹"
IDIOTT_COMPANY_NAME = "æ ªå¼ä¼ç¤¾ã¢ã¤ãã£ãªãã"
SARUTA_EMAIL = "saruta@aidiot.jp"


def get_billing_month() -> str:
    """ä»æã® YYYY-MM ãè¿ã"""
    return datetime.now(JST).strftime("%Y-%m")


# âââ Supabase REST helper ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async def _sb_get(table: str, params: dict = None) -> list:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase not configured")
        return []
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            params=params,
            headers=headers,
            timeout=10
        )
        if r.status_code >= 400:
            logger.error(f"Supabase GET {table} error {r.status_code}: {r.text[:200]}")
            return []
        return r.json()


async def _sb_upsert(table: str, data: dict) -> Optional[dict]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            json=data,
            headers=headers,
            timeout=10
        )
        if r.status_code >= 400:
            logger.error(f"Supabase UPSERT {table} error {r.status_code}: {r.text[:200]}")
            return None
        result = r.json()
        return result[0] if isinstance(result, list) and result else {}


async def _sb_patch(table: str, params: dict, data: dict) -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/{table}",
            params=params,
            json=data,
            headers=headers,
            timeout=10
        )
        return r.status_code < 400


# âââ Data access ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async def is_idiott_contact(email: str) -> bool:
    """idiott_contacts ã«ã¡ã¼ã«ã¢ãã¬ã¹ãå­å¨ãããç¢ºèª"""
    r = await _sb_get("idiott_contacts", {"email": f"ilike.{email}", "select": "id", "limit": "1"})
    return len(r) > 0


async def get_idiott_contacts_count() -> int:
    """idiott_contacts ã®ç·ä»¶æ°"""
    r = await _sb_get("idiott_contacts", {"select": "id"})
    return len(r)


async def get_all_idiott_contacts() -> List[dict]:
    """idiott_contacts ã®å¨ä»¶åå¾ï¼ååã»ã¡ã¼ã«ï¼"""
    return await _sb_get("idiott_contacts", {"select": "id,name,email"})


async def get_month_invoices(billing_month: str) -> List[dict]:
    """æå®æã®åé æ¸ã¿è«æ±æ¸ä¸è¦§"""
    return await _sb_get("idiott_invoices", {
        "billing_month": f"eq.{billing_month}",
        "select": "*",
        "order": "created_at.asc",
    })


async def store_contractor_invoice(
    billing_month: str,
    contractor_email: str,
    contractor_name: str,
    amount_excl_tax: int,
    amount_incl_tax: int,
    pdf_drive_url: str = None,
) -> Optional[dict]:
    """åé è«æ±æ¸ãSupabaseã«UPSERT"""
    return await _sb_upsert("idiott_invoices", {
        "billing_month": billing_month,
        "contractor_email": contractor_email.lower(),
        "contractor_name": contractor_name,
        "amount_excl_tax": amount_excl_tax,
        "amount_incl_tax": amount_incl_tax,
        "pdf_drive_url": pdf_drive_url,
        "status": "received",
    })


async def mark_month_registered(billing_month: str, freee_invoice_id: str):
    """ææ¬¡è«æ±æ¸ã®ã¹ãã¼ã¿ã¹ãfreeeç»é²æ¸ã¿ã«æ´æ°"""
    await _sb_patch(
        "idiott_invoices",
        {"billing_month": f"eq.{billing_month}"},
        {"status": "freee_registered", "freee_invoice_id": freee_invoice_id},
    )


async def store_saruta_reference(
    billing_month: str,
    amount_excl_tax: int,
    amount_incl_tax: int,
    person_count: int = 0,
    pdf_drive_url: str = None,
) -> Optional[dict]:
    """ç¿ç°ããã®è«æ±æ¸ãè¨ç»å¤ã¨ãã¦Supabaseã«ä¿å­"""
    return await _sb_upsert("idiott_saruta_refs", {
        "billing_month": billing_month,
        "amount_excl_tax": amount_excl_tax,
        "amount_incl_tax": amount_incl_tax,
        "person_count": person_count,
        "pdf_drive_url": pdf_drive_url,
    })


async def get_saruta_reference(billing_month: str) -> Optional[dict]:
    """æå®æã®ç¿ç°ããè¨ç»å¤ãåå¾"""
    r = await _sb_get("idiott_saruta_refs", {
        "billing_month": f"eq.{billing_month}",
        "select": "*",
        "limit": "1",
    })
    return r[0] if r else None


# âââ Warning logic âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async def check_warnings(
    billing_month: str,
    invoices: List[dict],
    saruta_ref: Optional[dict],
    total_contractor_excl: int,
) -> List[str]:
    """
    è­¦åãã§ãã¯ãä»¥ä¸ã®å ´åã«è­¦åãè¿ã:
    1. éé¡ãç¿ç°ããã®è¨ç»å¤ã¨10%ä»¥ä¸ããã¦ãã
    2. å°å¸³ï¼idiott_contactsï¼ã«ãªãã¡ã¼ã«ã¢ãã¬ã¹ããã®è«æ±æ¸ããã
    3. å°å¸³ã«å­å¨ããäººãæªæåºï¼ã¾ã è«æ±æ¸ãå±ãã¦ããªãï¼
    4. å°å¸³ã«ããã®ã«ç¿ç°ããã®è«æ±æ¸ã®äººæ°ã«å«ã¾ãã¦ããªã
    """
    warnings = []
    all_contacts = await get_all_idiott_contacts()
    registered_emails = {c["email"].lower() for c in all_contacts if c.get("email")}
    submitted_emails = {inv.get("contractor_email", "").lower() for inv in invoices}

    # 1. ç¿ç°ããè¨ç»å¤ã¨ã®éé¡ä¹é¢ãã§ãã¯ï¼10%ä»¥ä¸ï¼
    if saruta_ref:
        plan_excl = int(saruta_ref.get("amount_excl_tax", 0) or 0)
        if plan_excl > 0:
            diff_rate = abs(total_contractor_excl - plan_excl) / plan_excl
            diff_amount = total_contractor_excl - plan_excl
            sign = "+" if diff_amount >= 0 else ""
            if diff_rate >= 0.10:
                warnings.append(
                    f"â ï¸ *éé¡ä¹é¢ {diff_rate*100:.1f}%*: "
                    f"è¨ç»å¤ Â¥{plan_excl:,} ã«å¯¾ãã¦å®ç¸¾ Â¥{total_contractor_excl:,} "
                    f"ï¼{sign}Â¥{diff_amount:,}ï¼"
                )

    # 2. å°å¸³ã«ãªãäººããã®è«æ±æ¸ãã§ãã¯
    for inv in invoices:
        inv_email = inv.get("contractor_email", "").lower()
        if inv_email and inv_email not in registered_emails:
            warnings.append(
                f"â ï¸ *å°å¸³æªç»é²*: `{inv_email}` "
                f"({inv.get('contractor_name', 'ä¸æ')}) ã¯å°å¸³ã«å­å¨ãã¾ãã"
            )

    # 3. å°å¸³ã«ããã®ã«è«æ±æ¸ãæªæåºã®äºº
    missing_invoices = [
        c for c in all_contacts
        if c.get("email", "").lower() not in submitted_emails
    ]
    if missing_invoices:
        missing_names = ", ".join(
            c.get("name", c.get("email", "?")) for c in missing_invoices
        )
        warnings.append(f"â³ *æªæåº*: {missing_names}")

    # 4. å°å¸³ã«ããã®ã«ç¿ç°ããã®è¨ç»å¤ã®äººæ°ã«ã¾ãã¦ããªã
    if saruta_ref:
        saruta_person_count = int(saruta_ref.get("person_count", 0) or 0)
        ledger_count = len(all_contacts)
        if saruta_person_count > 0 and ledger_count > saruta_person_count:
            extra_count = ledger_count - saruta_person_count
            warnings.append(
                f"â ï¸ *"
                f"è¨ç»å¤ã®äººæ°ä¸ä¹ã*"
                f": å°å¸³ã«ã¯ {ledger_count} åãã¾ããã"
                f"ç¿ç°ããã®è¨ç»å¤ã¯ {saruta_person_count} ååã§ãã"
                f"å°å¸³ã® {extra_count} åãè¨ç»å¤ã«ä¹ã¾ãã¦ããªãå¯è½æ§ãããã¾ãã"
            )

    return warnings


# âââ Slack Block Kit builders âââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _progress_bar(done: int, total: int, width: int = 10) -> str:
    filled = int(done * width / max(total, 1))
    return "â" * filled + "â" * (width - filled)


def build_saruta_receipt_blocks(
    amount_excl_tax: int,
    amount_incl_tax: int,
    billing_month: str,
    person_count: int = 0,
    pdf_drive_url: str = None,
) -> List[dict]:
    """ç¿ç°ããï¼è¨ç»å¤ï¼è«æ±æ¸åé éç¥ã®Blocks"""
    person_text = f"{person_count} åå" if person_count > 0 else "äººæ°ä¸æ"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "ð ã¢ã¤ãã£ãªããè¨ç»å¤ åé "}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*éä»è*\nç¿ç°ããï¼{SARUTA_EMAIL}ï¼"},
                {"type": "mrkdwn", "text": f"*è«æ±æ*\n{billing_month}"},
                {"type": "mrkdwn", "text": f"*éé¡ï¼ç¨æï¼*\nÂ¥{amount_excl_tax:,}"},
                {"type": "mrkdwn", "text": f"*éé¡ï¼ç¨è¾¼ï¼*\nÂ¥{amount_incl_tax:,}"},
                {"type": "mrkdwn", "text": f"*è¨ä¸äººæ°*\n{person_text}"},
            ],
        },
    ]
    if pdf_drive_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"ð <{pdf_drive_url}|è¨ç»å¤PDFãéã>"},
        })
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "â¹ï¸ ãã®è«æ±æ¸ã¯ *è¨ç»å¤* ã¨ãã¦è¨é²ããã¾ããã\n"
                "æ¥­åå§è¨èã®å®ç¸¾ãå±ãæ¬¡ç¬¬ãåç®è«æ±æ¸ä½ææã«ç§åãã¾ãã"
            ),
        },
    })
    return blocks


def build_receipt_blocks(
    contractor_name: str,
    amount_incl_tax: int,
    billing_month: str,
    received_count: int,
    total_count: int,
    pdf_drive_url: str = None,
) -> List[dict]:
    """æ¥­åå§è¨è«æ±æ¸åé éç¥ã®Blocks"""
    pct = int(received_count * 100 / max(total_count, 1))
    bar = _progress_bar(received_count, total_count)
    all_received = received_count >= total_count
    status_icon = "â" if all_received else "â³"

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "ð æ¥­åå§è¨è«æ±æ¸ åé "}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*å§è¨è*\n{contractor_name}"},
                {"type": "mrkdwn", "text": f"*éé¡ï¼ç¨è¾¼ï¼*\nÂ¥{amount_incl_tax:,}"},
                {"type": "mrkdwn", "text": f"*è«æ±æ*\n{billing_month}"},
            ],
        },
    ]
    if pdf_drive_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"ð <{pdf_drive_url}}è«æ±æ¸PDFãéã>"},
        })
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"{status_icon} *åé ç¶æ³*: `{bar}` {received_count}/{total_count} ä»¶ ({pct}%)",
        },
    })

    if all_received:
        blocks += [
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"ð *å¨ {total_count} ä»¶ã®è«æ±æ¸ãæãã¾ããï¼*\n"
                        f"{IDIOTT_COMPANY_NAME}åãåç®è«æ±æ¸ãä½æã§ãã¾ãã"
                    ),
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "ð è«æ±æ¸ä½æ", "emoji": True},
                        "style": "primary",
                        "action_id": "idiott_create_invoice",
                        "value": billing_month,
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "ð ä¸æ¬ä½æ", "emoji": True},
                        "action_id": "idiott_create_invoice_bulk",
                        "value": billing_month,
                    },
                ],
            },
        ]
    return blocks


def build_invoice_preview_blocks(
    billing_month: str,
    invoices: List[dict],
    total_contractor_incl: int,
    management_fee_excl: int,
    management_fee_incl: int,
    grand_total_incl: int,
    tax_amount: int,
    saruta_ref: Optional[dict] = None,
    warnings: List[str] = None,
) -> List[dict]:
    """åç®è«æ±æ¸ãã¬ãã¥ã¼ã®Blocksï¼ç¿ç°ããè¨ç»å¤æ¯è¼ã»è­¦åä»ãï¼"""
    items_text = "\n".join(
        f"â¢ {inv['contractor_name']}: Â¥{int(inv.get('amount_incl_tax', 0)):,}"
        for inv in invoices
    )
    num = len(invoices)

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "ð§¾ åç®è«æ±æ¸ ãã¬ãã¥ã¼"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*è«æ±å*\n{IDIOTT_COMPANY_NAME}"},
                {"type": "mrkdwn", "text": f"*è«æ±å*\n{AO_COMPANY_NAME}"},
                {"type": "mrkdwn", "text": f"*è«æ±æ*\n{billing_month}"},
                {"type": "mrkdwn", "text": f"*æ¥­åå§è¨èæ°*\n{num} å"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*ãæç´°ãæ¥­åå§è¨è²» ç«æ¿åï¼ç¨è¾¼åè¨: Â¥{total_contractor_incl:,}ï¼*\n{items_text}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*ãæç´°ãç®¡çææ°æ*\n"
                    f"â¢ {num}å Ã Â¥{MANAGEMENT_FEE_PER_PERSON:,} = Â¥{management_fee_excl:,}ï¼ç¨æï¼"
                    f" â Â¥{management_fee_incl:,}ï¼ç¨è¾¼ï¼"
                ),
            },
        },
        {"type": "divider"},
    ]

    # ç¿ç°ããè¨ç»å¤ã¨ã®æ¯è¼
    if saruta_ref:
        plan_excl = int(saruta_ref.get("amount_excl_tax", 0) or 0)
        plan_incl = int(saruta_ref.get("amount_incl_tax", 0) or 0)
        saruta_person_count = int(saruta_ref.get("person_count", 0) or 0)
        total_contractor_excl = sum(int(inv.get("amount_excl_tax", 0) or 0) for inv in invoices)
        diff = total_contractor_excl - plan_excl
        sign = "+" if diff >= 0 else ""
        diff_rate = abs(diff) / plan_excl * 100 if plan_excl > 0 else 0
        saruta_pdf = saruta_ref.get("pdf_drive_url", "")
        saruta_link = f"<{saruta_pdf}|è¨ç»å¤PDF>" if saruta_pdf else "è¨ç»å¤PDFæªç»é²"
        person_info = f"ï¼è¨ä¸äººæ°: {saruta_person_count}åï¼" if saruta_person_count > 0 else ""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*ãç¿ç°ããè¨ç»å¤ã¨ã®ç§åã*\n"
                    f"â¢ ç¿ç°ããè¨ç»å¤ï¼ç¨æï¼: Â¥{plan_excl:,}{person_info}ï¼{saruta_link}ï¼\n"
                    f"â¢ å®ç¸¾åè¨ï¼ç¨æï¼: Â¥{total_contractor_excl:,}ï¼{num}åï¼\n"
                    f"â¢ å·®ç°: {sign}Â¥{diff:,}ï¼{diff_rate:.1f}%ï¼"
                ),
            },
        })
        blocks.append({"type": "divider"})
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "â¹ï¸ *ç¿ç°ããã®è¨ç»å¤ãã¾ã å±ãã¦ãã¾ããã* ç§åãªãã§ç»é²ãã¾ãã",
            },
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*f¥­åå§è¨è²»åè¨ï¼ç¨è¾¼ï¼*\nÂ¥{total_contractor_incl:,}"},
            {"type": "mrkdwn", "text": f"*ç®¡çææ°æï¼ç¨è¾¼ï¼*\nÂ¥{management_fee_incl:,}"},
            {"type": "mrkdwn", "text": f"*æ¶è²»ç¨ï¼10%ï¼*\nÂ¥{tax_amount:,}"},
            {"type": "mrkdwn", "text": f"*åè¨è«æ±é¡ï¼ç¨è¾¼ï¼*\n:money_with_wings: *Â¥{grand_total_incl:,}*"},
        ],
    })

    # è­¦åã»ã¯ã·ã§ã³
    if warnings:
        warning_text = "\n".join(warnings)
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*â ï¸ ç¢ºèªäºé *\n{warning_text}",
            },
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "åå®¹ãç¢ºèªãã¦ *freee ã«ç»é²* ããæ ªå¼ä¼ç¤¾ã¢ã¤ãã£ãªããã¸è«æ±æ¸ãéä»ãã¾ãã",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "â Freeeç»é² & éä»", "emoji": True},
                    "style": "primary",
                    "action_id": "idiott_freee_register",
                    "value": billing_month,
                    "confirm": {
                        "title": {"type": "plain_text", "text": "freeeç»é²ã®ç¢ºèª"},
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*Â¥{grand_total_incl:,}* ã®è«æ±æ¸ã freee ã«ç»é²ãã\n"
                                f"{IDIOTT_COMPANY_NAME} ã¸éä»ãã¾ãã"
                            ),
                        },
                        "confirm": {"type": "plain_text", "text": "ç»é²ãã"},
                        "deny": {"type": "plain_text", "text": "ã­ã£ã³ã»ã«"},
                    },
                },
            ],
        },
    ]
    return blocks


# âââ Main flow ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async def process_saruta_invoice(
    slack_client,
    analysis: dict,
    pdf_data: bytes,
    drive_handler,
) -> None:
    """ç¿ç°ããããã®è«æ±æ¸ï¼è¨ç»å¤ï¼ã®åé å¦ç"""
    billing_month = get_billing_month()
    amount_incl_tax = int(analysis.get("amount_incl_tax", 0) or 0)
    amount_excl_tax = int(analysis.get("amount_excl_tax", 0) or 0)
    if amount_excl_tax == 0 and amount_incl_tax > 0:
        amount_excl_tax = int(amount_incl_tax / 1.1)
    # Claude AI ãè«æ±æ¸ããæ½åºããäººæ°ï¼è¨è¼ãããã°ï¼
    person_count = int(analysis.get("person_count", 0) or 0)

    # Google Drive ã« PDF ä¿å­
    pdf_drive_url = None
    try:
        file_meta = await drive_handler.upload_invoice(
            pdf_data,
            f"IDT_Saruta_{billing_month}.pdf",
            datetime.now(JST),
            vendor_name="IDT_Saruta_è¨ç»å¤",
        )
        if file_meta and file_meta.get("id"):
            pdf_drive_url = f"https://drive.google.com/file/d/{file_meta['id']}/view"
    except Exception as e:
        logger.warning(f"Drive upload failed for saruta invoice: {e}")

    # Supabase ã«ä¿å­
    await store_saruta_reference(
        billing_month=billing_month,
        amount_excl_tax=amount_excl_tax,
        amount_incl_tax=amount_incl_tax,
        person_count=person_count,
        pdf_drive_url=pdf_drive_url,
    )

    # Slack ã¸éç¥
    blocks = build_saruta_receipt_blocks(
        amount_excl_tax=amount_excl_tax,
        amount_incl_tax=amount_incl_tax,
        billing_month=billing_month,
        person_count=person_count,
        pdf_drive_url=pdf_drive_url,
    )
    try:
        slack_client.chat_postMessage(
            channel=SLACK_IDIOTT_CHANNEL_ID,
            text=f"ð è¨ç»å¤åé : ç¿ç°ãã {billing_month} Â¥{amount_excl_tax:,}ï¼ç¨æï¼",
            blocks=blocks,
        )
        logger.info(f"Saruta reference posted: Â¥{amount_excl_tax:,} excl tax, {person_count}å for {billing_month}")
    except Exception as e:
        logger.error(f"Failed to post saruta reference to Slack: {e}")


async def process_contractor_invoice(
    slack_client,
    sender_email: str,
    contractor_name: str,
    analysis: dict,
    pdf_data: bytes,
    drive_handler,
) -> None:
    """æ­åå§è¨è«æ±æ¸ã®åé å¦çã¡ã¤ã³"""
    billing_month = get_billing_month()
    amount_incl_tax = int(analysis.get("amount_incl_tax", 0) or 0)
    amount_excl_tax = int(analysis.get("amount_excl_tax", 0) or 0)
    if amount_excl_tax == 0 and amount_incl_tax > 0:
        amount_excl_tax = int(amount_incl_tax / 1.1)

    # Google Drive ã« PDF ä¿å­
    pdf_drive_url = None
    try:
        file_meta = await drive_handler.upload_invoice(
            pdf_data,
            f"IDT_{contractor_name}_{billing_month}.pdf",
            datetime.now(JST),
            vendor_name=f"IDT_{contractor_name}",
        )
        if file_meta and file_meta.get("id"):
            pdf_drive_url = f"https://drive.google.com/file/d/{file_meta['id']}/view"
    except Exception as e:
        logger.warning(f"Drive upload failed for idiott invoice: {e}")

    # Supabase ã«ä¿å­ï¼UPSERTï¼
    await store_contractor_invoice(
        billing_month=billing_month,
        contractor_email=sender_email,
        contractor_name=contractor_name or sender_email,
        amount_excl_tax=amount_excl_tax,
        amount_incl_tax=amount_incl_tax,
        pdf_drive_url=pdf_drive_url,
    )

    # é²æç¢ºèª
    invoices = await get_month_invoices(billing_month)
    total_count = await get_idiott_contacts_count()
    received_count = len(invoices)

    # Slack ã¸éç¥
    blocks = build_receipt_blocks(
        contractor_name=contractor_name or sender_email,
        amount_incl_tax=amount_incl_tax,
        billing_month=billing_month,
        received_count=received_count,
        total_count=total_count,
        pdf_drive_url=pdf_drive_url,
    )
    try:
        slack_client.chat_postMessage(
            channel=SLACK_IDIOTT_CHANNEL_ID,
            text=f"ð æ¥­åå§è¨è«æ±æ¸åé : {contractor_name} Â¥{amount_incl_tax:,} ({received_count}/{total_count}ä»¶)",
            blocks=blocks,
        )
        logger.info(f"Idiott invoice posted: {sender_email} Â¥{amount_incl_tax:,} ({received_count}/{total_count})")
    except Exception as e:
        logger.error(f"Failed to post idiott invoice to Slack: {e}")


async def handle_create_invoice(
    slack_client,
    channel: str,
    message_ts: str,
    billing_month: str,
    user_id: str,
) -> None:
    """ãè«æ±æ¸ä½æããä¸æ¬ä½æããã¿ã³å¦ç"""
    invoices = await get_month_invoices(billing_month)
    if not invoices:
        slack_client.chat_postEphemeral(
            channel=channel,
            user=user_id,
            text=f"â ï¸ {billing_month} ã®åé æ¸ã¿è«æ±æ¸ãããã¾ããã",
        )
        return

    # éé¡è¨ç®
    total_contractor_incl = sum(int(inv.get("amount_incl_tax", 0) or 0) for inv in invoices)
    total_contractor_excl = sum(int(inv.get("amount_excl_tax", 0) or 0) for inv in invoices)
    num = len(invoices)
    management_fee_excl = num * MANAGEMENT_FEE_PER_PERSON
    management_fee_tax = int(management_fee_excl * 0.1)
    management_fee_incl = management_fee_excl + management_fee_tax
    grand_total_incl = total_contractor_incl + management_fee_incl
    grand_total_excl = total_contractor_excl + management_fee_excl
    total_tax = grand_total_incl - grand_total_excl

    # ç¿ç°ããè¨ç»å¤ã®åå¾
    saruta_ref = await get_saruta_reference(billing_month)

    # è­¦åãã§ãã¯ï¼éé¡ä¹é¢ã»å°å¸³æªç»é²ã»æªæåºã»è¨ç»å¤äººæ°ä¸è¶³ï¼
    warnings = await check_warnings(
        billing_month=billing_month,
        invoices=invoices,
        saruta_ref=saruta_ref,
        total_contractor_excl=total_contractor_excl,
    )

    blocks = build_invoice_preview_blocks(
        billing_month=billing_month,
        invoices=invoices,
        total_contractor_incl=total_contractor_incl,
        management_fee_excl=management_fee_excl,
        management_fee_incl=management_fee_incl,
        grand_total_incl=grand_total_incl,
        tax_amount=total_tax,
        saruta_ref=saruta_ref,
        warnings=warnings,
    )

    try:
        slack_client.chat_postMessage(
            channel=channel,
            text=f"ð§¾ {billing_month} åç®è«æ±æ¸ãã¬ãã¥ã¼ï¼åè¨: Â¥{grand_total_incl:,}ï¼",
            blocks=blocks,
        )
        logger.info(f"Invoice preview posted for {billing_month}: Â¥{grand_total_incl:,}")
    except Exception as e:
        logger.error(f"Failed to post invoice preview: {e}")


async def handle_freee_register(
    slack_client,
    freee_handler,
    channel: str,
    message_ts: str,
    billing_month: str,
    user_id: str,
) -> None:
    """ãFreeeç»é²ããã¿ã³å¦ç"""
    invoices = await get_month_invoices(billing_month)
    if not invoices:
        slack_client.chat_postEphemeral(
            channel=channel,
            user=user_id,
            text=f"â ï¸ {billing_month} ã®åé æ¸ã¿è«æ±æ¸ãããã¾ããã",
        )
        return

    # éé¡åè¨ç®
    total_contractor_incl = sum(int(inv.get("amount_incl_tax", 0) or 0) for inv in invoices)
    num = len(invoices)
    management_fee_excl = num * MANAGEMENT_FEE_PER_PERSON
    management_fee_incl = int(management_fee_excl * 1.1)
    grand_total_incl = total_contractor_incl + management_fee_incl

    try:
        from .idiott_freee import create_idiott_invoice_sync
        import asyncio

        freee_invoice_id = await asyncio.to_thread(
            create_idiott_invoice_sync,
            billing_month=billing_month,
            invoices=invoices,
            management_fee_excl=management_fee_excl,
            grand_total_incl=grand_total_incl,
        )

        # Supabase ã¹ãã¼ã¿ã¹æ´æ°
        await mark_month_registered(billing_month, str(freee_invoice_id))

        # Slack ãå®äºã¡ãã»ã¼ã¸ã«æ´æ°
        slack_client.chat_update(
            channel=channel,
            ts=message_ts,
            text=f"â freee è«æ±æ¸ç»é²å®äºï¼ID: {freee_invoice_id}ï¼",
            blocks=[
                {"type": "header", "text": {"type": "plain_text", "text": "â freee ç»é²å®äº"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*è«æ±æ*\n{billing_month}"},
                        {"type": "mrkdwn", "text": f"*åè¨éé¡ï¼ç¨è¾¼ï¼*\nÂ¥{grand_total_incl:,}"},
                        {"type": "mrkdwn", "text": f"*freee è«æ±æ¸ID*\n{freee_invoice_id}"},
                        {"type": "mrkdwn", "text": f"*ç»é²è*\n<@{user_id}>"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"freee ãã {IDIOTT_COMPANY_NAME} ã¸è«æ±æ¸ãéä»ããã¾ããã",
                    },
                },
            ],
        )
        logger.info(f"freee invoice registered: ID={freee_invoice_id}, Â¥{grand_total_incl:,}")
    except Exception as e:
        logger.error(f"Failed to register idiott invoice in freee: {e}", exc_info=True)
        slack_client.chat_postEphemeral(
            channel=channel,
            user=user_id,
            text=f"â freee ç»é²ã«å¤±æãã¾ãã: {str(e)[:300]}",
        )
