"""
アイディオット業務委託請求書フロー
- 業務委託者からの請求書PDF受領 → Supabase記録 → Slack通知
- 全員分揃ったら「請求書作成」ボタン表示
- 請求書プレビュー → 「Freee登録」ボタン
- Freeeに売上請求書登録
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
MANAGEMENT_FEE_PER_PERSON = 5000  # 税抜 5,000円/人
AO_COMPANY_NAME = "一般社団法人アソシエーションオフィス"
IDIOTT_COMPANY_NAME = "株式会社アイディオット"


def get_billing_month() -> str:
    """今月の YYYY-MM を返す"""
    return datetime.now(JST).strftime("%Y-%m")


# ─── Supabase REST helper ────────────────────────────────────────────────────

async def _sb_get(table: str, params: dict = None) -> list:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase not configured")
        return []
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{table}", params=params, headers=headers, timeout=10
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
            f"{SUPABASE_URL}/rest/v1/{table}", json=data, headers=headers, timeout=10
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
            f"{SUPABASE_URL}/rest/v1/{table}", params=params, json=data, headers=headers, timeout=10
        )
        return r.status_code < 400


# ─── Data access ─────────────────────────────────────────────────────────────

async def is_idiott_contact(email: str) -> bool:
    """idiott_contacts にメールアドレスが存在するか確認"""
    r = await _sb_get("idiott_contacts", {"email": f"ilike.{email}", "select": "id", "limit": "1"})
    return len(r) > 0


async def get_idiott_contacts_count() -> int:
    """idiott_contacts の総件数"""
    r = await _sb_get("idiott_contacts", {"select": "id"})
    return len(r)


async def get_month_invoices(billing_month: str) -> List[dict]:
    """指定月の受領済み請求書一覧"""
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
    """受領請求書をSupabaseにUPSERT"""
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
    """月次請求書のステータスをfreee登録済みに更新"""
    await _sb_patch(
        "idiott_invoices",
        {"billing_month": f"eq.{billing_month}"},
        {"status": "freee_registered", "freee_invoice_id": freee_invoice_id},
    )


# ─── Slack Block Kit builders ─────────────────────────────────────────────────

def _progress_bar(done: int, total: int, width: int = 10) -> str:
    filled = int(done * width / max(total, 1))
    return "█" * filled + "░" * (width - filled)


def build_receipt_blocks(
    contractor_name: str,
    amount_incl_tax: int,
    billing_month: str,
    received_count: int,
    total_count: int,
    pdf_drive_url: str = None,
) -> List[dict]:
    """業務委託請求書受領通知のBlocks"""
    pct = int(received_count * 100 / max(total_count, 1))
    bar = _progress_bar(received_count, total_count)
    all_received = received_count >= total_count
    status_icon = "✅" if all_received else "⏳"

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📄 業務委託請求書 受領"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*委託者*\n{contractor_name}"},
                {"type": "mrkdwn", "text": f"*金額（税込）*\n¥{amount_incl_tax:,}"},
                {"type": "mrkdwn", "text": f"*請求月*\n{billing_month}"},
            ],
        },
    ]
    if pdf_drive_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"📎 <{pdf_drive_url}|請求書PDFを開く>"},
        })
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"{status_icon} *受領状況*: `{bar}` {received_count}/{total_count} 件 ({pct}%)",
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
                        f"🎉 *全 {total_count} 件の請求書が揃いました！*\n"
                        f"{IDIOTT_COMPANY_NAME}向け合算請求書を作成できます。"
                    ),
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📋 請求書作成", "emoji": True},
                        "style": "primary",
                        "action_id": "idiott_create_invoice",
                        "value": billing_month,
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📋 一括作成", "emoji": True},
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
) -> List[dict]:
    """合算請求書プレビューのBlocks"""
    items_text = "\n".join(
        f"• {inv['contractor_name']}: ¥{int(inv.get('amount_incl_tax', 0)):,}"
        for inv in invoices
    )
    num = len(invoices)
    return [
        {"type": "header", "text": {"type": "plain_text", "text": "🧾 合算請求書 プレビュー"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*請求先*\n{IDIOTT_COMPANY_NAME}"},
                {"type": "mrkdwn", "text": f"*請求元*\n{AO_COMPANY_NAME}"},
                {"type": "mrkdwn", "text": f"*請求月*\n{billing_month}"},
                {"type": "mrkdwn", "text": f"*業務委託者数*\n{num} 名"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*【明細】業務委託費 立替分（税込合計: ¥{total_contractor_incl:,}）*\n{items_text}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*【明細】管理手数料*\n"
                    f"• {num}名 × ¥{MANAGEMENT_FEE_PER_PERSON:,} = ¥{management_fee_excl:,}（税抜）"
                    f"  →  ¥{management_fee_incl:,}（税込）"
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*業務委託費合計（税込）*\n¥{total_contractor_incl:,}"},
                {"type": "mrkdwn", "text": f"*管理手数料（税込）*\n¥{management_fee_incl:,}"},
                {"type": "mrkdwn", "text": f"*消費税（10%）*\n¥{tax_amount:,}"},
                {"type": "mrkdwn", "text": f"*合計請求額（税込）*\n:money_with_wings: *¥{grand_total_incl:,}*"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "内容を確認して *freee に登録* し、株式会社アイディオットへ請求書を送付します。",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Freee登録 & 送付", "emoji": True},
                    "style": "primary",
                    "action_id": "idiott_freee_register",
                    "value": billing_month,
                    "confirm": {
                        "title": {"type": "plain_text", "text": "freee登録の確認"},
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*¥{grand_total_incl:,}* の請求書を freee に登録し、\n"
                                f"{IDIOTT_COMPANY_NAME} へ送付します。"
                            ),
                        },
                        "confirm": {"type": "plain_text", "text": "登録する"},
                        "deny": {"type": "plain_text", "text": "キャンセル"},
                    },
                },
            ],
        },
    ]


# ─── Main flow ────────────────────────────────────────────────────────────────

async def process_contractor_invoice(
    slack_client,
    sender_email: str,
    contractor_name: str,
    analysis: dict,
    pdf_data: bytes,
    drive_handler,
) -> None:
    """業務委託請求書の受領処理メイン"""
    billing_month = get_billing_month()

    amount_incl_tax = int(analysis.get("amount_incl_tax", 0) or 0)
    amount_excl_tax = int(analysis.get("amount_excl_tax", 0) or 0)
    if amount_excl_tax == 0 and amount_incl_tax > 0:
        amount_excl_tax = int(amount_incl_tax / 1.1)

    # Google Drive に PDF 保存
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

    # Supabase に保存（UPSERT）
    await store_contractor_invoice(
        billing_month=billing_month,
        contractor_email=sender_email,
        contractor_name=contractor_name or sender_email,
        amount_excl_tax=amount_excl_tax,
        amount_incl_tax=amount_incl_tax,
        pdf_drive_url=pdf_drive_url,
    )

    # 進捗確認
    invoices = await get_month_invoices(billing_month)
    total_count = await get_idiott_contacts_count()
    received_count = len(invoices)

    # Slack へ通知
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
            text=f"📄 業務委託請求書受領: {contractor_name} ¥{amount_incl_tax:,} ({received_count}/{total_count}件)",
            blocks=blocks,
        )
        logger.info(f"Idiott invoice posted: {sender_email} ¥{amount_incl_tax:,} ({received_count}/{total_count})")
    except Exception as e:
        logger.error(f"Failed to post idiott invoice to Slack: {e}")


async def handle_create_invoice(
    slack_client,
    channel: str,
    message_ts: str,
    billing_month: str,
    user_id: str,
) -> None:
    """「請求書作成」「一括作成」ボタン処理"""
    invoices = await get_month_invoices(billing_month)
    if not invoices:
        slack_client.chat_postEphemeral(
            channel=channel, user=user_id,
            text=f"⚠️ {billing_month} の受領済み請求書がありません。",
        )
        return

    # 金額計算
    total_contractor_incl = sum(int(inv.get("amount_incl_tax", 0) or 0) for inv in invoices)
    total_contractor_excl = sum(int(inv.get("amount_excl_tax", 0) or 0) for inv in invoices)
    num = len(invoices)
    management_fee_excl = num * MANAGEMENT_FEE_PER_PERSON
    management_fee_tax = int(management_fee_excl * 0.1)
    management_fee_incl = management_fee_excl + management_fee_tax
    grand_total_incl = total_contractor_incl + management_fee_incl
    grand_total_excl = total_contractor_excl + management_fee_excl
    total_tax = grand_total_incl - grand_total_excl

    blocks = build_invoice_preview_blocks(
        billing_month=billing_month,
        invoices=invoices,
        total_contractor_incl=total_contractor_incl,
        management_fee_excl=management_fee_excl,
        management_fee_incl=management_fee_incl,
        grand_total_incl=grand_total_incl,
        tax_amount=total_tax,
    )
    try:
        slack_client.chat_postMessage(
            channel=channel,
            text=f"🧾 {billing_month} 合算請求書プレビュー（合計: ¥{grand_total_incl:,}）",
            blocks=blocks,
        )
        logger.info(f"Invoice preview posted for {billing_month}: ¥{grand_total_incl:,}")
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
    """「Freee登録」ボタン処理"""
    invoices = await get_month_invoices(billing_month)
    if not invoices:
        slack_client.chat_postEphemeral(
            channel=channel, user=user_id,
            text=f"⚠️ {billing_month} の受領済み請求書がありません。",
        )
        return

    # 金額再計算
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

        # Supabase ステータス更新
        await mark_month_registered(billing_month, str(freee_invoice_id))

        # Slack を完了メッセージに更新
        slack_client.chat_update(
            channel=channel,
            ts=message_ts,
            text=f"✅ freee 請求書登録完了（ID: {freee_invoice_id}）",
            blocks=[
                {"type": "header", "text": {"type": "plain_text", "text": "✅ freee 登録完了"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*請求月*\n{billing_month}"},
                        {"type": "mrkdwn", "text": f"*合計金額（税込）*\n¥{grand_total_incl:,}"},
                        {"type": "mrkdwn", "text": f"*freee 請求書ID*\n{freee_invoice_id}"},
                        {"type": "mrkdwn", "text": f"*登録者*\n<@{user_id}>"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"freee から {IDIOTT_COMPANY_NAME} へ請求書が送付されました。",
                    },
                },
            ],
        )
        logger.info(f"freee invoice registered: ID={freee_invoice_id}, ¥{grand_total_incl:,}")

    except Exception as e:
        logger.error(f"Failed to register idiott invoice in freee: {e}", exc_info=True)
        slack_client.chat_postEphemeral(
            channel=channel, user=user_id,
            text=f"❌ freee 登録に失敗しました: {str(e)[:300]}",
        )
