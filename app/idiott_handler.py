"""
アイディオット業務委託請求書フロー
- 業務委託者からな書PDF受領 → Supabase記録 → Slack通知
- 全員分揃のがもったら「請求書作成」ボタン表示
- 請求書プレビュー（猿田さん計画値比較・警告付お）
- 「Freee登録」ボタン → Freeeに売上請求書登録
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
SARUTA_EMAIL = "saruta@aidiot.jp"


def get_billing_month() -> str:
    """今月の YYYY-MM を返す"""
    return datetime.now(JST).strftime("%Y-%m")


# ─── Supabase REST helper ──────────────────────────────────────────────────────

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


# ─── Data access ────────────────────────────────────────────────────────────────

async def is_idiott_contact(email: str) -> bool:
    """idiott_contacts にメールアドレスが存在するか確認"""
    r = await _sb_get("idiott_contacts", {"email": f"ilike.{email}", "select": "id", "limit": "1"})
    return len(r) > 0


async def get_idiott_contacts_count() -> int:
    """idiott_contacts の総件数"""
    r = await _sb_get("idiott_contacts", {"select": "id"})
    return len(r)


async def get_all_idiott_contacts() -> List[dict]:
    """idiott_contacts の全件取得（名前・メール）"""
    return await _sb_get("idiott_contacts", {"select": "id,name,email"})


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


async def store_saruta_reference(
    billing_month: str,
    amount_excl_tax: int,
    amount_incl_tax: int,
    person_count: int = 0,
    pdf_drive_url: str = None,
) -> Optional[dict]:
    """猿田さんの請求書を計画値としてSupabaseに保存"""
    return await _sb_upsert("idiott_saruta_refs", {
        "billing_month": billing_month,
        "amount_excl_tax": amount_excl_tax,
        "amount_incl_tax": amount_incl_tax,
        "person_count": person_count,
        "pdf_drive_url": pdf_drive_url,
    })


async def get_saruta_reference(billing_month: str) -> Optional[dict]:
    """指定月の猿田さん計画値を取得"""
    r = await _sb_get("idiott_saruta_refs", {
        "billing_month": f"eq.{billing_month}",
        "select": "*",
        "limit": "1",
    })
    return r[0] if r else None


# ─── Warning logic ───────────────────────────────────────────────────────────────

async def check_warnings(
    billing_month: str,
    invoices: List[dict],
    saruta_ref: Optional[dict],
    total_contractor_excl: int,
) -> List[str]:
    """
    警告チェック。以下の場合に警告を返す:
    1. 金額が猿田さんの計画値と10%以上ずれている
    2. 台帳（idiott_contacts）にないメールアドレスからの請求書がある
    3. 台帳に存在する人が未提出（まだ請求書が届いていない）
    4. 台帳にいるのに猿田さんの請求書の人数に含まれていない
    """
    warnings = []
    all_contacts = await get_all_idiott_contacts()
    registered_emails = {c["email"].lower() for c in all_contacts if c.get("email")}
    submitted_emails = {inv.get("contractor_email", "").lower() for inv in invoices}

    # 1. 猿田さん計画値との金額乖離チェック（10%以上）
    if saruta_ref:
        plan_excl = int(saruta_ref.get("amount_excl_tax", 0) or 0)
        if plan_excl > 0:
            diff_rate = abs(total_contractor_excl - plan_excl) / plan_excl
            diff_amount = total_contractor_excl - plan_excl
            sign = "+" if diff_amount >= 0 else ""
            if diff_rate >= 0.10:
                warnings.append(
                    f"⚠️ *金額乖離 {diff_rate*100:.1f}%*: "
                    f"計画値 ¥{plan_excl:,} に対して実績 ¥{total_contractor_excl:,} "
                    f"（{sign}¥{diff_amount:,}）"
                )

    # 2. 台帳にない人からの請求書チェック
    for inv in invoices:
        inv_email = inv.get("contractor_email", "").lower()
        if inv_email and inv_email not in registered_emails:
            warnings.append(
                f"⚠️ *台帳未登録*: `{inv_email}` "
                f"({inv.get('contractor_name', '不明')}) は台帳に存在しません"
            )

    # 3. 台帳にいるのに請求書が未提出の人
    missing_invoices = [
        c for c in all_contacts
        if c.get("email", "").lower() not in submitted_emails
    ]
    if missing_invoices:
        missing_names = ", ".join(
            c.get("name", c.get("email", "?")) for c in missing_invoices
        )
        warnings.append(f"⏳ *未提出*: {missing_names}")

    # 4. 台帳にいるのに猿田さんの計画値の人数に��まれていない
    if saruta_ref:
        saruta_person_count = int(saruta_ref.get("person_count", 0) or 0)
        ledger_count = len(all_contacts)
        if saruta_person_count > 0 and ledger_count > saruta_person_count:
            extra_count = ledger_count - saruta_person_count
            warnings.append(
                f"⚠️ *"
                f"計画値の人数不之じ*"
                f": 台帳には {ledger_count} 名いますが、"
                f"猿田さんの計画値は {saruta_person_count} 名分です。"
                f"台帳の {extra_count} 名が計画値に之まれていない可能性があります。"
            )

    return warnings


# ─── Slack Block Kit builders ─────────────────────────────────────────────────────

def _progress_bar(done: int, total: int, width: int = 10) -> str:
    filled = int(done * width / max(total, 1))
    return "█" * filled + "░" * (width - filled)


def build_saruta_receipt_blocks(
    amount_excl_tax: int,
    amount_incl_tax: int,
    billing_month: str,
    person_count: int = 0,
    pdf_drive_url: str = None,
) -> List[dict]:
    """猿田さん（計画値）請求書受領通知のBlocks"""
    person_text = f"{person_count} 名分" if person_count > 0 else "人数不明"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📊 アイディオット計画値 受領"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*送付者*\n猿田さん（{SARUTA_EMAIL}）"},
                {"type": "mrkdwn", "text": f"*請求月*\n{billing_month}"},
                {"type": "mrkdwn", "text": f"*金額（税抜）*\n¥{amount_excl_tax:,}"},
                {"type": "mrkdwn", "text": f"*金額（税込）*\n¥{amount_incl_tax:,}"},
                {"type": "mrkdwn", "text": f"*計上人数*\n{person_text}"},
            ],
        },
    ]
    if pdf_drive_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"📎 <{pdf_drive_url}|計画値PDFを開く>"},
        })
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "ℹ️ この請求書は *計画値* として記録されました。\n"
                "業務委託者の実績が届き次第、合算請求書作成時に照合します。"
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
    saruta_ref: Optional[dict] = None,
    warnings: List[str] = None,
) -> List[dict]:
    """合算請求書プレビューのBlocks（猿田さん計画値比較・警告付ぎ）"""
    items_text = "\n".join(
        f"• {inv['contractor_name']}: ¥{int(inv.get('amount_incl_tax', 0)):,}"
        for inv in invoices
    )
    num = len(invoices)

    blocks = [
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
                    f" → ¥{management_fee_incl:,}（税込）"
                ),
            },
        },
        {"type": "divider"},
    ]

    # 猿田さん計画値との比較
    if saruta_ref:
        plan_excl = int(saruta_ref.get("amount_excl_tax", 0) or 0)
        plan_incl = int(saruta_ref.get("amount_incl_tax", 0) or 0)
        saruta_person_count = int(saruta_ref.get("person_count", 0) or 0)
        total_contractor_excl = sum(int(inv.get("amount_excl_tax", 0) or 0) for inv in invoices)
        diff = total_contractor_excl - plan_excl
        sign = "+" if diff >= 0 else ""
        diff_rate = abs(diff) / plan_excl * 100 if plan_excl > 0 else 0
        saruta_pdf = saruta_ref.get("pdf_drive_url", "")
        saruta_link = f"<{saruta_pdf}|計画値PDF>" if saruta_pdf else "計画値PDF未登録"
        person_info = f"（計上人数: {saruta_person_count}名）" if saruta_person_count > 0 else ""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*【猿田さん計画値との照合】*\n"
                    f"• 猿田さん計画値（税抜）: ¥{plan_excl:,}{person_info}（{saruta_link}）\n"
                    f"• 実績合計（税抜）: ¥{total_contractor_excl:,}（{num}名）\n"
                    f"• 差異: {sign}¥{diff:,}（{diff_rate:.1f}%）"
                ),
            },
        })
        blocks.append({"type": "divider"})
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "ℹ️ *猿田さんの計画値がまだ届いていません。* 照合なしで登録します。",
            },
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*f��務委託費合計（税込）*\n¥{total_contractor_incl:,}"},
            {"type": "mrkdwn", "text": f"*管理手数料（税込）*\n¥{management_fee_incl:,}"},
            {"type": "mrkdwn", "text": f"*消費税（10%）*\n¥{tax_amount:,}"},
            {"type": "mrkdwn", "text": f"*合計請求額（税込）*\n:money_with_wings: *¥{grand_total_incl:,}*"},
        ],
    })

    # 警告セクション
    if warnings:
        warning_text = "\n".join(warnings)
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*⚠️ 確認事項*\n{warning_text}",
            },
        })

    blocks += [
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
    return blocks


# ─── Main flow ──────────────────────────────────────────────────────────────────────

async def process_saruta_invoice(
    slack_client,
    analysis: dict,
    pdf_data: bytes,
    drive_handler,
) -> None:
    """猿田さんからの請求書（計画値）の受領処理"""
    billing_month = get_billing_month()
    amount_incl_tax = int(analysis.get("amount_incl_tax", 0) or 0)
    amount_excl_tax = int(analysis.get("amount_excl_tax", 0) or 0)
    if amount_excl_tax == 0 and amount_incl_tax > 0:
        amount_excl_tax = int(amount_incl_tax / 1.1)
    # Claude AI が請求書から抽出した人数（記載があれば）
    person_count = int(analysis.get("person_count", 0) or 0)

    # Google Drive に PDF 保存
    pdf_drive_url = None
    try:
        file_meta = await drive_handler.upload_invoice(
            pdf_data,
            f"IDT_Saruta_{billing_month}.pdf",
            datetime.now(JST),
            vendor_name="IDT_Saruta_計画値",
        )
        if file_meta and file_meta.get("id"):
            pdf_drive_url = f"https://drive.google.com/file/d/{file_meta['id']}/view"
    except Exception as e:
        logger.warning(f"Drive upload failed for saruta invoice: {e}")

    # Supabase に保存
    await store_saruta_reference(
        billing_month=billing_month,
        amount_excl_tax=amount_excl_tax,
        amount_incl_tax=amount_incl_tax,
        person_count=person_count,
        pdf_drive_url=pdf_drive_url,
    )

    # Slack へ通知
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
            text=f"📊 計画値受領: 猿田さん {billing_month} ¥{amount_excl_tax:,}（税抜）",
            blocks=blocks,
        )
        logger.info(f"Saruta reference posted: ¥{amount_excl_tax:,} excl tax, {person_count}名 for {billing_month}")
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
    """抭務委託請求書の受領処理メイン"""
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
            channel=channel,
            user=user_id,
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

    # 猿田さん計画値の取得
    saruta_ref = await get_saruta_reference(billing_month)

    # 警告チェック（金額乖離・台帳未登録・未提出・計画値人数不足）
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
            channel=channel,
            user=user_id,
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
            channel=channel,
            user=user_id,
            text=f"❌ freee 登録に失敗しました: {str(e)[:300]}",
        )
