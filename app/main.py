"""
ao-invoice-automation
受信メールの請求書PDFを自動処理するシステム
Gmail Pub/Sub → Claude AI 分析 → Slack承認 → freee登録 + Google Drive保存
"""

import os
import json
import base64
import asyncio
import logging
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import hmac
import hashlib
import time

from .gmail_handler import GmailHandler
from .invoice_analyzer import InvoiceAnalyzer
from .slack_handler import SlackHandler
from .freee_handler import FreeeHandler
from .drive_handler import DriveHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="AO Invoice Automation")

# 各ハンドラーの初期化
gmail = GmailHandler()
analyzer = InvoiceAnalyzer()
slack = SlackHandler()
freee = FreeeHandler()
drive = DriveHandler()

# 二重処理防止（in-memory: サーバー再起動でリセット）
processed_approvals: set = set()


@app.get("/health")
def health():
    return {"status": "ok", "service": "ao-invoice-automation"}


@app.post("/webhooks/gmail")
async def gmail_webhook(request: Request):
    """Gmail Pub/Sub webhook"""
    body = await request.json()
    message = body.get("message", {})
    if message.get("data"):
        try:
            data = json.loads(base64.b64decode(message["data"]).decode())
        except Exception as e:
            logger.error(f"Failed to decode Pub/Sub message: {e}")
            return JSONResponse({"status": "decode_error"})
    else:
        data = {}

    email_address = data.get("emailAddress", "")
    history_id = data.get("historyId", "")
    logger.info(f"Gmail webhook: email={email_address}, historyId={history_id}")

    if not history_id:
        return JSONResponse({"status": "no_history_id"})

    # 新着メールの取得と処理
    try:
        messages = await gmail.get_new_invoice_messages(history_id)
        logger.info(f"Found {len(messages)} invoice message(s)")

        for msg in messages:
            await process_invoice_message(msg)

        return JSONResponse({"status": "ok", "processed": len(messages)})
    except Exception as e:
        logger.error(f"Error processing gmail webhook: {e}", exc_info=True)
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=500)


@app.post("/webhooks/slack")
async def slack_webhook(request: Request):
    """Slack インタラクティブ webhook（ボタン押下）"""
    body_bytes = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    slack_signature = request.headers.get("X-Slack-Signature", "")

    # リプレイ攻撃防止
    if abs(time.time() - float(timestamp or 0)) > 300:
        raise HTTPException(status_code=401, detail="Request too old")

    signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if signing_secret:
        sig_basestring = f"v0:{timestamp}:{body_bytes.decode()}"
        computed = "v0=" + hmac.new(
            signing_secret.encode(), sig_basestring.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(computed, slack_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")

    # フォームデータのパース
    try:
        from urllib.parse import parse_qs
        form_data = parse_qs(body_bytes.decode())
        payload_str = form_data.get("payload", ["{}"])[0]
        payload = json.loads(payload_str)
    except Exception as e:
        logger.error(f"Failed to parse Slack payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")

    action_id = payload.get("actions", [{}])[0].get("action_id", "")
    action_value = payload.get("actions", [{}])[0].get("value", "{}")
    user_name = payload.get("user", {}).get("name", "unknown")

    logger.info(f"Slack action: {action_id} by {user_name}")

    try:
        invoice_data = json.loads(action_value)
    except Exception:
        invoice_data = {}

    if action_id == "approve_invoice":
        await handle_approval(invoice_data, payload, user_name)
    elif action_id == "reject_invoice":
        await handle_rejection(invoice_data, payload, user_name)
    else:
        logger.warning(f"Unknown action_id: {action_id}")

    return JSONResponse({"ok": True})


@app.post("/webhooks/slack/events")
async def slack_events_webhook(request: Request):
    """Slack Event API webhook（チャンネルへのPDFファイルアップロード検知）"""
    body_bytes = await request.body()

    try:
        body = json.loads(body_bytes.decode())
    except Exception as e:
        logger.error(f"Failed to parse Slack events webhook body: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # URL Verification（Slack App設定時の確認 - 署名なしで応答）
    if body.get("type") == "url_verification":
        logger.info("Slack URL verification challenge received")
        return JSONResponse({"challenge": body.get("challenge")})

    # 署名検証
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    slack_signature = request.headers.get("X-Slack-Signature", "")
    signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if signing_secret and timestamp and slack_signature:
        try:
            if abs(time.time() - float(timestamp)) > 300:
                raise HTTPException(status_code=401, detail="Request too old")
            sig_basestring = f"v0:{timestamp}:{body_bytes.decode()}"
            computed = "v0=" + hmac.new(
                signing_secret.encode(), sig_basestring.encode(), hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(computed, slack_signature):
                raise HTTPException(status_code=401, detail="Invalid signature")
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Slack signature verification failed: {e}")

    # Event callback 処理
    if body.get("type") == "event_callback":
        event = body.get("event", {})
        event_type = event.get("type", "")
        subtype = event.get("subtype", "")

        # ファイル共有イベント: チャンネルへのPDFアップロードを検知
        if event_type == "message" and subtype == "file_share":
            channel = event.get("channel", "")
            invoice_channel = os.environ.get("SLACK_INVOICE_CHANNEL_ID", "C0ANE67AU2X")
            if channel == invoice_channel:
                files = event.get("files", [])
                for file_info in files:
                    mimetype = file_info.get("mimetype", "")
                    filename = file_info.get("name", "")
                    if "pdf" in mimetype.lower() or filename.lower().endswith(".pdf"):
                        logger.info(f"PDF upload detected in invoice channel: {filename}")
                        # バックグラウンドで処理（Slackは3秒以内のレスポンスが必要）
                        asyncio.create_task(process_slack_file_upload(event, file_info))
                        break

    return JSONResponse({"ok": True})


async def process_invoice_message(msg: dict):
    """Gmailメッセージから請求書を処理"""
    msg_id = msg.get("id", "")
    subject = msg.get("subject", "")
    sender = msg.get("sender", "")
    pdf_data = msg.get("pdf_data")
    pdf_filename = msg.get("pdf_filename", "invoice.pdf")

    if not pdf_data:
        logger.warning(f"No PDF attachment in message {msg_id}")
        return

    # Claude AI で請求書を分析
    logger.info(f"Analyzing invoice with Claude AI...")
    analysis = await analyzer.analyze_invoice(pdf_data, sender, subject)
    logger.info(f"Analysis result: {analysis}")

    # Slack に承認依頼を投稿
    invoice_payload = {
        "msg_id": msg_id,
        "subject": subject,
        "sender": sender,
        "pdf_filename": pdf_filename,
        "pdf_data_b64": base64.b64encode(pdf_data).decode(),
        **analysis
    }

    await slack.post_invoice_approval(invoice_payload)
    logger.info(f"Posted approval request to Slack for {msg_id}")


async def process_slack_file_upload(event: dict, file_info: dict):
    """Slackにアップロードされた請求書PDFを処理（バックグラウンド）"""
    file_id = file_info.get("id", "")
    filename = file_info.get("name", "invoice.pdf")
    url_private = file_info.get("url_private_download", "") or file_info.get("url_private", "")
    user = event.get("user", "")

    logger.info(f"Processing Slack file upload: {filename} (id={file_id})")

    try:
        # PDFをSlackからダウンロード
        pdf_bytes = await slack.download_slack_file(url_private)
        if not pdf_bytes:
            logger.error(f"Failed to download PDF from Slack: {filename}")
            return

        # Claude AI で請求書を分析
        sender = f"Slack: <@{user}>"
        logger.info(f"Analyzing Slack-uploaded invoice with Claude AI: {filename}")
        analysis = await analyzer.analyze_invoice(pdf_bytes, sender, filename)
        logger.info(f"Slack file analysis result: {analysis}")

        # invoice_payloadを組み立て
        invoice_payload = {
            "msg_id": f"slack_file_{file_id}",
            "subject": filename,
            "sender": sender,
            "pdf_filename": filename,
            "pdf_data_b64": base64.b64encode(pdf_bytes).decode(),
            **analysis
        }

        await slack.post_invoice_approval(invoice_payload)
        logger.info(f"Posted approval request to Slack for uploaded file {file_id}")

    except Exception as e:
        logger.error(f"Error processing Slack file upload: {e}", exc_info=True)


async def handle_approval(invoice_data: dict, payload: dict, user_name: str):
    """請求書承認処理"""
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
    user_id = payload.get("user", {}).get("id", "")
    msg_id = invoice_data.get("msg_id", "")
    subject = invoice_data.get("subject", "")

    # ===== 二重クリック防止（in-memory）=====
    if message_ts in processed_approvals:
        logger.warning(f"Duplicate approval attempt for ts={message_ts} by {user_name}")
        try:
            slack.client.chat_postEphemeral(
                channel=channel,
                user=user_id,
                text="⚠️ この請求書はすでにfreeeに登録されています。"
            )
        except Exception as e:
            logger.error(f"Failed to post ephemeral: {e}")
        return

    # 早期にマーク（並行クリックも防ぐ）
    processed_approvals.add(message_ts)
    logger.info(f"Approving invoice: {msg_id} / {subject} by {user_name}")

    # PDFをGoogle Driveに保存
    pdf_b64 = invoice_data.get("pdf_data_b64", "")
    pdf_filename = invoice_data.get("pdf_filename", "invoice.pdf")
    invoice_date = invoice_data.get("invoice_date", datetime.now().strftime("%Y-%m-%d"))

    if pdf_b64:
        pdf_bytes = base64.b64decode(pdf_b64)
        drive_url = await drive.upload_invoice(pdf_bytes, pdf_filename, invoice_date)
        logger.info(f"Uploaded to Drive: {drive_url}")
    else:
        drive_url = None
        logger.warning("No PDF data to upload")

    # ===== freee 重複チェック =====
    existing_deal = await freee.check_duplicate(invoice_data)
    if existing_deal:
        deal_id = existing_deal.get("id", "")
        logger.warning(f"freee duplicate deal found: id={deal_id}")
        try:
            slack.client.chat_postEphemeral(
                channel=channel,
                user=user_id,
                text=f"⚠️ この請求書はすでにfreeeに登録されています。（取引ID: {deal_id}）"
            )
        except Exception as e:
            logger.error(f"Failed to post ephemeral for duplicate: {e}")
        # 既存登録でもメッセージを更新して完了状態にする
        await slack.update_invoice_message(
            channel=channel,
            ts=message_ts,
            status="approved",
            user_name=user_name,
            drive_url=drive_url,
            freee_result=existing_deal
        )
        return

    # freeeに経費登録
    freee_result = await freee.create_expense(invoice_data)
    logger.info(f"Created freee deal: {freee_result}")

    # Slackメッセージを更新
    await slack.update_invoice_message(
        channel=channel,
        ts=message_ts,
        status="approved",
        user_name=user_name,
        drive_url=drive_url,
        freee_result=freee_result
    )

    # 承認スレッドへ完了通知を返信
    await slack.post_completion_reply(
        channel=channel,
        ts=message_ts,
        vendor_name=invoice_data.get("vendor_name"),
        drive_url=drive_url,
        freee_result=freee_result
    )


async def handle_rejection(invoice_data: dict, payload: dict, user_name: str):
    """請求書却下処理"""
    msg_id = invoice_data.get("msg_id", "")
    logger.info(f"Rejecting invoice: {msg_id} by {user_name}")

    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
    await slack.update_invoice_message(
        channel=channel,
        ts=message_ts,
        status="rejected",
        user_name=user_name
    )
