"""
ao-invoice-automation
受信メールの請求書PDFを自動処理するシステム
Gmail Pub/Sub → Claude AI 分析 → Slack承認 → freee登録 + Google Drive保存
"""

import os
import json
import base64
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


@app.get("/health")
def health():
    return {"status": "ok", "service": "ao-invoice-automation"}


@app.post("/webhooks/gmail")
async def gmail_webhook(request: Request):
    """Gmail Pub/Sub からの通知を受け取る"""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Pub/Sub メッシージのデコード
    message = body.get("message", {})
    if not message:
        return JSONResponse({"status": "no_message"})

    data_b64 = message.get("data", "")
    if data_b64:
        try:
            data = json.loads(base64.b64decode(data_b64).decode("utf-8"))
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
    """Slack インタラクティブメッセージ（承認ボタン）の受け取り"""
    # Slack署名の検証
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    slack_signature = request.headers.get("X-Slack-Signature", "")
    body_bytes = await request.body()

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

    return JSONResponse({"status": "ok"})


async def process_invoice_message(msg: dict):
    """請求書メッセージを処理する"""
    msg_id = msg["id"]
    subject = msg.get("subject", "（件名なし）")
    sender = msg.get("sender", "unknown")
    logger.info(f"Processing invoice: id={msg_id}, subject={subject}, from={sender}")

    # PDFの取得
    pdf_data = msg.get("pdf_data")  # bytes
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


async def handle_approval(invoice_data: dict, payload: dict, user_name: str):
    """請求書承認処理"""
    msg_id = invoice_data.get("msg_id", "")
    subject = invoice_data.get("subject", "")
    logger.info(f"Approving invoice: {msg_id} by {user_name}")

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

    # freeeに経費登録
    freee_result = await freee.create_expense(invoice_data)
    logger.info(f"Created freee deal: {freee_result}")

    # Slackメッセージを更新
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
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
