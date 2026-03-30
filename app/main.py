"""
ao-invoice-automation
åä¿¡ã¡ã¼ã«ã®è«æ±æ¸PDFãèªåå¦çããã·ã¹ãã 
Gmail Pub/Sub â Claude AI åæ â Slacfæ¿èª â freeeç»é² + Google Driveä¿å­
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
import re
from .gmail_handler import GmailHandler
from .invoice_analyzer import InvoiceAnalyzer
from .slack_handler import SlackHandler
from .freee_handler import FreeeHandler
from .drive_handler import DriveHandler
from .reconciliation.runner import run_reconciliation
from .reconciliation.seed_data import seed_name_mapping
from .reconciliation.slack_handler import handle_reconciliation_action as handle_recon_action
from . import idiott_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="AO Invoice Automation")

# åãã³ãã©ã¼ã®åæå
gmail = GmailHandler()
analyzer = InvoiceAnalyzer()
slack = SlackHandler()
freee = FreeeHandler()
drive = DriveHandler()

# äºéå¦çé²æ­¢ï¼in-memory: ãµã¼ãã¼åèµ·åã§ãªã»ããï¼
processed_approvals: set = set()
processed_rejections: set = set()


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
    """Slack ã¤ã³ã¿ã©ã¯ãã£ã webhookï¼ãã¿ã³æ¼ä¸ï¼"""
    body_bytes = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    slack_signature = request.headers.get("X-Slack-Signature", "")

    # ãªãã¬ã¤æ»æé²æ­¢
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

    # ãã©ã¼ã ãã¼ã¿ã®ãã¼ã¹
    try:
        from urllib.parse import parse_qs
        form_data = parse_qs(body_bytes.decode())
        payload_str = form_data.get("payload", ["{}"])[0]
        payload = json.loads(payload_str)
    except Exception as e:
        logger.error(f"Failed to parse Slack payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")

    action_id = payload.get("actions", [{}])[0].get("action_id", "")
    value = payload.get("actions", [{}])[0].get("value", "")
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
    user_id = payload.get("user", {}).get("id", "")
    user_name = payload.get("user", {}).get("name", "unknown")

    logger.info(f"Slack action: {action_id} by {user_name}")

    try:
        invoice_data = json.loads(value)
    except Exception:
        invoice_data = {}

    # ===== ãã¿ã³äºéæ¼ãé²æ­¢ï¼ã¡ãã»ã¼ã¸ãæ¢ã«å¦çæ¸ããã§ãã¯ =====
    if action_id in ("approve_invoice", "reject_invoice"):
        _message_blocks = payload.get("message", {}).get("blocks", [])
        _has_action_block = any(b.get("type") == "actions" for b in _message_blocks)
        if not _has_action_block:
            _msg_text = ""
            for blk in _message_blocks:
                if blk.get("type") == "section":
                    _msg_text = blk.get("text", {}).get("text", "")
                    break
            if "æ¿èªæ¸ã¿" in _msg_text or "freeeç»é²å®äº" in _msg_text:
                _ephemeral_text = "â ï¸ ãã®è«æ±æ¸ã¯ãã§ã«æ¿èªæ¸ã¿ã»freeeç»é²æ¸ã¿ã§ãã"
            elif "å´ä¸æ¸ã¿" in _msg_text:
                _ephemeral_text = "â ï¸ ãã®è«æ±æ¸ã¯ãã§ã«å´ä¸æ¸ã¿ã§ãã"
            else:
                _ephemeral_text = "â ï¸ ãã®è«æ±æ¸ã¯æ¢ã«å¦çæ¸ã¿ã§ãã"
            try:
                slack.client.chat_postEphemeral(
                    channel=channel,
                    user=user_id,
                    text=_ephemeral_text,
                )
            except Exception as _e:
                logger.error(f"Failed to post ephemeral (already processed): {_e}")
            return JSONResponse({"ok": True})

    if action_id == "approve_invoice":
        await handle_approval(invoice_data, payload, user_name)
    elif action_id == "reject_invoice":
        await handle_rejection(invoice_data, payload, user_name)
    elif action_id.startswith("recon_"):
        await handle_recon_action(action_id, invoice_data, payload, user_name)
    elif action_id in ("idiott_create_invoice", "idiott_create_invoice_bulk"):
        billing_month = value or idiott_handler.get_billing_month()
        await idiott_handler.handle_create_invoice(
            slack_client=slack.client,
            channel=channel,
            message_ts=message_ts,
            billing_month=billing_month,
            user_id=user_id,
        )
    elif action_id == "idiott_freee_register":
        billing_month = value or idiott_handler.get_billing_month()
        await idiott_handler.handle_freee_register(
            slack_client=slack.client,
            freee_handler=freee,
            channel=channel,
            message_ts=message_ts,
            billing_month=billing_month,
            user_id=user_id,
        )
    else:
        logger.warning(f"Unknown action_id: {action_id}")

    return JSONResponse({"ok": True})


@app.post("/webhooks/slack/events")
async def slack_events_webhook(request: Request):
    """Slack Event API webhookï¼ãã£ã³ãã«ã¸ã®PDFãã¡ã¤ã«ã¢ããã­ã¼ãæ¤ç¥ï¼"""
    body_bytes = await request.body()
    try:
        body = json.loads(body_bytes.decode())
    except Exception as e:
        logger.error(f"Failed to parse Slack events webhook body: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # URL Verificationï¼Slack Appè¨­å®æã®ç¢ºèª - ç½²åãªãã§å¿ç­ï¼
    if body.get("type") == "url_verification":
        logger.info("Slack URL verification challenge received")
        return JSONResponse({"challenge": body.get("challenge")})

    # ç½²åæ¤è¨¼
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

    # Event callback å¦ç
    if body.get("type") == "event_callback":
        event = body.get("event", {})
        event_type = event.get("type", "")
        subtype = event.get("subtype", "")

        if event_type == "message" and subtype == "file_share":
            ch = event.get("channel", "")
            invoice_channel = os.environ.get("SLACK_INVOICE_CHANNEL_ID", "C0ANE67AU2X")
            if ch == invoice_channel:
                files = event.get("files", [])
                for file_info in files:
                    mimetype = file_info.get("mimetype", "")
                    filename = file_info.get("name", "")
                    if "pdf" in mimetype.lower() or filename.lower().endswith(".pdf"):
                        logger.info(f"PDF upload detected in invoice channel: {filename}")
                        asyncio.create_task(process_slack_file_upload(event, file_info))
                        break

    return JSONResponse({"ok": True})


async def process_invoice_message(msg: dict):
    """Gmailã¡ãã»ã¼ã¸ããè«æ±æ¸ãå¦ç"""
    msg_id = msg.get("id", "")
    subject = msg.get("subject", "")
    sender = msg.get("sender", "")
    pdf_data = msg.get("pdf_data")
    pdf_filename = msg.get("pdf_filename", "invoice.pdf")

    if not pdf_data:
        logger.warning(f"No PDF attachment in message {msg_id}")
        return

    logger.info(f"Analyzing invoice with Claude AI...")
    analysis = await analyzer.analyze_invoice(pdf_data, sender, subject)
    logger.info(f"Analysis result: {analysis}")

    # 1. ç¿ç°ããï¼è¨ç»å¤ï¼ãã§ãã¯ - idiott_contacts ããåã«ç¢ºèª
    if idiott_handler.SARUTA_EMAIL.lower() in sender.lower():
        logger.info(f"Saruta reference invoice received from {sender}")
        await idiott_handler.process_saruta_invoice(
            slack_client=slack.client,
            analysis=analysis,
            pdf_data=pdf_data,
            drive_handler=drive,
        )
        return

    # 2. ã¢ã¤ãã£ãªããæ¥­åå§è¨èãã§ãã¯
    if await idiott_handler.is_idiott_contact(sender):
        contractor_name = re.sub(r'<[^>]+>', '', sender).strip()
        await idiott_handler.process_contractor_invoice(
            slack_client=slack.client,
            sender_email=sender,
            contractor_name=contractor_name,
            analysis=analysis,
            pdf_data=pdf_data,
            drive_handler=drive,
        )
        return

    # 3. éå¸¸ã®è«æ±æ¸æ¿èªãã­ã¼
    invoice_payload = {
        "msg_id": msg_id,
        "subject": subject,
        "sender": sender,
        "pdf_filename": pdf_filename,
        "pdf_data_b64": base64.b64encode(pdf_data).decode(),
        **analysis,
    }
    await slack.post_invoice_approval(invoice_payload)
    logger.info(f"Posted approval request to Slack for {msg_id}")


async def process_slack_file_upload(event: dict, file_info: dict):
    """Slackã«ã¢ããã­ã¼ããããè«æ±æ¸PDFãå¦çï¼ããã¯ã°ã©ã¦ã³ãï¼"""
    file_id = file_info.get("id", "")
    filename = file_info.get("name", "invoice.pdf")
    url_private = file_info.get("url_private_download", "") or file_info.get("url_private", "")
    user = event.get("user", "")
    logger.info(f"Processing Slack file upload: {filename} (id={file_id})")

    try:
        pdf_bytes = await slack.download_slack_file(url_private)
        if not pdf_bytes:
            logger.error(f"Failed to download PDF from Slack: {filename}")
            return

        sender = f"Slack: <@{user}>"
        logger.info(f"Analyzing Slack-uploaded invoice with Claude AI: {filename}")
        analysis = await analyzer.analyze_invoice(pdf_bytes, sender, filename)
        logger.info(f"Slack file analysis result: {analysis}")

        invoice_payload = {
            "msg_id": f"slack_file_{file_id}",
            "subject": filename,
            "sender": sender,
            "pdf_filename": filename,
            "pdf_data_b64": base64.b64encode(pdf_bytes).decode(),
            **analysis,
        }
        await slack.post_invoice_approval(invoice_payload)
        logger.info(f"Posted approval request to Slack for uploaded file {file_id}")
    except Exception as e:
        logger.error(f"Error processing Slack file upload: {e}", exc_info=True)


async def handle_approval(invoice_data: dict, payload: dict, user_name: str):
    """è«æ±æ¸æ¿èªå¦ç"""
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
    user_id = payload.get("user", {}).get("id", "")
    msg_id = invoice_data.get("msg_id", "")
    subject = invoice_data.get("subject", "")
    vendor_name = invoice_data.get("vendor_name", "")

    if message_ts in processed_approvals:
        logger.warning(f"Duplicate approval attempt for ts={message_ts} by {user_name}")
        try:
            slack.client.chat_postEphemeral(
                channel=channel,
                user=user_id,
                text="â ï¸ ãã®è«æ±æ¸ã¯ãã§ã«æ¿èªæ¸ã¿ã»freeeç»é²æ¸ã¿ã§ãã",
            )
        except Exception as e:
            logger.error(f"Failed to post ephemeral: {e}")
        return

    processed_approvals.add(message_ts)
    logger.info(f"Approving invoice: {msg_id} / {subject} by {user_name}")

    pdf_b64 = invoice_data.get("pdf_data_b64", "")
    pdf_filename = invoice_data.get("pdf_filename", "invoice.pdf")
    invoice_date = invoice_data.get("invoice_date", datetime.now().strftime("%Y-%m-%d"))
    if pdf_b64:
        pdf_bytes = base64.b64decode(pdf_b64)
        drive_url = await drive.upload_invoice(
            pdf_bytes,
            pdf_filename,
            invoice_date,
            vendor_name=vendor_name,
        )
        logger.info(f"Uploaded to Drive: {drive_url}")
    else:
        drive_url = None
        logger.warning("No PDF data to upload")

    existing_deal = await freee.check_duplicate(invoice_data)
    if existing_deal:
        deal_id = existing_deal.get("id", "")
        logger.warning(f"freee duplicate deal found: id={deal_id}")
        try:
            slack.client.chat_postEphemeral(
                channel=channel,
                user=user_id,
                text=f"â ï¸ ãã®è«æ±æ¸ã¯ãã§ã«freeeã«ç»é²ããã¦ãã¾ãï¼åå¼ID: {deal_id}ï¼ã",
            )
        except Exception as e:
            logger.error(f"Failed to post ephemeral for duplicate: {e}")
        await slack.update_invoice_message(
            channel=channel,
            ts=message_ts,
            status="approved",
            user_name=user_name,
            drive_url=drive_url,
            freee_result=existing_deal,
        )
        return

    freee_result = await freee.create_expense(invoice_data)
    logger.info(f"Created freee deal: {freee_result}")

    await slack.update_invoice_message(
        channel=channel,
        ts=message_ts,
        status="approved",
        user_name=user_name,
        drive_url=drive_url,
        freee_result=freee_result,
    )

    await slack.post_completion_reply(
        channel=channel,
        ts=message_ts,
        vendor_name=vendor_name,
        drive_url=drive_url,
        freee_result=freee_result,
    )


async def handle_rejection(invoice_data: dict, payload: dict, user_name: str):
    "" è«æ±æ¸å´ä¸å¦ç"""
    msg_id = invoice_data.get("msg_id", "")
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")
    user_id = payload.get("user", {}).get("id", "")

    if message_ts in processed_rejections:
        logger.warning(f"Duplicate rejection attempt for ts={message_ts} by {user_name}")
        try:
            slack.client.chat_postEphemeral(
                channel=channel,
                user=user_id,
                text="â ï¸ ãã®è«æ±æ¸ã¯ãã§ã«å´ä¸æ¸ã¿ã§ãã",
            )
        except Exception as e:
            logger.error(f"Failed to post ephemeral: {e}")
        return

    processed_rejections.add(message_ts)
    logger.info(f"Rejecting invoice: {msg_id} by {user_name}")

    await slack.update_invoice_message(
        channel=channel,
        ts=message_ts,
        status="rejected",
        user_name=user_name,
    )


@app.post("/reconciliation/run")
async def reconciliation_run(request: Request):
    """Run reconciliation batch manually"""
    secret = os.environ.get("RECONCILIATION_RUN_SECRET", "")
    if secret:
        auth = request.headers.get("X-Run-Secret", "")
        if auth != secret:
            raise HTTPException(status_code=401, detail="Unauthorized")
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    dry_run = body.get("dry_run", False)
    stats = await run_reconciliation(dry_run=dry_run)
    return JSONResponse({"ok": True, "stats": stats})


@app.post("/reconciliation/seed")
async def reconciliation_seed_endpoint(request: Request):
    """Seed reconciliation master from freee partners"""
    secret = os.environ.get("RECONCILIATION_RUN_SECRET", "")
    if secret:
        auth = request.headers.get("X-Run-Secret", "")
        if auth != secret:
            raise HTTPException(status_code=401, detail="Unauthorized")
    result = await asyncio.get_event_loop().run_in_executor(None, seed_name_mapping)
    return JSONResponse({"ok": True, "seeded": result})
