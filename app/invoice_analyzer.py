"""Claude AI を使った請求書分析"""
import os
import base64
import logging
import anthropic

logger = logging.getLogger(__name__)

# freee勘定科目マッピング（AI分類用のヒント）
ACCOUNT_ITEMS = {
    "外注費": 675785125,
    "外注費（仕入れ外）": 818154170,
    "支払手数料": 675785145,
    "支払報酵料": 675785153,
    "通信費": 675785135,
    "賃借料": 675785148,
    "リース料": 675785150,
    "消耗品費": 675785138,
    "広告宣伝費": 675785128,
    "採用教育費": 675785124,
    "研修費": 675785203,
    "地代家賃": 675785147,
    "会議費": 675785131,
    "旅費交通費": 675785132,
    "雑費": 675785162,
    "事務用品費": 675785139,
    "新聞図書費": 675785143,
    "水道光熱費": 675785142,
    "保険料": 675785151,
    "租税公課": 675785152,
}

SYSTEM_PROMPT = """あなたは日本の会計・経費処理の専門家です。
添付された請求書PDFを分析し、以下の情報をJSON形式で抜出してください。

必ず以下のJSONキーを含めてください：
- vendor_name: 請求元会社名（文字列）
- invoice_number: 請求書番号（文字列、不明なら空文字）
- invoice_date: 請求日または発行日（YYYY-MM-DD形式、不明なら今日の日付）
- due_date: 支払期限（YYYY-MM-DD形式、不明なら空文字）
- amount_excl_tax: 税抜金額（整数）
- tax_amount: 消費税額（整数）
- amount_incl_tax: 税込金額（整数）
- actual_hours: 実稼働時間（数値、記載がない場合はnull）
- currency: 通貨コード（JPY, USD等）
- description: 内容・摘要（最大90文字）
- suggested_account: freeeの務定科目名（下記リストから最適なものを選択）
- suggested_account_id: 上記務定科目のfreee ID（整数）
- confidence: 分析の確信度（high/medium/low）
- notes: 特記事項（税率、分割払い、前払いなど）

actual_hours について：
- 請求書に「稼働時間」「実稼働時間」「作業時間」「工数」等の記載がある場合、その数値を抜出してください
- 単位は「時間」として数値のみ返してください（例: 40.5）
- 記載がない場合は null を返してください

務定科目の選択基準：
- AI・クラウドサービス、API利用料 → 外注費または支払手数料
- ソフトウェア・SaaS利用料 → 賃借料または支払手数料
- オフィス用品・消耗品 → 消耗品費または事務用品費
- 通信・インターネット → 通信費
- 広告・マーケティング → 広告宣会費
- 外注・フリーランス → 外注費
- その他・分類困難 → 雑費

利用可能な務定科目：""" + "\n".join([f"- {k}\uff08ID: {v}\uff09" for k, v in {
    "外注費": 675785125, "外注費（仕入れ外）": 818154170, "支払手数料": 675785145,
    "支払報酵料": 675785153, "通信費": 675785135, "賃借料": 675785148,
    "リース料": 675785150, "消耗品費": 675785138, "広告宣伝費": 675785128,
    "採用教育費": 675785124, "研修費": 675785203, "地代家賃": 675785147,
    "会議費": 675785131, "旅費交通費": 675785132, "雑費": 675785162,
    "事務用品費": 675785139, "新聞図書費": 675785143, "水道光熱費": 675785142,
    "保険料": 675785151, "租税公課": 675785152,
}.items()]) + """

JSONのみを返してください。説明文は不要です。"""


class InvoiceAnalyzer:
    def __init__(self):
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.client = anthropic.Anthropic(api_key=api_key)

    async def analyze_invoice(self, pdf_data: bytes, sender: str, subject: str) -> dict:
        pdf_b64 = base64.standard_b64encode(pdf_data).decode("utf-8")
        user_message = (
            f"以下の請求書PDFを分析してください。\n"
            f"送信者: {sender}\n"
            f"件名: {subject}\n"
            f"PDFの内容を解析し、指定されたJSON形式で情報を抜出してください"
        )
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": "application/pdf",
                                    "data": pdf_b64,
                                },
                            },
                            {"type": "text", "text": user_message},
                        ],
                    }
                ],
            )
            content = response.content[0].text.strip()
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            import json
            result = json.loads(content)
            logger.info(
                f"Invoice analysis: vendor={result.get('vendor_name')}, "
                f"amount={result.get('amount_incl_tax')}, "
                f"actual_hours={result.get('actual_hours')}, "
                f"account={result.get('suggested_account')}"
            )
            return result
        except Exception as e:
            logger.error(f"Invoice analysis failed: {e}", exc_info=True)
            return {
                "vendor_name": sender.split("<")[0].strip() if "<" in sender else sender,
                "invoice_number": "",
                "invoice_date": "",
                "due_date": "",
                "amount_excl_tax": 0,
                "tax_amount": 0,
                "amount_incl_tax": 0,
                "actual_hours": None,
                "currency": "JPY",
                "description": subject,
                "suggested_account": "雑費",
                "suggested_account_id": 675785162,
                "confidence": "low",
                "notes": f"自動分析エラー: {str(e)}",
            }
