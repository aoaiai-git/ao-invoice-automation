"""初期データ投入 — freee取引先からname_mappingを自動生成

実行方法:
  python -m app.reconciliation.seed_data
"""

import os
import logging
from .sheets_client import ReconciliationSheetsClient
from .freee_client import FreeeReconcileClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def seed_name_mapping():
    """freeeの取引先一覧を取得して、消込マスタに初期データを投入する

    既存エントリは上書きしない（スキップ）。
    keywords フィールドは空欄で登録し、後から手動で編集する。
    """
    logger.info("消込マスタの初期データ投入を開始します")

    sheets = ReconciliationSheetsClient()
    freee = FreeeReconcileClient()

    # シード儝期化
    sheets.ensure_sheets_exist()

    # 既存マッピングを取得（重複防止）
    existing_mapping = sheets.get_name_mapping()
    existing_names = {r.get("freee_partner_name", "") for r in existing_mapping}
    logger.info(f"既存エントリ数: {len(existing_names)}")

    # freeeから取引先一覧を取得
    partners = freee.get_partners()
    logger.info(f"freee取引先数: {len(partners)}")

    added = 0
    skipped = 0
    for partner in partners:
        name = partner.get("name", "").strip()
        if not name:
            continue
        if name in existing_names:
            skipped += 1
            continue

        # 会社名から簡単なキーワード候補を生成（株式会社・有限会社などを除去）
        import re
        short_name = re.sub(r"(株式会社|有限会社|合同会社|一般社団法人|NPO法人|公益財団法人)", "", name).strip()
        keywords = short_name if short_name and short_name != name else ""

        sheets.upsert_name_mapping(
            partner_name=name,
            keywords=keywords,
            notes=f"freee partner_id={partner.get('id', '')}",
        )
        existing_names.add(name)
        added += 1

    logger.info(f"投入完了: 追加={added}, スキップ（既存）={skipped}")
    return {"added": added, "skipped": skipped}


if __name__ == "__main__":
    result = seed_name_mapping()
    print(f"完了: {result}")
