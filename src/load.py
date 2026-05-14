from datetime import datetime
import json
import logging


def save_raw_data(data, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"users_raw_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    logging.info(f"Arquivo bruto salvo em: {file_path}")

    return file_path


def save_processed_data(df, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"users_processed_{timestamp}.csv"

    df.to_csv(file_path, index=False, encoding="utf-8-sig")

    logging.info(f"Arquivo processado salvo em: {file_path}")

    return file_path