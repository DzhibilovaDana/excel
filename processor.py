# processor.py
import argparse
import json
import logging
import os
import shutil
import time
import base64
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Tuple
import math
import pandas as pd
from google import genai
from google.genai import types
from openpyxl import load_workbook
from openpyxl import Workbook

# Константы выходных колонок артефакта 1
RESULT_COLUMNS = {
    "trigger": "Инициирующее событие",
    "executor": "Исполнитель",
    "step_description": "Описание шага",
    "mirapolis_action": "Действие в системе",
}

ARTIFACT2_SHEET = "ФТ Mirapolis"


@dataclass
class PipelineConfig:
    input_xlsx: str
    output_xlsx: str
    prompt_file: str
    batch_size: int = 10
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.0-flash"
    log_dir: str = "logs"
    state_dir: str = "snapshots"
    overwrite_input: bool = False

    @classmethod
    def from_env_and_args(cls) -> "PipelineConfig":
        parser = argparse.ArgumentParser(description="Process Excel through Gemini in batches.")
        parser.add_argument("--input", dest="input_xlsx", required=True, help="Входной XLSX файл")
        parser.add_argument("--output", dest="output_xlsx", required=True, help="Итоговый XLSX файл")
        parser.add_argument("--prompt", dest="prompt_file", required=True, help="Путь к файлу промта")
        parser.add_argument("--batch-size", dest="batch_size", type=int, default=int(os.getenv("BATCH_SIZE", 10)))
        parser.add_argument("--model", dest="gemini_model", default=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
        parser.add_argument("--log-dir", dest="log_dir", default=os.getenv("LOG_DIR", "logs"))
        parser.add_argument("--state-dir", dest="state_dir", default=os.getenv("STATE_DIR", "snapshots"))
        parser.add_argument("--api-key", dest="gemini_api_key", default=os.getenv("GEMINI_API_KEY", ""))
        parser.add_argument(
            "--overwrite-input",
            dest="overwrite_input",
            action="store_true",
            default=os.getenv("OVERWRITE_INPUT", "false").lower() in ("1", "true", "yes"),
            help="Если задан, после каждого батча перезаписывать input.xlsx текущим output.xlsx",
        )

        args = parser.parse_args()
        api_key = args.gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        if not api_key:
            raise ValueError("GEMINI_API_KEY обязателен (env или --api-key).")

        batch_size = args.batch_size or int(os.getenv("BATCH_SIZE", 10))
        if batch_size <= 0:
            raise ValueError("BATCH_SIZE должен быть > 0")

        return cls(
            input_xlsx=args.input_xlsx,
            output_xlsx=args.output_xlsx,
            prompt_file=args.prompt_file,
            batch_size=batch_size,
            gemini_api_key=api_key,
            gemini_model=args.gemini_model,
            log_dir=args.log_dir,
            state_dir=args.state_dir,
            overwrite_input=args.overwrite_input,
        )


def ensure_output_workbook(input_path: str, output_path: str) -> None:
    if not os.path.exists(output_path):
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        shutil.copyfile(input_path, output_path)


def get_process_sheet_name(workbook) -> str:
    return "Process" if "Process" in workbook.sheetnames else workbook.sheetnames[0]


def load_dataframe(output_path: str, process_sheet: str) -> pd.DataFrame:
    df = pd.read_excel(output_path, sheet_name=process_sheet)
    df = df.reset_index(drop=True)
    for col in RESULT_COLUMNS.values():
        if col not in df.columns:
            df[col] = ""
    df["status"] = df.get("status", "")
    return df


def _is_empty_cell(value: Any) -> bool:
    """
    Возвращает True, если значение считается пустым:
    - None
    - numpy.nan / float('nan')
    - пустая строка (после strip)
    """
    if value is None:
        return True
    # pandas / numpy NaN проверяем через math.isnan для чисел, или через pandas.isna
    try:
        # Это сработает для np.nan и для float('nan')
        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass
    # pandas uses pd.isna for complex types
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    # Finally, empty string after strip
    try:
        if isinstance(value, str) and value.strip() == "":
            return True
    except Exception:
        pass
    return False


def is_row_processed(row: pd.Series) -> bool:
    """
    Строка считается обработанной если:
    - статус == 'done' (регистронезависимо)
    ИЛИ
    - все целевые колонки RESULT_COLUMNS заполнены (т.е. НЕ пусты по _is_empty_cell)
    """
    status = row.get("status", "")
    if isinstance(status, str) and status.strip().lower() == "done":
        return True

    for col in RESULT_COLUMNS.values():
        val = row.get(col, "")
        if _is_empty_cell(val):
            # как только находим пустую целевую колонку — строка НЕ обработана
            return False
    # все целевые колонки непустые — строка обработана
    return True


def select_batch(df: pd.DataFrame, batch_size: int) -> pd.DataFrame:
    unprocessed = df[~df.apply(is_row_processed, axis=1)]
    return unprocessed.head(batch_size)


def _normalize(name: str) -> str:
    """
    Нормализует имя колонки: убирает пробелы, переносы строк, кавычки,
    скобки, приводит к нижнему регистру — для нечувствительного поиска.
    """
    if not isinstance(name, str):
        return ""
    # Убираем пробелы, переносы строк, кавычки и круглые скобки, слэши и прочие
    normalized = "".join(ch for ch in name.lower() if ch.isalnum())
    return normalized


def _ensure_columns(batch_df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    """
    Для каждого required_cols пытаемся найти в batch_df похожую колонку (по нормализованному имени).
    Если найдено — копируем её под стандартным именем required_cols[i].
    Если не найдено — создаём пустую колонку с этим именем.
    Возвращаем модифицированный batch_df.
    """
    # Создаём индекс нормализованных существующих колонок -> оригинальные имена
    norm_map = {}
    for col in batch_df.columns:
        norm = _normalize(col)
        if norm:
            norm_map[norm] = col

    for required in required_cols:
        req_norm = _normalize(required)
        if req_norm in norm_map:
            orig = norm_map[req_norm]
            if orig != required:
                # Копируем/переименовываем в новую колонку с каноническим именем
                batch_df[required] = batch_df[orig]
                logging.info("Сопоставлена колонка '%s' -> '%s'.", orig, required)
        else:
            # Попробуем более либеральный поиск: смотрим, есть ли существующая колонка,
            # содержащая ключевые слова из required (например 'mirapolis' и 'l4')
            words = [w for w in "".join(ch if ch.isalnum() else " " for ch in required.lower()).split() if len(w) > 2]
            found = None
            for col in batch_df.columns:
                col_low = col.lower()
                if all(word in col_low for word in words):
                    found = col
                    break
            if found:
                batch_df[required] = batch_df[found]
                logging.info("Либерально сопоставлена колонка '%s' -> '%s'.", found, required)
            else:
                # Создаём пустую колонку
                batch_df[required] = ""
                logging.warning("Колонка '%s' не найдена — создана пустая колонка.", required)

    return batch_df


def build_batch_payload(batch_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Построение JSON-пейлоада для батча. Функция теперь толерантна к названиям колонок:
    она попытается сопоставить нужные колонки по нормализованным именам и создать недостающие.
    """
    required_cols = [
        "L1",
        "L2",
        "L3",
        "L4",
        "L5",
        "Mirapolis L4 (Да/Нет)",
        "Ответ Клиента",
        "Комментарии/уточнения",
        "Комментарии со встреч",
        "Проблема текущего состояния",
        "Предложение по решению /улучшению HCM",
        "Функциональные требования",
    ]

    # Приводим batch_df к версии с гарантированными колонками
    batch_df = _ensure_columns(batch_df, required_cols)

    # Теперь строим payload (как раньше), опираясь на стандартные имена
    payload = []
    for idx, row in batch_df.iterrows():
        excel_row_index = int(idx) + 2  # +2: смещение на заголовок
        payload.append(
            {
                "row_index": excel_row_index,
                "l2_name": row.get("L2", ""),
                "L1": row.get("L1", ""),
                "L2": row.get("L2", ""),
                "L3": row.get("L3", ""),
                "L4": row.get("L4", ""),
                "L5": row.get("L5", ""),
                "Mirapolis_L4": row.get("Mirapolis L4 (Да/Нет)", ""),
                "Ответ_Клиента": row.get("Ответ Клиента", ""),
                "Комментарии_уточнения": row.get("Комментарии/уточнения", ""),
                "Комментарии_со_встреч": row.get("Комментарии со встреч", ""),
                "Проблема_текущего_состояния": row.get("Проблема текущего состояния", ""),
                "Предложение_по_решению": row.get("Предложение по решению /улучшению HCM", ""),
                "Функциональные_требования": row.get("Функциональные требования", ""),
            }
        )
    return payload


class GeminiClient:
    def __init__(self, api_key: str, model_name: str, log_dir: str):
        self.model_name = f"models/{model_name}"
        self.client = genai.Client(api_key=api_key)
        safety_categories = [
            "HARM_CATEGORY_HARASSMENT",
            "HARM_CATEGORY_HATE_SPEECH",
            "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "HARM_CATEGORY_DANGEROUS_CONTENT",
        ]
        self.config = types.GenerateContentConfig(
            temperature=0.1,
            safety_settings=[types.SafetySetting(category=c, threshold="BLOCK_NONE") for c in safety_categories],
        )
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    def call(self, prompt_text: str, batch_json: List[Dict[str, Any]], batch_idx: int) -> Dict[str, Any]:
        system_instruction = "Верни только валидный JSON, без markdown, без пояснений, без текста вокруг. Язык значений — русский."
        contract = {
            "artifact1_rows": [
                {
                    "row_index": 123,
                    "process_l4_name": "...",
                    "trigger": "...",
                    "executor": "...",
                    "step_description": "...",
                    "mirapolis_action": "...",
                }
            ],
            "artifact2_by_l2": [
                {
                    "l2_name": "...",
                    "requirements": {
                        "data_master": ["..."],
                        "ui_forms": ["..."],
                        "business_logic": ["..."],
                        "approval_routing": ["..."],
                        "integrations": ["..."],
                        "reporting_analytics": ["..."],
                        "security_access": ["..."],
                    },
                }
            ],
        }

        full_prompt = "\n\n".join(
            [
                system_instruction,
                prompt_text,
                "Ожидаемый JSON:\n" + json.dumps(contract, ensure_ascii=False, indent=2),
                "Вот данные (первичный источник, массив объектов):",
                json.dumps(batch_json, ensure_ascii=False, indent=2),
            ]
        )

        input_log = os.path.join(self.log_dir, f"batch_{batch_idx}_input.json")
        raw_log = os.path.join(self.log_dir, f"batch_{batch_idx}_raw.txt")
        parsed_log = os.path.join(self.log_dir, f"batch_{batch_idx}_parsed.json")

        with open(input_log, "w", encoding="utf-8") as f:
            json.dump(batch_json, f, ensure_ascii=False, indent=2)

        for attempt in range(3):
            try:
                response = self.client.models.generate_content(model=self.model_name, contents=[full_prompt], config=self.config)
                response_text = (response.text or "").strip()
                with open(raw_log, "w", encoding="utf-8") as f:
                    f.write(response_text)

                cleaned = response_text
                if cleaned.startswith("```"):
                    cleaned = cleaned.strip("`").strip()
                    if cleaned.lower().startswith("json"):
                        cleaned = cleaned[4:].strip()

                # First try to parse JSON
                try:
                    parsed = json.loads(cleaned)
                except Exception:
                    # If parsing fails, maybe it's raw base64 xlsx
                    text = cleaned.strip()
                    if len(text) > 300 and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r" for c in text[:200]):
                        try:
                            xbytes = base64.b64decode(text)
                            filled_path = os.path.join(self.log_dir, f"batch_{batch_idx}_filled_from_gemini.xlsx")
                            wb = load_workbook(filename=BytesIO(xbytes))
                            wb.save(filled_path)
                            # Write a minimal parsed log
                            with open(parsed_log, "w", encoding="utf-8") as f:
                                json.dump({"filled_xlsx_path": filled_path}, f, ensure_ascii=False, indent=2)
                            return {"filled_xlsx_path": filled_path}
                        except Exception:
                            raise

                    raise

                # If parsed is dict and contains filled_xlsx_base64
                if isinstance(parsed, dict) and "filled_xlsx_base64" in parsed:
                    try:
                        b64 = parsed["filled_xlsx_base64"]
                        xbytes = base64.b64decode(b64)
                        filled_path = os.path.join(self.log_dir, f"batch_{batch_idx}_filled_from_gemini.xlsx")
                        wb = load_workbook(filename=BytesIO(xbytes))
                        wb.save(filled_path)
                        parsed_no_b64 = dict(parsed)
                        parsed_no_b64.pop("filled_xlsx_base64", None)
                        parsed_no_b64["_filled_xlsx_path"] = filled_path
                        with open(parsed_log, "w", encoding="utf-8") as f:
                            json.dump(parsed_no_b64, f, ensure_ascii=False, indent=2)
                        return {"filled_xlsx_path": filled_path, **parsed_no_b64}
                    except Exception as exc:
                        logging.warning("Ошибка декодирования base64 xlsx: %s", exc)
                        raise

                # Normal JSON case
                with open(parsed_log, "w", encoding="utf-8") as f:
                    json.dump(parsed, f, ensure_ascii=False, indent=2)
                return parsed

            except Exception as exc:
                logging.warning("Gemini ошибка, попытка %s: %s", attempt + 1, exc)
                time.sleep(15)

        raise RuntimeError("Не удалось получить валидный JSON/файл от Gemini после 3 попыток.")


def ensure_result_columns(wb, process_sheet: str) -> None:
    ws = wb[process_sheet]
    headers = [cell.value for cell in ws[1]]
    for col_name in RESULT_COLUMNS.values():
        if col_name not in headers:
            ws.cell(row=1, column=ws.max_column + 1).value = col_name


def apply_artifact1_rows(wb, process_sheet: str, artifact_rows: List[Dict[str, Any]]) -> None:
    ws = wb[process_sheet]
    header_to_col = {cell.value: cell.column for cell in ws[1]}
    for item in artifact_rows:
        row_idx = int(item["row_index"])
        ws.cell(row=row_idx, column=header_to_col[RESULT_COLUMNS["trigger"]]).value = item.get("trigger", "")
        ws.cell(row=row_idx, column=header_to_col[RESULT_COLUMNS["executor"]]).value = item.get("executor", "")
        ws.cell(row=row_idx, column=header_to_col[RESULT_COLUMNS["step_description"]]).value = item.get("step_description", "")
        ws.cell(row=row_idx, column=header_to_col[RESULT_COLUMNS["mirapolis_action"]]).value = item.get("mirapolis_action", "")


def merge_artifact2_sheet(wb, artifact2: List[Dict[str, Any]]) -> None:
    if ARTIFACT2_SHEET not in wb.sheetnames:
        ws = wb.create_sheet(ARTIFACT2_SHEET)
        ws.append(["L2", "Категория", "Требование"])
    ws = wb[ARTIFACT2_SHEET]
    existing = set()
    for l2, category, req in ws.iter_rows(min_row=2, values_only=True):
        existing.add((l2, category, req))

    new_rows: List[Tuple[str, str, str]] = []
    for item in artifact2:
        l2 = item.get("l2_name", "")
        reqs = item.get("requirements", {})
        for category, items in reqs.items():
            for text in items or []:
                candidate = (l2, category, text)
                if candidate not in existing:
                    existing.add(candidate)
                    new_rows.append(candidate)
    for row in new_rows:
        ws.append(list(row))


def merge_workbooks_preserve(old_wb: Workbook, new_wb: Workbook, process_sheet_name: str):
    """
    Переносит непустые значения из new_wb в old_wb для sheet process_sheet_name.
    Добавляет колонки из new_wb, если их нет в old_wb.
    Объединяет ARTIFACT2_SHEET с дедупом.
    """
    if process_sheet_name not in new_wb.sheetnames:
        return

    old_ws = old_wb[process_sheet_name]
    new_ws = new_wb[process_sheet_name]

    header_to_col_old = {cell.value: cell.column for cell in old_ws[1]}
    header_to_col_new = {cell.value: cell.column for cell in new_ws[1]}

    # Добавляем отсутствующие колонки
    for col_name in header_to_col_new.keys():
        if col_name not in header_to_col_old:
            old_ws.cell(row=1, column=old_ws.max_column + 1).value = col_name
            header_to_col_old[col_name] = old_ws.max_column

    # Перенос непустых значений
    for r in range(2, new_ws.max_row + 1):
        for col_name, new_col_idx in header_to_col_new.items():
            new_val = new_ws.cell(row=r, column=new_col_idx).value
            if new_val is not None and str(new_val).strip() != "":
                old_col_idx = header_to_col_old[col_name]
                old_ws.cell(row=r, column=old_col_idx).value = new_val

    # Объединение ФТ Mirapolis
    if ARTIFACT2_SHEET in new_wb.sheetnames:
        if ARTIFACT2_SHEET not in old_wb.sheetnames:
            old_ws2 = old_wb.create_sheet(ARTIFACT2_SHEET)
            old_ws2.append(["L2", "Категория", "Требование"])
        else:
            old_ws2 = old_wb[ARTIFACT2_SHEET]
        new_ws2 = new_wb[ARTIFACT2_SHEET]

        existing = set()
        for l2, cat, req in old_ws2.iter_rows(min_row=2, values_only=True):
            existing.add(((l2 or ""), (cat or ""), (req or "")))

        for r in range(2, new_ws2.max_row + 1):
            l2 = new_ws2.cell(row=r, column=1).value or ""
            cat = new_ws2.cell(row=r, column=2).value or ""
            req = new_ws2.cell(row=r, column=3).value or ""
            candidate = (l2, cat, req)
            if candidate not in existing:
                old_ws2.append(list(candidate))
                existing.add(candidate)


def update_dataframe_with_artifact1(df: pd.DataFrame, artifact_rows: List[Dict[str, Any]]) -> None:
    for item in artifact_rows:
        df_idx = int(item["row_index"]) - 2
        if df_idx < 0 or df_idx >= len(df):
            continue
        df.loc[df_idx, RESULT_COLUMNS["trigger"]] = item.get("trigger", "")
        df.loc[df_idx, RESULT_COLUMNS["executor"]] = item.get("executor", "")
        df.loc[df_idx, RESULT_COLUMNS["step_description"]] = item.get("step_description", "")
        df.loc[df_idx, RESULT_COLUMNS["mirapolis_action"]] = item.get("mirapolis_action", "")
        df.loc[df_idx, "status"] = "done"


def process_excel(cfg: PipelineConfig) -> None:
    logging.info("Запуск обработки: %s", cfg)
    os.makedirs(cfg.log_dir, exist_ok=True)
    os.makedirs(cfg.state_dir, exist_ok=True)
    ensure_output_workbook(cfg.input_xlsx, cfg.output_xlsx)

    wb = load_workbook(cfg.output_xlsx)
    process_sheet = get_process_sheet_name(wb)
    ensure_result_columns(wb, process_sheet)
    wb.save(cfg.output_xlsx)
    df = load_dataframe(cfg.output_xlsx, process_sheet)

    batch_idx = 1
    client = GeminiClient(cfg.gemini_api_key, cfg.gemini_model, cfg.log_dir)

    while True:
        batch_df = select_batch(df, cfg.batch_size)
        if batch_df.empty:
            logging.info("Необработанных строк нет.")
            break

        payload = build_batch_payload(batch_df)
        with open(cfg.prompt_file, "r", encoding="utf-8") as f:
            prompt_text = f.read()

        # Сохраняем pre-batch CSV
        pre_batch_csv = os.path.join(cfg.log_dir, f"batch_{batch_idx}_pre.csv")
        batch_df.to_csv(pre_batch_csv, index=False, encoding="utf-8-sig")
        logging.info("Сохранили входной CSV батча: %s", pre_batch_csv)

        response = client.call(prompt_text, payload, batch_idx)

        # Если Gemini вернул файл xlsx
        if isinstance(response, dict) and response.get("filled_xlsx_path"):
            filled_path = response["filled_xlsx_path"]
            logging.info("Gemini вернул готовый XLSX: %s. Выполняю merge.", filled_path)
            new_wb = load_workbook(filled_path)
            merge_workbooks_preserve(wb, new_wb, process_sheet)
            wb.save(cfg.output_xlsx)

            # Если в parsed были данные artifact1/2 — применяем их (если есть)
            artifact1_rows = response.get("artifact1_rows", [])
            if artifact1_rows:
                apply_artifact1_rows(wb, process_sheet, artifact1_rows)
                update_dataframe_with_artifact1(df, artifact1_rows)
            artifact2_by_l2 = response.get("artifact2_by_l2", [])
            if artifact2_by_l2:
                merge_artifact2_sheet(wb, artifact2_by_l2)

            wb.save(cfg.output_xlsx)
            snapshot_path = os.path.join(cfg.state_dir, f"output_batch_{batch_idx}.xlsx")
            wb.save(snapshot_path)
            logging.info("Батч %s обработан (xlsx-ответ), снапшот: %s", batch_idx, snapshot_path)
            # Обновляем df из нового output
            df = load_dataframe(cfg.output_xlsx, process_sheet)

        else:
            # Обычная логика: JSON-ответы
            artifact1_rows = response.get("artifact1_rows", [])
            artifact2_by_l2 = response.get("artifact2_by_l2", [])
            apply_artifact1_rows(wb, process_sheet, artifact1_rows)
            update_dataframe_with_artifact1(df, artifact1_rows)
            merge_artifact2_sheet(wb, artifact2_by_l2)

            wb.save(cfg.output_xlsx)
            snapshot_path = os.path.join(cfg.state_dir, f"output_batch_{batch_idx}.xlsx")
            wb.save(snapshot_path)
            logging.info("Батч %s обработан, снапшот: %s", batch_idx, snapshot_path)

        # Сохраняем пост-batch CSV
        post_batch_csv = os.path.join(cfg.log_dir, f"batch_{batch_idx}_post.csv")
        df.to_csv(post_batch_csv, index=False, encoding="utf-8-sig")
        logging.info("Сохранили выходной CSV батча: %s", post_batch_csv)

        # Если требуется перезаписать input.xlsx текущим output
        if cfg.overwrite_input:
            try:
                if os.path.abspath(cfg.input_xlsx) != os.path.abspath(cfg.output_xlsx):
                    shutil.copyfile(cfg.output_xlsx, cfg.input_xlsx)
                    logging.info("Перезаписан исходный файл input.xlsx новой версией output.xlsx")
            except Exception as exc:
                logging.warning("Не удалось перезаписать input.xlsx: %s", exc)

        batch_idx += 1


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = PipelineConfig.from_env_and_args()
    process_excel(cfg)


if __name__ == "__main__":
    main()
