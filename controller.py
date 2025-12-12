# Файл: controller.py

import logging
import os
import threading
from queue import Queue
from typing import Dict, Any, List

import pandas as pd

from services import FileHandler, GeminiService
from config import ConfigManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AnalysisController(threading.Thread):
    # ... (__init__ и set_model без изменений) ...
    def __init__(
        self,
        input_path: str,
        output_dir: str,
        config: ConfigManager,
        log_queue: Queue,
        stop_event: threading.Event
    ):
        super().__init__()
        self.daemon = True
        self.input_path = input_path
        self.output_dir = output_dir
        self.config = config
        self.log_queue = log_queue
        self.stop_event = stop_event
        self.api_key = self.config.get_api_key()
        self.model_name = config.get_model_names()[0] # Инициализируем модель по умолчанию
        self.batch_size = self.config.get_batch_size()
        self.save_interval = self.config.get_save_interval()
        self.file_handler = FileHandler()
        self.gemini_service = None

    def set_model(self, model_name: str):
        self.model_name = model_name

    def _log(self, message: str):
        self.log_queue.put(message)

    def _update_dataframe(self, df: pd.DataFrame, results: List[Dict[str, Any]]) -> None:
        """Обновляет DataFrame результатами, сопоставляя по 'internal_comment_id'."""
        results_map = {res.get('comment_id'): res for res in results}
        
        for idx in df.index:
            comment_id = df.loc[idx, 'internal_comment_id']
            if comment_id in results_map:
                result = results_map[comment_id]
                df.loc[idx, 'llm_sentiment'] = result.get('sentiment')
                df.loc[idx, 'llm_environment'] = result.get('environment')
                df.loc[idx, 'llm_l2_factor'] = result.get('l2_factor')
                df.loc[idx, 'llm_l3_factor'] = result.get('l3_factor')
                df.loc[idx, 'llm_justification_taxonomy'] = result.get('justification_taxonomy')
                df.loc[idx, 'llm_ejm_stage'] = result.get('ejm_stage')
                df.loc[idx, 'llm_ejm_step'] = result.get('ejm_step')
                df.loc[idx, 'llm_justification_ejm'] = result.get('justification_ejm')

    def run(self):
        """
        Основной метод, выполняемый в отдельном потоке.
        Реализует всю логику анализа файла с поддержкой возобновления.
        """
        df = None # Инициализируем DataFrame
        try:
            self._log("▶️ Контроллер анализа запущен...")
            
            self.gemini_service = GeminiService(api_key=self.api_key, model_name=self.model_name)
            
            base_filename = os.path.splitext(os.path.basename(self.input_path))[0]
            output_csv_path = os.path.join(self.output_dir, f"{base_filename}_processed_intermediate.csv")
            output_xlsx_path = os.path.join(self.output_dir, f"{base_filename}_processed_final.xlsx")

            # --- ИЗМЕНЕННАЯ ЛОГИКА ЗАГРУЗКИ ---
            df = self.file_handler.load_data(self.input_path, output_csv_path)
            total_rows = len(df)
            
            result_columns = [
                'llm_sentiment', 'llm_environment', 'llm_l2_factor', 'llm_l3_factor',
                'llm_justification_taxonomy', 'llm_ejm_stage', 'llm_ejm_step', 'llm_justification_ejm'
            ]
            for col in result_columns:
                if col not in df.columns:
                    df[col] = ""
            df[result_columns] = df[result_columns].fillna("").astype(str)
            
            # --- ЛОГИКА ВОЗОБНОВЛЕНИЯ ---
            # Фильтруем строки, которые еще не обработаны (где результат - пустая строка)
            unprocessed_df = df[df['llm_sentiment'] == ""]
            
            if len(unprocessed_df) == 0:
                self._log("✅ Все строки уже обработаны. Пропускаем анализ.")
            else:
                self._log(f"Найдено {len(unprocessed_df)} из {total_rows} необработанных строк для анализа.")

            processed_count_current_session = 0
            total_already_processed = total_rows - len(unprocessed_df)
            
            for i in range(0, len(unprocessed_df), self.batch_size):
                if self.stop_event.is_set():
                    self._log("⏹️ Получен сигнал остановки. Прерывание анализа...")
                    break

                batch_df = unprocessed_df.iloc[i:i + self.batch_size]
                
                batch_data_to_send = []
                comment_column_name = df.columns[1] # Второй столбец после нашего internal_comment_id
                
                for _, row in batch_df.iterrows():
                    comment_text = str(row[comment_column_name]) if pd.notna(row[comment_column_name]) else ""
                    batch_data_to_send.append({'comment_id': row['internal_comment_id'], 'text': comment_text})

                start_row_num_overall = total_already_processed + i + 1
                end_row_num_overall = min(start_row_num_overall + self.batch_size - 1, total_rows)
                self._log(f"⚙️ Обработка строк {start_row_num_overall}-{end_row_num_overall} из {total_rows}...")
                
                results = self.gemini_service.analyze_batch(batch_data_to_send)
                
                # Обновляем результаты в исходном DataFrame по индексам батча
                df.update(batch_df.assign(**{col: [res.get(col.replace('llm_', '')) for res in results] for col in result_columns}))
                self._update_dataframe(df.loc[batch_df.index], results)

                processed_count_current_session += len(batch_df)
                total_processed = total_already_processed + processed_count_current_session
                self._log(f"__PROGRESS__;{total_processed};{total_rows}")

                # Условие для промежуточного сохранения
                if total_processed % self.save_interval < self.batch_size and total_processed > 0 and total_processed < total_rows:
                    self.file_handler.save_to_csv(df, output_csv_path)
                    self._log(f"💾 Промежуточный результат сохранен ({total_processed} строк обработано).")
            
            if not self.stop_event.is_set():
                self._log("✅ Анализ всех строк завершен.")
            
            # --- ИЗМЕНЕННАЯ ЛОГИКА СОХРАНЕНИЯ ---
            self._log("Начинаем финальное сохранение...")
            self.file_handler.save_to_csv(df, output_csv_path)
            self._log(f"💾 Итоговый CSV сохранен: {output_csv_path}")
            self.file_handler.save_to_formatted_xlsx(df, output_xlsx_path)
            self._log(f"✨ Итоговый XLSX сохранен и отформатирован: {output_xlsx_path}")

        except Exception as e:
            error_message = f"КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__} - {e}"
            logging.exception("Критическая ошибка в потоке анализа:")
            self._log(f"🛑 {error_message}")
        finally:
            if 'df' in locals() and df is not None and self.stop_event.is_set():
                self._log("Сохранение прогресса после остановки...")
                self.file_handler.save_to_csv(df, output_csv_path)
                self.file_handler.save_to_formatted_xlsx(df, output_xlsx_path)
                self._log(f"💾 Промежуточный результат после остановки сохранен в CSV и XLSX.")
            self._log("__DONE__" if not self.stop_event.is_set() else "__STOPPED__")