# Файл: services.py

import json
import logging
import os
import re 
import time
from typing import List, Dict, Any

import pandas as pd
from google import genai
from google.genai import types
from openpyxl.styles import Alignment # <-- Добавляем импорт для форматирования

# Настройка базового логгера для модуля
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FileHandler:
    """Класс, отвечающий за все операции с файлами."""
    @staticmethod
    def load_data(input_path: str, intermediate_path: str) -> pd.DataFrame:
        """
        Загружает данные. Приоритетно из промежуточного CSV, если он существует.
        В противном случае - из исходного XLSX.
        """
        if os.path.exists(intermediate_path):
            logging.info(f"Найден промежуточный файл: {intermediate_path}. Возобновляем работу.")
            try:
                # Загружаем CSV, убеждаемся, что internal_comment_id читается как число
                df = pd.read_csv(intermediate_path, sep=';', encoding='utf-8-sig', dtype={'internal_comment_id': int})
                logging.info(f"Успешно загружено {len(df)} строк из промежуточного файла.")
                return df
            except Exception as e:
                logging.error(f"Не удалось прочитать промежуточный CSV файл: {e}. Начинаем с исходного XLSX.")
        
        logging.info(f"Загрузка данных из исходного файла: {input_path}")
        try:
            df = pd.read_excel(input_path, engine='openpyxl')
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'internal_comment_id'}, inplace=True)
            logging.info(f"Успешно загружено {len(df)} строк.")
            return df
        except FileNotFoundError:
            logging.error(f"Исходный файл не найден по пути: {input_path}")
            raise
        except Exception as e:
            logging.error(f"Произошла ошибка при чтении XLSX файла: {e}")
            raise

    @staticmethod
    def save_to_csv(dataframe: pd.DataFrame, filepath: str) -> None:
        # (Этот метод остается без изменений)
        logging.info(f"Сохранение промежуточных результатов в: {filepath}")
        try:
            dataframe.to_csv(filepath, sep=';', encoding='utf-8-sig', index=False)
            logging.info("Промежуточный результат успешно сохранен.")
        except Exception as e:
            logging.error(f"Не удалось сохранить промежуточный CSV файл: {e}")
            raise

    @staticmethod
    def save_to_formatted_xlsx(dataframe: pd.DataFrame, filepath: str) -> None:
        """
        Сохраняет итоговый DataFrame в отформатированный XLSX файл.
        Настраивает ширину столбцов, перенос текста и замораживает заголовок.
        """
        logging.info(f"Сохранение отформатированного результата в: {filepath}")
        
        df_to_save = dataframe.drop(columns=['internal_comment_id'], errors='ignore')

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_to_save.to_excel(writer, index=False, sheet_name='Анализ')
                
                # Получаем доступ к листу
                worksheet = writer.sheets['Анализ']

                # Настраиваем ширину и перенос текста
                column_widths = {
                    'A': 55,  # Идеи и предложения...
                    'B': 15,  # llm_sentiment
                    'C': 35,  # llm_environment
                    'D': 35,  # llm_l2_factor
                    'E': 45,  # llm_l3_factor
                    'F': 30,  # llm_justification_taxonomy
                    'G': 30,  # llm_ejm_stage
                    'H': 35,  # llm_ejm_step
                    'I': 30   # llm_justification_ejm
                }
                
                wrap_columns = ['A', 'F', 'I'] # Колонки с переносом текста

                for col_letter, width in column_widths.items():
                    worksheet.column_dimensions[col_letter].width = width
                
                alignment_wrap = Alignment(wrap_text=True, vertical='top')

                for col_letter in wrap_columns:
                    for cell in worksheet[col_letter]:
                        cell.alignment = alignment_wrap
                
                # Замораживаем верхнюю строку
                worksheet.freeze_panes = 'A2'

            logging.info("Итоговый результат успешно сохранен и отформатирован.")
        except Exception as e:
            logging.error(f"Не удалось сохранить итоговый XLSX файл: {e}")
            raise


class GeminiService:
    """
    Класс-сервис для взаимодействия с Google Gemini API (v. google-genai).
    Использует клиент-ориентированный подход и корректную структуру GenerateContentConfig.
    """
    def __init__(self, api_key: str, model_name: str, prompts_dir: str = 'prompts'):
        self.model_name = f'models/{model_name}'
        self.prompts_dir = prompts_dir
        
        try:
            self.client = genai.Client(api_key=api_key)
        except Exception as e:
            logging.error(f"Не удалось инициализировать genai.Client: {e}")
            raise
            
        system_prompt = self._load_prompts()
        
        # Формирование safety_settings как списка объектов
        safety_categories = [
            "HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
            "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"
        ]
        safety_settings_list = [
            types.SafetySetting(category=cat, threshold="BLOCK_NONE") for cat in safety_categories
        ]

        # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Создание единого объекта конфигурации ---
        # Все параметры (system_instruction, safety_settings, temperature)
        # упаковываются внутрь одного объекта GenerateContentConfig.
        self.request_config = types.GenerateContentConfig(
            temperature=0.25,
            system_instruction=system_prompt,
            safety_settings=safety_settings_list,
        )
        
        self.RPM_LIMITS = {
            'models/gemini-2.5-flash-lite': 15, 'models/gemini-2.5-flash': 10,
            'models/gemini-2.5-pro': 5, 'models/gemini-2.0-flash': 15, 'models/gemini-2.0-flash-lite': 30,
        }
        self.request_delay = 60.0 / self.RPM_LIMITS.get(self.model_name, 15)
        self.last_api_call_time = 0

    def _load_prompts(self) -> str:
        logging.info("Загрузка модулей промпта...")
        prompt_files = ['prompt_main.md', 'prompt_taxonomy.md', 'prompt_ejm.md']
        full_prompt = []
        for filename in prompt_files:
            filepath = os.path.join(self.prompts_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    full_prompt.append(f.read())
            except FileNotFoundError:
                logging.error(f"Критическая ошибка: файл промпта '{filepath}' не найден.")
                raise
        logging.info("Промпты успешно загружены и объединены.")
        return "\n\n---\n\n".join(full_prompt)

    def analyze_batch(self, batch_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        time_since_last_call = time.time() - self.last_api_call_time
        if time_since_last_call < self.request_delay:
            sleep_time = self.request_delay - time_since_last_call
            logging.info(f"Соблюдение лимита RPM. Пауза: {sleep_time:.2f} сек.")
            time.sleep(sleep_time)

        user_prompt = (
            f"Проанализируй следующий пакет из {len(batch_data)} комментариев. "
            f"Верни результат СТРОГО как JSON-массив объектов без какого-либо другого текста.\n\n"
            f"{json.dumps(batch_data, ensure_ascii=False, indent=2)}"
        )
        
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                self.last_api_call_time = time.time()
                logging.info(f"Отправка запроса к API для {len(batch_data)} строк (попытка {attempt + 1})...")
                
                # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Аргумент называется 'config', а не 'generation_config' ---
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=[user_prompt],
                    config=self.request_config # Правильное имя аргумента
                )
                
                if not response.candidates:
                     raise ValueError(f"Ответ от API был заблокирован. Причина: {getattr(response.prompt_feedback, 'block_reason', 'Неизвестно')}")
                
                response_text = response.text.strip()
                if response_text.startswith("```json"):
                    response_text = response_text[7:-3].strip()
                
                parsed_response = json.loads(response_text)

                if isinstance(parsed_response, list) and all(isinstance(item, dict) for item in parsed_response):
                     if len(parsed_response) != len(batch_data):
                         logging.warning(f"API вернуло {len(parsed_response)} результатов вместо {len(batch_data)}. Попытка сопоставить по ID.")
                     logging.info("Успешный ответ от API получен и распарсен.")
                     return parsed_response
                else:
                    raise ValueError("Структура ответа не является JSON-массивом объектов.")

            except json.JSONDecodeError:
                logging.error(f"Ошибка декодирования JSON. Ответ модели: '{response.text[:500]}...'")
                return self._create_error_batch(batch_data, "Ошибка декодирования JSON")
            
            except Exception as e:
                if "Resource has been exhausted" in str(e) or "service is currently unavailable" in str(e):
                    logging.warning(f"Сервисная ошибка API (попытка {attempt + 1}/{max_retries}): {type(e).__name__}. Повтор через {retry_delay} сек.")
                    time.sleep(retry_delay)
                    retry_delay *= 2 
                else:
                    logging.error(f"Непредвиденная ошибка при запросе к API: {type(e).__name__} - {e}")
                    return self._create_error_batch(batch_data, f"Unexpected Error: {e}")

        logging.error(f"Не удалось получить ответ от API после {max_retries} попыток.")
        return self._create_error_batch(batch_data, "Ошибка API после нескольких попыток")

    def _create_error_batch(self, batch_data: List[Dict[str, Any]], error_message: str) -> List[Dict[str, Any]]:
        error_result = {
            "comment_id": 0,
            "sentiment": "Ошибка", "environment": "Ошибка", "l2_factor": "Ошибка",
            "l3_factor": "Ошибка", "justification_taxonomy": error_message,
            "ejm_stage": "Ошибка", "ejm_step": "Ошибка", "justification_ejm": error_message,
        }
        error_list = []
        for item in batch_data:
            result = error_result.copy()
            result['comment_id'] = item['comment_id']
            error_list.append(result)
        return error_list
        
# --- НОВЫЙ КЛАСС ДЛЯ ПРЕДОБРАБОТКИ ДАННЫХ ---
class DataPreprocessor:
    """
    Класс для выполнения операций по очистке и анонимизации данных в DataFrame.
    """
    @staticmethod
    def mask_company_names(
        df: pd.DataFrame, 
        comment_column: str, 
        variants: List[str], 
        mask: str
    ) -> pd.DataFrame:
        """
        Выполняет регистронезависимую замену названий компаний.

        Args:
            df (pd.DataFrame): Исходный DataFrame.
            comment_column (str): Название столбца с комментариями.
            variants (List[str]): Список вариантов названий для поиска.
            mask (str): Строка для замены.

        Returns:
            pd.DataFrame: DataFrame с замаскированными названиями.
        """
        if not mask:
            # Если маска пустая, ничего не делаем
            return df
            
        logging.info(f"Маскировка названий компаний на '{mask}'...")
        # Удаляем пустые строки из вариантов
        search_variants = [v for v in variants if v]
        if not search_variants:
            return df

        # Создаем одно регулярное выражение для всех вариантов
        # `|` означает "ИЛИ", `re.IGNORECASE` делает поиск регистронезависимым
        regex_pattern = '|'.join(map(re.escape, search_variants))
        
        df[comment_column] = df[comment_column].str.replace(
            regex_pattern, mask, regex=True, flags=re.IGNORECASE
        )
        logging.info("Маскировка названий завершена.")
        return df

    @staticmethod
    def mask_fio(df: pd.DataFrame, comment_column: str, mask: str = "[ФИО СОТРУДНИКА]") -> pd.DataFrame:
        """
        Эвристический поиск и маскировка ФИО и инициалов с помощью регулярных выражений.

        Args:
            df (pd.DataFrame): Исходный DataFrame.
            comment_column (str): Название столбца с комментариями.
            mask (str): Строка для замены.

        Returns:
            pd.DataFrame: DataFrame с замаскированными ФИО.
        """
        logging.info("Эвристическая маскировка ФИО и инициалов...")

        # Шаблоны (от более конкретных к более общим)
        patterns = [
            # 1. Фамилия Имя Отчество (напр., "Иванов Иван Иванович")
            r'\b([А-ЯЁ][а-яё]+)\s+([А-ЯЁ][а-яё]+)\s+([А-ЯЁ][а-яё]+)\b',
            # 2. Фамилия И.О. (напр., "Иванов И.И." или "Иванов И. И.")
            r'\b([А-ЯЁ][а-яё]+)\s+([А-ЯЁ]\.\s?[А-ЯЁ]\.)\b',
            # 3. И.О. Фамилия (напр., "И.И. Иванов" или "И. И. Иванов")
            r'\b([А-ЯЁ]\.\s?[А-ЯЁ]\.)\s+([А-ЯЁ][а-яё]+)\b',
        ]

        df_copy = df.copy()
        for pattern in patterns:
            df_copy[comment_column] = df_copy[comment_column].str.replace(
                pattern, mask, regex=True
            )
        
        logging.info("Маскировка ФИО завершена.")
        return df_copy