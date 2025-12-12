# Файл: config.py

import configparser
import os
from typing import List

class ConfigManager:
    """
    Класс для управления конфигурацией приложения.
    Читает настройки из файла config.ini, валидирует их и предоставляет
    типизированный доступ к параметрам для других модулей приложения.
    """
    def __init__(self, filepath: str = 'config.ini'):
        """
        Инициализирует менеджер конфигурации.

        Args:
            filepath (str): Путь к файлу config.ini.

        Raises:
            FileNotFoundError: Если файл конфигурации не найден.
            configparser.Error: Если файл конфигурации имеет неверный формат.
        """
        self.filepath = filepath
        self.config = configparser.ConfigParser()

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(
                f"Файл конфигурации '{self.filepath}' не найден. "
                f"Пожалуйста, создайте его на основе шаблона."
            )

        try:
            self.config.read(self.filepath, encoding='utf-8')
        except configparser.Error as e:
            raise configparser.Error(
                f"Ошибка парсинга файла конфигурации '{self.filepath}': {e}"
            )

    def get_api_key(self) -> str:
        """
        Получает API ключ из конфигурации.

        Returns:
            str: API ключ Gemini.

        Raises:
            ValueError: Если секция [API] или ключ GEMINI_API_KEY отсутствуют,
                        или если ключ не был заменен пользователем.
        """
        try:
            api_key = self.config.get('API', 'GEMINI_API_KEY')
            if not api_key or api_key == 'YOUR_API_KEY_HERE':
                raise ValueError(
                    "API ключ не указан в файле config.ini. "
                    "Пожалуйста, вставьте ваш ключ в секцию [API]."
                )
            return api_key
        except (configparser.NoSectionError, configparser.NoOptionError):
            raise ValueError(
                "Секция [API] или ключ 'GEMINI_API_KEY' не найдены в config.ini."
            )

    def get_model_names(self) -> List[str]:
        """
        Получает список доступных моделей из конфигурации.

        Returns:
            List[str]: Список имен моделей.

        Raises:
            ValueError: Если секция [Models] или ключ available_models отсутствуют.
        """
        try:
            models_str = self.config.get('Models', 'available_models')
            if not models_str:
                return []
            # Парсим строку, разделенную запятыми, и убираем лишние пробелы
            return [model.strip() for model in models_str.split(',')]
        except (configparser.NoSectionError, configparser.NoOptionError):
            raise ValueError(
                "Секция [Models] или ключ 'available_models' не найдены в config.ini."
            )

    def get_batch_size(self) -> int:
        """
        Получает размер пакета для обработки из конфигурации.

        Returns:
            int: Количество строк в одном батче.

        Raises:
            ValueError: Если параметр не является целым положительным числом.
        """
        try:
            size = self.config.getint('Settings', 'batch_size')
            if size <= 0:
                raise ValueError("batch_size должен быть положительным числом.")
            return size
        except (configparser.NoSectionError, configparser.NoOptionError):
            raise ValueError(
                "Секция [Settings] или ключ 'batch_size' не найдены в config.ini."
            )
        except ValueError:
            raise ValueError("Значение 'batch_size' в config.ini должно быть целым числом.")

    def get_save_interval(self) -> int:
        """
        Получает интервал сохранения промежуточных результатов.

        Returns:
            int: Количество строк, после обработки которых происходит сохранение.

        Raises:
            ValueError: Если параметр не является целым положительным числом.
        """
        try:
            interval = self.config.getint('Settings', 'save_interval')
            if interval <= 0:
                raise ValueError("save_interval должен быть положительным числом.")
            return interval
        except (configparser.NoSectionError, configparser.NoOptionError):
            raise ValueError(
                "Секция [Settings] или ключ 'save_interval' не найдены в config.ini."
            )
        except ValueError:
            raise ValueError("Значение 'save_interval' в config.ini должно быть целым числом.")

# # Пример использования (для самостоятельного тестирования модуля)
# if __name__ == '__main__':
    # try:
        # # Для теста создадим временный config.ini
        # with open('config.ini', 'w', encoding='utf-8') as f:
            # f.write("""
# [API]
# GEMINI_API_KEY = A_FAKE_KEY_FOR_TESTING

# [Models]
# available_models = gemini-2.5-flash-lite, gemini-2.5-pro

# [Settings]
# batch_size = 10
# save_interval = 100
            # """)

        # config_manager = ConfigManager()
        # print(f"API Key loaded: {'*' * 10}{config_manager.get_api_key()[-4:]}")
        # print(f"Available models: {config_manager.get_model_names()}")
        # print(f"Batch size: {config_manager.get_batch_size()}")
        # print(f"Save interval: {config_manager.get_save_interval()}")

        # # Удаляем временный файл после теста
        # os.remove('config.ini')

    # except (FileNotFoundError, ValueError, configparser.Error) as e:
        # print(f"Ошибка при работе с конфигурацией: {e}")