# Файл: main.py

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import queue
import threading
import os
from datetime import datetime

from config import ConfigManager
from controller import AnalysisController
from services import DataPreprocessor, FileHandler
from processor import PipelineConfig, process_excel

class MainApplication:
    """
    Главный класс приложения с графическим интерфейсом на Tkinter.
    Версия 1.2 - Исправлена ошибка вызова FileHandler, улучшена логика.
    """
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Анализатор Отзывов v1.2")
        self.root.geometry("850x700")
        self.root.minsize(700, 550)

        # --- Состояние приложения ---
        self.config = None
        self.worker_thread = None
        self.is_running = False
        self.input_file_path = ""
        self.prepared_file_path = "" # Путь к файлу после маскировки
        self.stop_event = threading.Event()
        self.log_queue = queue.Queue()
        self.pipeline_thread = None

        try:
            self.config = ConfigManager()
        except (FileNotFoundError, ValueError) as e:
            messagebox.showerror("Критическая ошибка конфигурации", f"{e}\n\nПриложение будет закрыто.")
            self.root.destroy()
            return

        self._setup_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _setup_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)

        file_frame = ttk.Frame(self.root, padding="10")
        file_frame.grid(row=0, column=0, sticky="ew")
        file_frame.columnconfigure(1, weight=1)
        
        ttk.Label(file_frame, text="1. Исходный файл:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.file_path_label = ttk.Label(file_frame, text="Файл не выбран", anchor="w", relief="sunken", padding=5, foreground="blue")
        self.file_path_label.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        self.select_file_button = ttk.Button(file_frame, text="Выбрать XLSX...", command=self._select_file)
        self.select_file_button.grid(row=0, column=2, sticky="e", padx=5, pady=5)

        prep_frame = ttk.LabelFrame(self.root, text="2. Подготовка данных (Опционально)", padding="10")
        prep_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        prep_frame.columnconfigure(1, weight=1)
        prep_frame.columnconfigure(3, weight=1)

        ttk.Label(prep_frame, text="Названия для поиска:").grid(row=0, column=0, columnspan=2, sticky="w", padx=5)
        ttk.Label(prep_frame, text="Заменить на:").grid(row=0, column=2, sticky="w", padx=5)

        self.company_vars = [tk.StringVar() for _ in range(4)]
        ttk.Entry(prep_frame, textvariable=self.company_vars[0]).grid(row=1, column=0, sticky="ew", padx=5, pady=2)
        ttk.Entry(prep_frame, textvariable=self.company_vars[1]).grid(row=1, column=1, sticky="ew", padx=5, pady=2)
        ttk.Entry(prep_frame, textvariable=self.company_vars[2]).grid(row=2, column=0, sticky="ew", padx=5, pady=2)
        ttk.Entry(prep_frame, textvariable=self.company_vars[3]).grid(row=2, column=1, sticky="ew", padx=5, pady=2)
        
        self.company_mask_var = tk.StringVar(value="[КОМПАНИЯ]")
        ttk.Entry(prep_frame, textvariable=self.company_mask_var).grid(row=1, column=2, sticky="ew", padx=5, pady=2)

        self.mask_company_button = ttk.Button(prep_frame, text="Найти и заменить", command=self._run_company_masking)
        self.mask_company_button.grid(row=1, column=3, rowspan=2, sticky="nsew", padx=5, pady=2)
        
        ttk.Separator(prep_frame, orient='horizontal').grid(row=3, column=0, columnspan=4, sticky="ew", pady=10)
        
        self.mask_fio_button = ttk.Button(prep_frame, text="Найти и скрыть инициалы и ФИО", command=self._run_fio_masking)
        self.mask_fio_button.grid(row=4, column=0, columnspan=4, sticky="ew", padx=5, pady=2)

        analysis_frame = ttk.LabelFrame(self.root, text="3. Запуск анализа", padding="10")
        analysis_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
        analysis_frame.columnconfigure(1, weight=1)
        analysis_frame.rowconfigure(1, weight=1)

        ttk.Label(analysis_frame, text="Модель Gemini:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.model_var = tk.StringVar()
        model_names = self.config.get_model_names()
        self.model_selector = ttk.Combobox(analysis_frame, textvariable=self.model_var, values=model_names, state='readonly')
        if model_names: self.model_selector.set(model_names[0])
        self.model_selector.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        
        self.start_stop_button = ttk.Button(analysis_frame, text="Начать анализ", command=self._start_analysis)
        self.start_stop_button.grid(row=0, column=2, sticky="e", padx=5, pady=5)

        self.log_text = scrolledtext.ScrolledText(analysis_frame, wrap=tk.WORD, state='disabled', font=("Courier New", 9))
        self.log_text.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=5)
        
        progress_frame = ttk.Frame(self.root, padding="10 5 10 5")
        progress_frame.grid(row=3, column=0, sticky="ew")
        progress_frame.columnconfigure(0, weight=1)
        self.progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', mode='determinate')
        self.progress_bar.grid(row=0, column=0, sticky="ew", padx=(0,10))
        self.status_label = ttk.Label(self.root, text=" Готово к работе", anchor="w", relief="sunken")
        self.status_label.grid(row=4, column=0, sticky="ew")

        # --- Новый блок: батчевый анализ через Gemini ---
        pipeline_frame = ttk.LabelFrame(self.root, text="4. Батчевый анализ Gemini (L1-L5)", padding="10")
        pipeline_frame.grid(row=5, column=0, sticky="ew", padx=10, pady=5)
        pipeline_frame.columnconfigure(1, weight=1)

        ttk.Label(pipeline_frame, text="Промт-файл:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.prompt_var = tk.StringVar()
        ttk.Entry(pipeline_frame, textvariable=self.prompt_var).grid(row=0, column=1, sticky="ew", padx=5, pady=2)
        ttk.Button(pipeline_frame, text="Выбрать...", command=self._select_prompt).grid(row=0, column=2, padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Выходной XLSX:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        self.output_var = tk.StringVar()
        ttk.Entry(pipeline_frame, textvariable=self.output_var).grid(row=1, column=1, sticky="ew", padx=5, pady=2)
        ttk.Button(pipeline_frame, text="Сохранить как...", command=self._select_output).grid(row=1, column=2, padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Batch size:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        self.batch_size_var = tk.IntVar(value=10)
        ttk.Spinbox(pipeline_frame, from_=1, to=100, textvariable=self.batch_size_var, width=7).grid(row=2, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Gemini API key:").grid(row=3, column=0, sticky="w", padx=5, pady=2)
        self.api_key_var = tk.StringVar(value=os.getenv("GEMINI_API_KEY", ""))
        ttk.Entry(pipeline_frame, textvariable=self.api_key_var, show="*").grid(row=3, column=1, sticky="ew", padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Модель Gemini:").grid(row=4, column=0, sticky="w", padx=5, pady=2)
        self.pipeline_model_var = tk.StringVar(value="gemini-2.0-flash")
        ttk.Combobox(
            pipeline_frame,
            textvariable=self.pipeline_model_var,
            values=["gemini-2.0-flash", "gemini-2.5-flash", "gemini-2.5-pro"],
            state="readonly"
        ).grid(row=4, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Папка логов:").grid(row=5, column=0, sticky="w", padx=5, pady=2)
        self.log_dir_var = tk.StringVar(value="logs")
        ttk.Entry(pipeline_frame, textvariable=self.log_dir_var).grid(row=5, column=1, sticky="ew", padx=5, pady=2)

        ttk.Label(pipeline_frame, text="Папка снапшотов:").grid(row=6, column=0, sticky="w", padx=5, pady=2)
        self.state_dir_var = tk.StringVar(value="snapshots")
        ttk.Entry(pipeline_frame, textvariable=self.state_dir_var).grid(row=6, column=1, sticky="ew", padx=5, pady=2)

        self.pipeline_button = ttk.Button(pipeline_frame, text="Запустить батчи", command=self._start_pipeline)
        self.pipeline_button.grid(row=0, column=3, rowspan=3, sticky="ns", padx=10, pady=2)
        
    def _get_current_file_for_analysis(self) -> str:
        """Возвращает путь к файлу, который должен быть использован для анализа."""
        return self.prepared_file_path or self.input_file_path

    def _run_masking(self, masking_function, *args):
        source_path = self._get_current_file_for_analysis()
        if not source_path:
            messagebox.showwarning("Внимание", "Пожалуйста, сначала выберите исходный файл.")
            return

        try:
            self.status_label.config(text="Идет подготовка данных...")
            self.root.update_idletasks()
            
            # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Используем правильный метод load_data ---
            # Передаем пустой путь для intermediate, чтобы он всегда читал XLSX-файл
            df = FileHandler.load_data(source_path, intermediate_path="")
            
            # Имя первого столбца берем динамически из исходного файла
            comment_column = df.columns[1] # [0] - это наш internal_comment_id
            
            df_processed = masking_function(df, comment_column, *args)
            
            base, _ = os.path.splitext(self.input_file_path)
            self.prepared_file_path = f"{base}_prepared.xlsx"
            
            FileHandler.save_to_formatted_xlsx(df_processed, self.prepared_file_path)
            
            display_path = "..." + self.prepared_file_path[-50:] if len(self.prepared_file_path) > 53 else self.prepared_file_path
            self.file_path_label.config(text=display_path, foreground="green")
            self.status_label.config(text=f"Данные подготовлены и сохранены в: {os.path.basename(self.prepared_file_path)}")
            messagebox.showinfo("Успех", "Маскировка данных успешно завершена. Новый файл готов для анализа.")

        except Exception as e:
            messagebox.showerror("Ошибка подготовки данных", str(e))
            self.status_label.config(text="Ошибка при подготовке данных.")

    def _run_company_masking(self):
        variants = [var.get() for var in self.company_vars if var.get()]
        mask = self.company_mask_var.get()
        if not variants:
            messagebox.showwarning("Внимание", "Введите хотя бы один вариант названия компании для поиска.")
            return
        self._run_masking(DataPreprocessor.mask_company_names, variants, mask)

    def _run_fio_masking(self):
        self._run_masking(DataPreprocessor.mask_fio)

    def _select_file(self):
        filetypes = (("Excel files", "*.xlsx"), ("All files", "*.*"))
        filepath = filedialog.askopenfilename(title="Выберите файл для анализа", filetypes=filetypes)
        if filepath:
            self.input_file_path = filepath
            self.prepared_file_path = ""
            display_path = "..." + filepath[-50:] if len(filepath) > 53 else filepath
            self.file_path_label.config(text=display_path, foreground="blue")
            self.status_label.config(text=f"Выбран файл: {os.path.basename(filepath)}")
            # Автозаполнить выходной путь для батчевого анализа
            base, ext = os.path.splitext(filepath)
            self.output_var.set(f"{base}_processed.xlsx")
            
    def _start_analysis(self):
        analysis_path = self._get_current_file_for_analysis()
        if not analysis_path:
            messagebox.showwarning("Внимание", "Пожалуйста, сначала выберите файл для анализа.")
            return

        self.is_running = True
        self.stop_event.clear()
        
        self.start_stop_button.config(text="Остановить", command=self._stop_analysis)
        self.select_file_button.config(state='disabled')
        self.model_selector.config(state='disabled')
        self.mask_company_button.config(state='disabled')
        self.mask_fio_button.config(state='disabled')
        
        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        self.progress_bar['value'] = 0
        self.status_label.config(text="Анализ запущен...")
        
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
        os.makedirs(output_dir, exist_ok=True)
        
        self.worker_thread = AnalysisController(
            input_path=analysis_path,
            output_dir=output_dir,
            config=self.config,
            log_queue=self.log_queue,
            stop_event=self.stop_event
        )
        self.worker_thread.set_model(self.model_var.get())
        self.worker_thread.start()
        self.root.after(100, self._process_log_queue)

    def _stop_analysis(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.stop_event.set()
            self.start_stop_button.config(text="Останавливаем...", state='disabled')
            self.status_label.config(text="Процесс останавливается...")

    # --- Батчевый пайплайн ---
    def _select_prompt(self):
        filepath = filedialog.askopenfilename(title="Выберите файл промта", filetypes=(("Markdown", "*.md"), ("All files", "*.*")))
        if filepath:
            self.prompt_var.set(filepath)

    def _select_output(self):
        filepath = filedialog.asksaveasfilename(title="Куда сохранить результат", defaultextension=".xlsx", filetypes=(("Excel", "*.xlsx"),))
        if filepath:
            self.output_var.set(filepath)

    def _start_pipeline(self):
        if self.pipeline_thread and self.pipeline_thread.is_alive():
            messagebox.showinfo("Идёт обработка", "Дождитесь завершения текущего запуска.")
            return
        input_path = self._get_current_file_for_analysis()
        prompt_path = self.prompt_var.get().strip()
        output_path = self.output_var.get().strip()
        if not input_path:
            messagebox.showwarning("Внимание", "Выберите входной XLSX.")
            return
        if not prompt_path:
            messagebox.showwarning("Внимание", "Укажите файл промта.")
            return
        if not output_path:
            messagebox.showwarning("Внимание", "Укажите путь для выходного XLSX.")
            return

        api_key = self.api_key_var.get().strip()
        if not api_key:
            messagebox.showwarning("Внимание", "Укажите GEMINI_API_KEY.")
            return
        os.environ["GEMINI_API_KEY"] = api_key

        batch_size = self.batch_size_var.get() or 10
        model = self.pipeline_model_var.get()
        log_dir = self.log_dir_var.get() or "logs"
        state_dir = self.state_dir_var.get() or "snapshots"

        self.pipeline_button.config(text="В работе...", state="disabled")
        self.status_label.config(text="Батчевый анализ запущен...")

        def runner():
            try:
                cfg = PipelineConfig(
                    input_xlsx=input_path,
                    output_xlsx=output_path,
                    prompt_file=prompt_path,
                    batch_size=batch_size,
                    gemini_api_key=api_key,
                    gemini_model=model,
                    log_dir=log_dir,
                    state_dir=state_dir,
                )
                process_excel(cfg)
                self.log_queue.put("__PIPE_DONE__")
            except Exception as exc:
                self.log_queue.put(f"PIPE_ERROR: {exc}")

        self.pipeline_thread = threading.Thread(target=runner, daemon=True)
        self.pipeline_thread.start()

    def _process_log_queue(self):
        try:
            while not self.log_queue.empty():
                message = self.log_queue.get_nowait()
                
                # --- УЛУЧШЕННАЯ ЛОГИКА ОБРАБОТКИ СООБЩЕНИЙ ---
                if message == "__DONE__" or message == "__STOPPED__":
                    self._on_analysis_complete(was_stopped=(message == "__STOPPED__"))
                    return
                elif message.startswith("__PROGRESS__;"):
                    try:
                        _, current, total = message.split(';')
                        progress = (int(current) / int(total)) * 100 if int(total) > 0 else 0
                        self.progress_bar['value'] = progress
                    except (IndexError, ValueError):
                        pass
                elif message == "__PIPE_DONE__":
                    self.pipeline_button.config(text="Запустить батчи", state="normal")
                    self.status_label.config(text="Батчевый анализ завершен.")
                elif message.startswith("PIPE_ERROR"):
                    self.pipeline_button.config(text="Запустить батчи", state="normal")
                    self.status_label.config(text="Ошибка батчевого анализа")
                    messagebox.showerror("Ошибка", message)
                else:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    self.log_text.config(state='normal')
                    self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
                    self.log_text.see(tk.END)
                    self.log_text.config(state='disabled')
        finally:
            if self.is_running:
                self.root.after(100, self._process_log_queue)

    def _on_analysis_complete(self, was_stopped: bool):
        self.is_running = False
        self.start_stop_button.config(text="Начать анализ", command=self._start_analysis, state='normal')
        self.select_file_button.config(state='normal')
        self.model_selector.config(state='readonly')
        self.mask_company_button.config(state='normal')
        self.mask_fio_button.config(state='normal')
        
        if was_stopped:
            self.status_label.config(text="Анализ остановлен. Промежуточные результаты сохранены.")
            messagebox.showinfo("Остановлено", "Процесс анализа был остановлен. Промежуточные результаты сохранены.")
        else:
            self.progress_bar['value'] = 100
            self.status_label.config(text="Готово. Анализ успешно завершен.")
            messagebox.showinfo("Завершено", "Анализ успешно завершен!")
            
    def _on_closing(self):
        if self.is_running:
            if messagebox.askyesno("Подтверждение", "Процесс анализа еще не завершен. Вы уверены, что хотите выйти?"):
                self._stop_analysis()
                # Даем потоку немного времени на завершение
                self.root.after(500, self.root.destroy)
        else:
            self.root.destroy()

if __name__ == "__main__":
    app_root = tk.Tk()
    app = MainApplication(app_root)
    app_root.mainloop()