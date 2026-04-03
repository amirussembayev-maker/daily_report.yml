import os
import re
import json
import requests
import pandas as pd
import gspread
from datetime import datetime
from playwright.sync_api import sync_playwright


# ──────────────────────────────────────────────
# 1. СКАЧИВАНИЕ CSV ЧЕРЕЗ PLAYWRIGHT
# ──────────────────────────────────────────────

def run_bot() -> list[str]:
    """
    Авторизуется на BBB и скачивает Report.CSV для ВСЕХ встреч сегодняшнего дня.
    Возвращает список путей к скачанным файлам.
    """
    password = os.getenv("BBB_PASSWORD")
    if not password:
        raise ValueError("BBB_PASSWORD не задан!")

    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        # Логин
        print("BOT: Открываю страницу логина...")
        page.goto("https://biggerbluebutton.com/login",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        btn = page.query_selector("button:has-text('Sign In')")
        if btn:
            btn.click()
            page.wait_for_timeout(1500)

        inputs = page.query_selector_all("input")
        visible_text, visible_pass = None, None
        for inp in inputs:
            t = inp.get_attribute("type") or "text"
            if inp.is_visible() and t in ("text", "email") and visible_text is None:
                visible_text = inp
            if inp.is_visible() and t == "password" and visible_pass is None:
                visible_pass = inp

        if not visible_text or not visible_pass:
            page.screenshot(path="debug_login.png", full_page=True)
            browser.close()
            raise RuntimeError("Поля логина не найдены. Смотри debug_login.png")

        visible_text.click()
        page.wait_for_timeout(300)
        page.keyboard.type("260401190051930", delay=50)

        visible_pass.click()
        page.wait_for_timeout(300)
        page.keyboard.type(password, delay=50)

        page.wait_for_timeout(500)
        page.click('button:has-text("SIGN IN")')
        page.wait_for_load_state("networkidle", timeout=20000)
        print(f"BOT: После логина URL: {page.url}")

        if "login" in page.url:
            page.screenshot(path="debug_login.png", full_page=True)
            browser.close()
            raise RuntimeError("Авторизация не прошла. Смотри debug_login.png")

        print("BOT: Авторизован.")

        # Переходим на Meeting History
        page.goto("https://biggerbluebutton.com/rooms/meetings",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        print(f"BOT: Meetings URL: {page.url}")
        page.screenshot(path="debug_meetings.png", full_page=True)

        # Скачиваем Report.CSV для каждой сегодняшней встречи
        today = datetime.now().strftime("%-m/%-d/%Y")  # например "4/3/2026"
        print(f"BOT: Ищу встречи за сегодня ({today})...")

        # Находим все строки таблицы встреч
        rows = page.query_selector_all("tr")
        print(f"BOT: Найдено строк в таблице: {len(rows)}")

        saved_files = []
        for row in rows:
            # Проверяем что строка содержит сегодняшнюю дату
            row_text = row.inner_text()
            if today not in row_text:
                continue

            # Ищем ссылку Report.CSV в этой строке
            report_link = row.query_selector("a:has-text('Report.CSV'), a:has-text('Report.csv')")
            if not report_link:
                continue

            # Получаем название встречи из строки
            name_el = row.query_selector("td:first-child a, td:first-child")
            meeting_name = name_el.inner_text().strip() if name_el else "Unknown"
            print(f"BOT: Скачиваю репорт для '{meeting_name}'...")

            with page.expect_download(timeout=30000) as dl_info:
                report_link.click()

            download = dl_info.value
            filename = f"{meeting_name}.csv"
            # Убираем запрещённые символы из имени файла
            filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
            save_path = os.path.join(download_dir, filename)
            download.save_as(save_path)
            print(f"BOT: Сохранён → {save_path}")
            saved_files.append((meeting_name, save_path))

            page.wait_for_timeout(1000)  # пауза между скачиваниями

        browser.close()

        if not saved_files:
            raise RuntimeError(
                f"Не найдено встреч за сегодня ({today}). "
                "Либо занятий не было, либо смотри debug_meetings.png"
            )

        print(f"BOT: Всего скачано файлов: {len(saved_files)}")
        return saved_files


# ──────────────────────────────────────────────
# 2. ЗАПИСЬ В GOOGLE ТАБЛИЦУ
# ──────────────────────────────────────────────

# Колонки из CSV которые записываем (в нужном порядке)
COLUMNS = ["Date", "Name", "Role", "Duration", "Activity Score",
           "Talk Time", "Webcam Time", "Messages", "Reactions",
           "Poll Votes", "Raise Hands", "Join", "Left"]

def update_sheets(meeting_name: str, file_path: str, gc, sh):
    """Дописывает данные из одного CSV в соответствующий лист таблицы."""

    # Определяем название листа = название встречи
    sheet_name = meeting_name.strip()

    try:
        df = pd.read_csv(file_path)
        print(f"  Файл прочитан: {len(df)} строк")
    except Exception as e:
        print(f"  ОШИБКА чтения файла: {e}")
        return

    # Убираем Anonymous строки
    df = df[df["Name"].notna() & (df["Name"] != "Anonymous")]

    # Добавляем колонку Date (дата сегодня)
    today_str = datetime.now().strftime("%d.%m.%Y")
    df["Date"] = today_str

    # Добавляем колонку Role
    df["Role"] = df["Moderator"].apply(lambda x: "Moderator" if str(x).upper() == "TRUE" else "Student")

    # Переименовываем колонки из CSV
    df = df.rename(columns={"Left": "Left"})  # уже называется Left

    # Берём только нужные колонки (те что есть в файле)
    available = [c for c in COLUMNS if c in df.columns]
    df = df[available]

    # Открываем или создаём лист
    try:
        worksheet = sh.worksheet(sheet_name)
        existing = worksheet.get_all_values()
        is_new = len(existing) == 0
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")
        is_new = True

    if is_new:
        # Новый лист — пишем заголовки
        header = [available]
        worksheet.update("A1", header)
        start_row = 2
    else:
        start_row = len(worksheet.get_all_values()) + 1

    # Добавляем разделитель-дату перед записями
    date_separator = [[f"── {today_str} · {meeting_name} ──"] + [""] * (len(available) - 1)]
    worksheet.append_rows(date_separator, value_input_option="RAW")

    # Записываем данные
    rows_to_write = df.values.tolist()
    worksheet.append_rows(rows_to_write, value_input_option="RAW")

    print(f"  Лист '{sheet_name}': добавлено {len(rows_to_write)} строк (начиная со строки {start_row})")


# ──────────────────────────────────────────────
# 3. ТОЧКА ВХОДА
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== СТАРТ ===")

    # Авторизация Google
    try:
        service_account_info = json.loads(os.getenv("GOOGLE_JSON"))
        gc = gspread.service_account_from_dict(service_account_info)
        print("Google авторизация: ОК")
    except Exception as e:
        print(f"ОШИБКА Google авторизации: {e}")
        exit(1)

    spreadsheet_id = "1VNShaKqmrA7iRFxwFV2mp66pgZteWSzhADCQF58J48w"
    try:
        sh = gc.open_by_key(spreadsheet_id)
        print(f"Таблица '{sh.title}': ОК")
    except Exception as e:
        print(f"ОШИБКА открытия таблицы: {e}")
        exit(1)

    # Скачиваем все сегодняшние репорты
    try:
        files = run_bot()
    except Exception as e:
        print(f"ОШИБКА бота: {e}")
        exit(1)

    # Записываем каждый в свой лист
    for meeting_name, file_path in files:
        print(f"\nОбрабатываю: {meeting_name}")
        update_sheets(meeting_name, file_path, gc, sh)

    print("\n=== ГОТОВО ===")
