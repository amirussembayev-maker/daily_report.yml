import os
import re
import json
import pandas as pd
import gspread
from datetime import datetime
from playwright.sync_api import sync_playwright


# ──────────────────────────────────────────────
# 1. СКАЧИВАНИЕ ВСЕХ CSV ЧЕРЕЗ PLAYWRIGHT
# ──────────────────────────────────────────────

def run_bot() -> list:
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

        # ── Логин ────────────────────────────────────────────────────────
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
            raise RuntimeError("Поля логина не найдены.")

        visible_text.click()
        page.wait_for_timeout(300)
        page.keyboard.type("260401190051930", delay=50)

        visible_pass.click()
        page.wait_for_timeout(300)
        page.keyboard.type(password, delay=50)
        page.wait_for_timeout(500)

        print("BOT: Нажимаю SIGN IN...")
        try:
            with page.expect_navigation(timeout=15000):
                page.click('button:has-text("SIGN IN")')
        except Exception:
            page.wait_for_timeout(5000)

        print(f"BOT: URL после входа: {page.url}")
        page.screenshot(path="debug_login.png", full_page=True)

        if "login" in page.url:
            browser.close()
            raise RuntimeError("Авторизация не прошла. Смотри debug_login.png")

        print("BOT: Авторизован!")

        # ── Переход на Meeting History ────────────────────────────────────
        page.goto("https://biggerbluebutton.com/rooms/meetings",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        print(f"BOT: Meetings URL: {page.url}")
        page.screenshot(path="debug_meetings.png", full_page=True)

        # ── Скачиваем ВСЕ репорты (без фильтра по дате) ──────────────────
        print("BOT: Скачиваю ВСЕ репорты...")

        rows = page.query_selector_all("tr")
        print(f"BOT: Строк в таблице: {len(rows)}")

        saved_files = []
        for i, row in enumerate(rows):
            report_link = row.query_selector("a:has-text('Report.CSV'), a:has-text('Report.csv')")
            if not report_link:
                continue

            # Название встречи
            name_el = row.query_selector("td:first-child a, td:first-child")
            meeting_name = name_el.inner_text().strip() if name_el else f"Meeting_{i}"

            # Дата встречи из строки (для записи в таблицу)
            cells = row.query_selector_all("td")
            meeting_date = cells[1].inner_text().strip() if len(cells) > 1 else ""

            print(f"BOT: Скачиваю '{meeting_name}' ({meeting_date})...")

            try:
                with page.expect_download(timeout=30000) as dl_info:
                    report_link.click()

                download = dl_info.value
                filename = re.sub(r'[\\/*?:"<>|]', "_", f"{meeting_name}_{meeting_date}") + ".csv"
                save_path = os.path.join(download_dir, filename)
                download.save_as(save_path)
                print(f"BOT: Сохранён → {save_path}")
                saved_files.append((meeting_name, meeting_date, save_path))
                page.wait_for_timeout(800)
            except Exception as e:
                print(f"BOT: Ошибка при скачивании '{meeting_name}': {e}")
                continue

        browser.close()

        if not saved_files:
            raise RuntimeError("Ни одного репорта не скачано. Смотри debug_meetings.png")

        print(f"BOT: Всего скачано: {len(saved_files)} файлов")
        return saved_files


# ──────────────────────────────────────────────
# 2. ЗАПИСЬ В GOOGLE ТАБЛИЦУ
# ──────────────────────────────────────────────

COLUMNS = ["Date", "Name", "Role", "Duration", "Activity Score",
           "Talk Time", "Webcam Time", "Messages", "Reactions",
           "Poll Votes", "Raise Hands", "Join", "Left"]


def update_sheets(meeting_name: str, meeting_date: str, file_path: str, gc, sh):
    sheet_name = meeting_name.strip()

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"  ОШИБКА чтения: {e}")
        return

    # Убираем Anonymous
    df = df[df["Name"].notna() & (df["Name"].astype(str) != "Anonymous")]
    if df.empty:
        print(f"  Пропускаю '{sheet_name}' — нет данных.")
        return

    # Добавляем служебные колонки
    df["Date"] = meeting_date
    df["Role"] = df["Moderator"].apply(
        lambda x: "Moderator" if str(x).upper() == "TRUE" else "Student"
    )

    available = [c for c in COLUMNS if c in df.columns]
    df = df[available]

    # Открываем или создаём лист
    try:
        worksheet = sh.worksheet(sheet_name)
        is_new = len(worksheet.get_all_values()) == 0
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")
        is_new = True

    if is_new:
        worksheet.update("A1", [available])

    # Разделитель
    separator = [[f"── {meeting_date} ──"] + [""] * (len(available) - 1)]
    worksheet.append_rows(separator, value_input_option="RAW")

    # Данные
    rows_data = df.values.tolist()
    worksheet.append_rows(rows_data, value_input_option="RAW")
    print(f"  Лист '{sheet_name}': записано {len(rows_data)} строк.")


# ──────────────────────────────────────────────
# 3. ТОЧКА ВХОДА
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== СТАРТ ===")

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

    try:
        files = run_bot()
    except Exception as e:
        print(f"ОШИБКА бота: {e}")
        exit(1)

    for meeting_name, meeting_date, file_path in files:
        print(f"\nОбрабатываю: {meeting_name} ({meeting_date})")
        update_sheets(meeting_name, meeting_date, file_path, gc, sh)

    print("\n=== ГОТОВО ===")
