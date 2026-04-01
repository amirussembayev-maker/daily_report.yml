import os
import json
import pandas as pd
import gspread
from playwright.sync_api import sync_playwright

def run_bot() -> str:
    password = os.getenv("BBB_PASSWORD")
    if not password:
        raise ValueError("BBB_PASSWORD не задан!")

    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("BOT: Открываю страницу логина...")
        page.goto("https://biggerbluebutton.com/rooms/sessions/sign_in",
                  wait_until="networkidle", timeout=30000)

        page.fill('input[name="session[email]"]', "260401190051930")
        page.fill('input[name="session[password]"]', password)
        page.click('input[type="submit"], button[type="submit"]')
        page.wait_for_load_state("networkidle", timeout=20000)
        print("BOT: Авторизация выполнена.")

        page.goto("https://biggerbluebutton.com/rooms/meetings",
                  wait_until="networkidle", timeout=30000)
        print("BOT: Страница meetings загружена.")

        dashboard_selectors = [
            "a[href*='learning_dashboard']",
            "button:has-text('Learning Dashboard')",
            "a:has-text('Learning Dashboard')",
            "[data-action*='download']",
            "a[href*='csv']",
        ]

        download_link = None
        for sel in dashboard_selectors:
            elements = page.query_selector_all(sel)
            if elements:
                download_link = elements[-1]
                print(f"BOT: Найдена кнопка: {sel}")
                break

        if download_link is None:
            page.screenshot(path="debug_screenshot.png", full_page=True)
            browser.close()
            raise RuntimeError("Кнопка не найдена. Смотри debug_screenshot.png")

        print("BOT: Скачиваю...")
        with page.expect_download(timeout=30000) as dl_info:
            download_link.click()

        download = dl_info.value
        save_path = os.path.join(download_dir, download.suggested_filename or "report.csv")
        download.save_as(save_path)
        print(f"BOT: Файл сохранён → {save_path}")
        browser.close()
        return save_path


def update_sheets(file_path: str):
    print("--- СТАРТ РАБОТЫ ---")

    try:
        service_account_info = json.loads(os.getenv("GOOGLE_JSON"))
        gc = gspread.service_account_from_dict(service_account_info)
        print("1. Авторизация: ОК")
    except Exception as e:
        print(f"ОШИБКА АВТОРИЗАЦИИ: {e}")
        return

    spreadsheet_id = "1VNShaKqmrA7iRFxwFV2mp66pgZteWSzhADCQF58J48w"
    try:
        sh = gc.open_by_key(spreadsheet_id)
        print(f"2. Таблица '{sh.title}' открыта: ОК")
    except Exception as e:
        print(f"ОШИБКА ОТКРЫТИЯ ТАБЛИЦЫ: {e}")
        return

    try:
        df = pd.read_csv(file_path)
        print(f"3. Файл прочитан: ОК ({len(df)} строк)")
    except Exception as e:
        print(f"ОШИБКА ЧТЕНИЯ ФАЙЛА: {e}")
        return

    group_name = "RTI 1.2"
    try:
        try:
            worksheet = sh.worksheet(group_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=group_name, rows="100", cols="20")

        students_only = df[df["Moderator"] == False][["Name", "Duration"]]
        data_to_save = [students_only.columns.tolist()] + students_only.values.tolist()
        worksheet.clear()
        worksheet.update("A1", data_to_save)
        print(f"4. Данные в лист '{group_name}' выгружены: УСПЕХ!")
    except Exception as e:
        print(f"ОШИБКА ВЫГРУЗКИ: {e}")


if __name__ == "__main__":
    csv_path = run_bot()
    update_sheets(csv_path)
