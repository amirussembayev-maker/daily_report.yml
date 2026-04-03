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

        print("BOT: Открываю страницу логина...")
        page.goto("https://biggerbluebutton.com/login",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        print(f"BOT: URL = {page.url}")
        page.screenshot(path="debug_login_page.png", full_page=True)

        # Кликаем на вкладку Sign In (на странице есть Sign In / Sign Up)
        signin_tab_selectors = [
            "button:has-text('Sign In')",
            "a:has-text('Sign In')",
            "li:has-text('Sign In')",
            "[role='tab']:has-text('Sign In')",
            "button:has-text('LOGIN')",
            "button:has-text('Log In')",
        ]
        for sel in signin_tab_selectors:
            el = page.query_selector(sel)
            if el and el.is_visible():
                print(f"BOT: Кликаю на вкладку: {sel}")
                el.click()
                page.wait_for_timeout(1000)
                break

        page.screenshot(path="debug_signin_tab.png", full_page=True)

        # Ждём пока поле #email станет видимым
        print("BOT: Жду поле email...")
        page.wait_for_selector("#email:visible", timeout=10000)

        page.fill('#email', "260401190051930")
        print("BOT: Логин введён.")

        page.fill('#password', password)
        print("BOT: Пароль введён.")

        page.screenshot(path="debug_before_submit.png")

        # Нажимаем кнопку SIGN IN
        page.click('button:has-text("SIGN IN")')
        page.wait_for_load_state("networkidle", timeout=20000)

        print(f"BOT: После логина URL: {page.url}")
        page.screenshot(path="debug_after_login.png", full_page=True)

        if "login" in page.url:
            with open("debug_after_login.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError("Авторизация не прошла. Смотри debug_after_login.png")

        print("BOT: Авторизация выполнена.")

        page.goto("https://biggerbluebutton.com/rooms/meetings",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        page.screenshot(path="debug_meetings.png", full_page=True)
        with open("debug_meetings.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"BOT: Meetings URL: {page.url}")

        # Выводим ссылки для диагностики
        links = page.query_selector_all("a, button")
        print(f"BOT: Найдено элементов: {len(links)}")
        for i, el in enumerate(links[:30]):
            txt = el.inner_text().strip()[:50]
            href = el.get_attribute("href") or ""
            print(f"  [{i}] text='{txt}', href='{href}'")

        dashboard_selectors = [
            "a[href*='learning_dashboard']",
            "button:has-text('Learning Dashboard')",
            "a:has-text('Learning Dashboard')",
            "button:has-text('Dashboard')",
            "a:has-text('Dashboard')",
            "[data-action*='download']",
            "a[href*='csv']",
            "button:has-text('Download')",
            "a:has-text('Download')",
        ]

        download_link = None
        for sel in dashboard_selectors:
            elements = page.query_selector_all(sel)
            if elements:
                download_link = elements[-1]
                print(f"BOT: Найдена кнопка: {sel}")
                break

        if download_link is None:
            browser.close()
            raise RuntimeError("Кнопка не найдена. Смотри debug_meetings.png и debug_meetings.html")

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
