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

        # Открываем главную страницу
        print("BOT: Открываю главную страницу...")
        page.goto("https://biggerbluebutton.com/",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        print(f"BOT: URL = {page.url}")
        print(f"BOT: Title = {page.title()}")
        page.screenshot(path="debug_home.png", full_page=True)

        # Нажимаем кнопку LOGIN в правом верхнем углу
        print("BOT: Ищу кнопку LOGIN...")
        login_btn_selectors = [
            "a:has-text('LOGIN')",
            "a:has-text('Login')",
            "a[href*='login']",
            "a[href*='sign_in']",
            "a[href*='signin']",
            ".login",
            "#login",
        ]
        clicked = False
        for sel in login_btn_selectors:
            el = page.query_selector(sel)
            if el and el.is_visible():
                print(f"BOT: Найдена кнопка логина: {sel}")
                el.click()
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                page.wait_for_timeout(3000)
                clicked = True
                break

        if not clicked:
            page.screenshot(path="debug_login_page.png", full_page=True)
            browser.close()
            raise RuntimeError("Кнопка LOGIN не найдена! Смотри debug_home.png")

        print(f"BOT: После клика LOGIN — URL = {page.url}")
        page.screenshot(path="debug_login_page.png", full_page=True)
        with open("debug_login_page.html", "w", encoding="utf-8") as f:
            f.write(page.content())

        # Выводим все input для диагностики
        inputs = page.query_selector_all("input")
        print(f"BOT: Найдено input-полей: {len(inputs)}")
        for i, inp in enumerate(inputs):
            print(f"  input[{i}]: type={inp.get_attribute('type')}, "
                  f"name={inp.get_attribute('name')}, "
                  f"placeholder={inp.get_attribute('placeholder')}, "
                  f"id={inp.get_attribute('id')}")

        # Заполняем первое видимое текстовое поле (логин)
        filled = False
        for inp in inputs:
            t = inp.get_attribute("type") or "text"
            if t in ("text", "email") and inp.is_visible():
                inp.fill("260401190051930")
                print(f"BOT: Логин введён (type={t})")
                filled = True
                break

        if not filled:
            raise RuntimeError("Поле логина не найдено!")

        # Заполняем поле пароля
        for inp in inputs:
            if inp.get_attribute("type") == "password" and inp.is_visible():
                inp.fill(password)
                print("BOT: Пароль введён.")
                break

        # Нажимаем SIGN IN
        page.click('button:has-text("SIGN IN")')
        page.wait_for_load_state("networkidle", timeout=20000)
        print(f"BOT: После логина URL: {page.url}")
        print("BOT: Авторизация выполнена.")

        page.goto("https://biggerbluebutton.com/rooms/meetings",
                  wait_until="networkidle", timeout=30000)
        page.screenshot(path="debug_meetings.png", full_page=True)
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
