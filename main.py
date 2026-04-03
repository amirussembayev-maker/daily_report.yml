import os
import json
import requests
import pandas as pd
import gspread
from playwright.sync_api import sync_playwright


def run_bot() -> str:
    password = os.getenv("BBB_PASSWORD")
    if not password:
        raise ValueError("BBB_PASSWORD не задан!")

    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)

    # ── Шаг 1: Авторизация через HTTP (обходим anti-bot) ──────────────
    print("BOT: Авторизуюсь через HTTP...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://biggerbluebutton.com",
        "Referer": "https://biggerbluebutton.com/login",
    })

    # Пробуем несколько вариантов API логина
    login_endpoints = [
        ("POST", "https://biggerbluebutton.com/api/v1/auth/login"),
        ("POST", "https://biggerbluebutton.com/api/auth/login"),
        ("POST", "https://biggerbluebutton.com/api/login"),
        ("POST", "https://biggerbluebutton.com/auth/login"),
    ]

    login_data = {
        "email": "260401190051930",
        "password": password,
    }

    auth_cookies = None
    for method, url in login_endpoints:
        try:
            resp = session.post(url, json=login_data, timeout=10)
            print(f"BOT: {url} → {resp.status_code}")
            if resp.status_code in (200, 201):
                print(f"BOT: Ответ: {resp.text[:200]}")
                auth_cookies = session.cookies.get_dict()
                print(f"BOT: Куки: {list(auth_cookies.keys())}")
                break
        except Exception as e:
            print(f"BOT: {url} → ошибка: {e}")

    # ── Шаг 2: Открываем браузер с куками ─────────────────────────────
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

        # Добавляем куки от HTTP-сессии в браузер
        if auth_cookies:
            print(f"BOT: Добавляю {len(auth_cookies)} куки в браузер...")
            for name, value in auth_cookies.items():
                context.add_cookies([{
                    "name": name,
                    "value": value,
                    "domain": "biggerbluebutton.com",
                    "path": "/",
                }])

        page = context.new_page()

        # ── Шаг 3: Пробуем войти через браузер с медленным вводом ──────
        print("BOT: Открываю страницу логина...")
        page.goto("https://biggerbluebutton.com/login",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        # Кликаем Sign In
        btn = page.query_selector("button:has-text('Sign In')")
        if btn:
            btn.click()
            page.wait_for_timeout(1500)

        # Вводим логин медленно (имитируем человека)
        inputs = page.query_selector_all("input")
        visible_text = None
        visible_pass = None
        for inp in inputs:
            t = inp.get_attribute("type") or "text"
            if inp.is_visible() and t in ("text", "email") and visible_text is None:
                visible_text = inp
            if inp.is_visible() and t == "password" and visible_pass is None:
                visible_pass = inp

        if visible_text and visible_pass:
            visible_text.click()
            page.wait_for_timeout(300)
            # Печатаем посимвольно как человек
            page.keyboard.type("260401190051930", delay=50)
            print("BOT: Логин введён.")

            visible_pass.click()
            page.wait_for_timeout(300)
            page.keyboard.type(password, delay=50)
            print("BOT: Пароль введён.")

            page.wait_for_timeout(500)
            page.click('button:has-text("SIGN IN")')
            page.wait_for_load_state("networkidle", timeout=20000)
            print(f"BOT: После логина URL: {page.url}")

        page.screenshot(path="debug_after_login.png", full_page=True)

        # ── Шаг 4: Переходим на Meeting History ─────────────────────────
        # Пробуем разные URL
        for meetings_url in [
            "https://biggerbluebutton.com/rooms/meetings",
            "https://biggerbluebutton.com/meeting-history",
            "https://biggerbluebutton.com/meetings",
        ]:
            page.goto(meetings_url, wait_until="networkidle", timeout=20000)
            page.wait_for_timeout(2000)
            current = page.url
            print(f"BOT: {meetings_url} → {current}")
            if "login" not in current and "403" not in page.title():
                print("BOT: Страница meetings найдена!")
                break

        page.screenshot(path="debug_meetings.png", full_page=True)
        with open("debug_meetings.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"BOT: Итоговый URL meetings: {page.url}")

        # Выводим ссылки
        links = page.query_selector_all("a, button")
        print(f"BOT: Элементов на странице: {len(links)}")
        for i, el in enumerate(links[:40]):
            txt = el.inner_text().strip()[:60]
            href = el.get_attribute("href") or ""
            print(f"  [{i}] '{txt}' href='{href}'")

        # Ищем Report.CSV (видно на скриншоте!)
        dashboard_selectors = [
            "a:has-text('Report.CSV')",
            "a:has-text('Report.csv')",
            "a:has-text('report.csv')",
            "a[href*='report']",
            "a[href*='csv']",
            "a:has-text('Learning Dashboard')",
            "button:has-text('Download')",
            "a:has-text('Download')",
        ]

        download_link = None
        for sel in dashboard_selectors:
            elements = page.query_selector_all(sel)
            if elements:
                download_link = elements[0]  # первая/последняя встреча
                print(f"BOT: Найдена кнопка: {sel} ({len(elements)} шт.)")
                break

        if download_link is None:
            browser.close()
            raise RuntimeError("Кнопка Report.CSV не найдена. Смотри debug_meetings.png")

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
