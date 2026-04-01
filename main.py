import os
import pandas as pd
import gspread
from playwright.sync_api import sync_playwright
import json

# Настройки из ваших данных
URL = "https://biggerbluebutton.com/rooms/meetings"
LOGIN = "260401190051930"
PASSWORD = os.getenv("BBB_PASSWORD") # Берем из секретов GitHub

def run_bot():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context().new_page()
        
        # 1. Авторизация
        page.goto(URL)
        page.fill('input[name="user[login]"]', LOGIN) # Селекторы могут потребовать уточнения
        page.fill('input[name="user[password]"]', PASSWORD)
        page.click('button[type="submit"]')
        
        # 2. Логика скачивания (здесь бот ищет кнопки загрузки CSV)
        # Примечание: тут будет код клика по кнопке "Download CSV"
        # Для теста используем ваш загруженный файл
        
        browser.close()

def update_sheets(file_path):
    # Авторизация в Google через секрет
    service_account_info = json.loads(os.getenv("GOOGLE_JSON"))
    gc = gspread.service_account_from_dict(service_account_info)
    sh = gc.open_by_key("ID_ВАШЕЙ_ТАБЛИЦЫ")

    df = pd.read_csv(file_path)
    
    # Логика: из названия "Learning Dashboard RTI 1.2 Apr 2026.csv" 
    # вытаскиваем группу "RTI 1.2"
    group_name = "RTI 1.2" 
    
    # Ищем или создаем лист для группы
    try:
        worksheet = sh.worksheet(group_name)
    except:
        worksheet = sh.add_worksheet(title=group_name, rows="100", cols="20")

    # Преподаватель (Moderator == TRUE)
    teacher = df[df['Moderator'] == True]['Name'].iloc[0]
    
    # Выгружаем данные студентов
    students_data = df[df['Moderator'] == False][['Name', 'Duration']]
    worksheet.update([students_data.columns.values.tolist()] + students_data.values.tolist())

if __name__ == "__main__":
    # Сначала скачиваем, потом обновляем
    # run_bot() 
    update_sheets("Learning Dashboard RTI 1.2 Apr 2026.csv")
