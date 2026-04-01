import os
import pandas as pd
import gspread
import json

def update_sheets(file_path):
    print(f"--- Запуск обновления таблицы ---")
    
    # 1. Проверка наличия файла
    if not os.path.exists(file_path):
        print(f"ОШИБКА: Файл {file_path} не найден в репозитории!")
        print(f"Список файлов в папке: {os.listdir('.')}")
        return

    # 2. Авторизация в Google
    try:
        service_account_info = json.loads(os.getenv("GOOGLE_JSON"))
        gc = gspread.service_account_from_dict(service_account_info)
        print("Авторизация в Google: УСПЕШНО")
    except Exception as e:
        print(f"ОШИБКА АВТОРИЗАЦИИ: {e}")
        return

    # 3. Открытие таблицы (ID из твоего сообщения)
    spreadsheet_id = "1VNShaKqmrA7iRFxwFV2mp66pgZteWSzhADCQF58J48w"
    try:
        sh = gc.open_by_key(spreadsheet_id)
        print(f"Таблица '{sh.title}' открыта: УСПЕШНО")
    except Exception as e:
        print(f"ОШИБКА ПРИ ОТКРЫТИИ ТАБЛИЦЫ: {e}")
        print("Проверь: 1. Правильный ли ID. 2. Добавлен ли email бота в 'Поделиться'.")
        return

    # 4. Чтение данных
    df = pd.read_csv(file_path)
    group_name = "RTI 1.2" 
    
    # Ищем или создаем лист
    try:
        worksheet = sh.worksheet(group_name)
    except:
        worksheet = sh.add_worksheet(title=group_name, rows="100", cols="20")
        print(f"Создан новый лист: {group_name}")

    # Фильтруем данные (убираем модератора и берем Имя + Длительность)
    students_data = df[df['Moderator'] == False][['Name', 'Duration']]
    
    # Подготовка данных для вставки
    header = [students_data.columns.values.tolist()]
    values = students_data.values.tolist()
    all_data = header + values

    # Очистка и обновление листа
    worksheet.clear()
    worksheet.update('A1', all_data)
    print(f"Данные успешно выгружены в лист {group_name}!")

if __name__ == "__main__":
    # Пока закомментируем бота для скачивания, чтобы проверить только таблицу
    # run_bot() 
    update_sheets("Learning Dashboard RTI 1.2 Apr 2026.csv")
