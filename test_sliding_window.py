from datetime import datetime, timedelta
import random
from google.oauth2.service_account import Credentials
import gspread
import string

def get_column_letter(column_number):
    """Преобразует номер столбца в буквенное обозначение"""
    result = ""
    while column_number > 0:
        column_number -= 1
        result = string.ascii_uppercase[column_number % 26] + result
        column_number //= 26
    return result

def generate_test_data(worksheet, days=90):
    """
    Генерирует тестовые данные для проверки механизма sliding window.
    """
    try:
        # Начинаем с даты 90 дней назад
        start_date = datetime.now() - timedelta(days=days)
        
        # Получаем список артикулов
        articles = worksheet.col_values(1)[5:]  # Пропускаем первые 5 строк
        if not articles:
            print("Внимание: В таблице нет артикулов!")
            return
            
        # Начальная колонка для статистики (E = 5)
        start_col = 5
        
        # Вычисляем конечную колонку
        end_col = start_col + (days * 2) - 1
        end_row = 5 + len(articles)
        
        # Получаем буквенное обозначение последней колонки
        end_col_letter = get_column_letter(end_col)
        
        # Формируем данные
        data = []
        
        # Заголовки
        header_row = []
        for day in range(days):
            current_date = (start_date + timedelta(days=day)).strftime("%d.%m.%y")
            header_row.extend(['Ост', current_date])
        data.append(header_row)
        
        # Данные для каждого артикула
        for _ in articles:
            row_data = []
            for _ in range(days):
                stock = random.randint(0, 100)
                orders = random.randint(0, 10)
                row_data.extend([stock, orders])
            data.append(row_data)
        
        print(f"Обновляем диапазон E5:{end_col_letter}{end_row}")
        
        # Обновляем данные
        worksheet.update(values=data, range_name=f'E5:{end_col_letter}{end_row}')
        
        print(f"Обновлено {len(data)} строк и {len(data[0])} столбцов")
        
    except Exception as e:
        print(f"Ошибка при генерации данных: {str(e)}")
        raise

def main():
    # Настройки
    CREDENTIALS_PATH = "./cred.json"
    SHEET_NAME = "test"
    
    try:
        # Аутентификация
        credentials = Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=['https://spreadsheets.google.com/feeds',
                   'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(credentials)
        
        # Открываем таблицу
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.sheet1
        
        print("Начинаем генерацию тестовых данных...")
        generate_test_data(worksheet)
        print("Тестовые данные успешно сгенерированы!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main() 