from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime, timedelta

def fill_dates_in_worksheet(worksheet):
    """
    Заполняет даты в формате дд.мм.гггг начиная с колонки E2 вправо на 90 дней.
    """
    try:
        # Начальная колонка для заполнения дат (E)
        start_col = 5
        start_row = 2
        
        # Текущая дата
        current_date = datetime.now()
        
        # Формируем список дат
        dates = [(current_date + timedelta(days=i)).strftime("%d.%m.%Y") for i in range(90)]
        
        # Заполняем даты в строке
        cell_list = worksheet.range(start_row, start_col, start_row, start_col + len(dates) - 1)
        for i, cell in enumerate(cell_list):
            cell.value = dates[i]
        
        # Обновляем ячейки на листе
        worksheet.update_cells(cell_list)
        
        print("Даты успешно заполнены на 90 дней.")
        
    except Exception as e:
        print(f"Произошла ошибка при заполнении дат: {str(e)}")

def main():
    # Настройки
    CREDENTIALS_PATH = "./cred.json"
    SHEET_NAME = "test"
    WORKSHEET_NAME = "Лист3"
    
    try:
        # Аутентификация
        credentials = Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=['https://spreadsheets.google.com/feeds',
                   'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(credentials)
        
        # Открываем таблицу и лист
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        
        print("Начинаем заполнение дат...")
        fill_dates_in_worksheet(worksheet)
        print("Заполнение завершено!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

if __name__ == "__main__":
    main() 