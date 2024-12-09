from google.oauth2.service_account import Credentials
import gspread

def adjust_sliding_window_columns(worksheet):
    """
    Устанавливает минимальную читаемую ширину для столбцов в области Sliding Window.
    """
    try:
        # Начальная колонка для статистики (E)
        start_col = 5
        
        # Получаем все заголовки
        headers = worksheet.row_values(5)
        
        # Находим количество пар столбцов (остаток + дата)
        dates = [date for date in headers[start_col+1::2] if date.strip()]
        num_pairs = len(dates)
        
        # Формируем список обновлений для каждой пары столбцов
        column_updates = []
        
        for i in range(num_pairs):
            # Индексы текущей пары столбцов
            stock_col = start_col + (i * 2)
            date_col = stock_col + 1
            
            # Добавляем настройки для столбца с остатко�� (40 пикселей)
            column_updates.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": stock_col - 1,  # API использует 0-based индексы
                        "endIndex": stock_col
                    },
                    "properties": {
                        "pixelSize": 40
                    },
                    "fields": "pixelSize"
                }
            })
            
            # Добавляем настройки для столбца с датой (50 пикселей)
            column_updates.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": date_col - 1,
                        "endIndex": date_col
                    },
                    "properties": {
                        "pixelSize": 65
                    },
                    "fields": "pixelSize"
                }
            })
        
        # Применяем все обновления одним запросо��
        worksheet.spreadsheet.batch_update({
            "requests": column_updates
        })
        
        print(f"Ширина столбцов успешно обновлена для {num_pairs} пар столбцов")
        
    except Exception as e:
        print(f"Произошла ошибка при обновлении ширины столбцов: {str(e)}")

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
        
        print("Начинаем обновление ширины столбцов...")
        adjust_sliding_window_columns(worksheet)
        print("Обновление завершено!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

if __name__ == "__main__":
    main() 