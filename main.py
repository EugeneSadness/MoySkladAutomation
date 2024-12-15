from datetime import datetime

import gspread

from auth.google_auth import authenticate_google_sheets
from auth.moysklad_auth import get_access_token
from services.google_sheets_handler import get_products_with_details, update_sheet3, get_products_with_details_sheet2
from sheet_processor import (
    process_sheet1, process_sheet2, process_sheet3, process_sheet5
)
import config

def main():
    try:
        client = authenticate_google_sheets(config.CREDENTIALS_PATH)
        spreadsheet = client.open(config.SHEET_NAME)

        # Get MoySklad token
        token = get_access_token(config.MS_USERNAME, config.MS_PASSWORD)
        print("Access Token:", token)

        # Process Sheet1
        process_sheet1(spreadsheet, token)

        # Process Sheet2
        process_sheet2(client, spreadsheet, token)

        # Обработка данных для Листа3
        try:
            worksheet3 = spreadsheet.worksheet("Лист3")
        except gspread.WorksheetNotFound:
            worksheet3 = spreadsheet.add_worksheet(title="Лист3", rows="1000", cols="3")
            print("Лист3 создан.")

        # Объединяем продукты из Sheet1 и Sheet2
        combined_products = {**get_products_with_details(spreadsheet.sheet1), **get_products_with_details_sheet2(spreadsheet.worksheet("Лист2"))}

        # Обновляем Лист3
        update_sheet3(worksheet3, combined_products)
        print("Данные успешно записаны в Лист3.")

        # Process Sheet3
        process_sheet3(spreadsheet, token)

        # Process Sheet5
        try:
            worksheet5 = spreadsheet.worksheet("Лист5")
        except gspread.WorksheetNotFound:
            worksheet5 = spreadsheet.add_worksheet(title="Лист5", rows="1000", cols="20")
            print("Лист5 создан.")

        process_sheet5(worksheet5, token)

        return 0
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

if __name__ == "__main__":
    main() 