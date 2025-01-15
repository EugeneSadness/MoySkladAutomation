from datetime import datetime, timedelta

def get_current_day_date_range() -> tuple:
    today = datetime.today()
    start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = (start_of_day - timedelta(days=91)).replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_of_day.strftime("%Y-%m-%d %H:%M:%S"), end_of_day.strftime("%Y-%m-%d %H:%M:%S") 

def convert_date_formats(date_str: str, from_format: str, to_format: str) -> str:
    """
    Конвертирует дату из одного формата в другой.
    
    Args:
        date_str (str): Строка с датой
        from_format (str): Исходный формат (например, '%d.%m.%Y')
        to_format (str): Целевой формат (например, '%Y-%m-%d')
    
    Returns:
        str: Дата в новом формате
    """
    try:
        date_obj = datetime.strptime(date_str, from_format)
        return date_obj.strftime(to_format)
    except ValueError as e:
        print(f"Ошибка преобразования даты {date_str}: {e}")
        return None 