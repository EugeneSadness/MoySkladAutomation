from datetime import datetime, timedelta

def get_current_day_date_range() -> tuple:
    today = datetime.today()
    start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_of_day.strftime("%Y-%m-%d %H:%M:%S"), end_of_day.strftime("%Y-%m-%d %H:%M:%S") 