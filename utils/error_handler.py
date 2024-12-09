def print_api_errors(response):
    try:
        error_data = response.json()
        errors = error_data.get("errors", [])
        if errors:
            for error in errors:
                error_message = error.get("error", "Неизвестная ошибка")
                code = error.get("code", "Нет кода ошибки")
                more_info = error.get("moreInfo", "Нет дополнительной информации")
                print(f"Ошибка: {error_message}")
                print(f"Код: {code}")
                print(f"Подробнее: {more_info}\n")
        else:
            print(f"HTTP Ошибка: {response.status_code} - {response.text}")
    except ValueError:
        print(f"HTTP Ошибка: {response.status_code} - {response.text}") 