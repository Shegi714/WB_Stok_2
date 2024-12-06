import requests
from google.oauth2.service_account import Credentials
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

api_token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMTE4djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc0ODk5MzcyNiwiaWQiOiIwMTkzOGM0ZC1jMTVhLTczMTAtYmZjYS03NDM3NGI2YzY3NzEiLCJpaWQiOjI3ODc5NDgwLCJvaWQiOjUwMTM4LCJzIjoxMDczNzQ1MDIyLCJzaWQiOiJlYWMyYjNjNy1iZTA5LTVkYzYtOWE2MS01NTRlYWU3ZDgyNTEiLCJ0IjpmYWxzZSwidWlkIjoyNzg3OTQ4MH0.d8xSaX6q4raxU5Dk_1xQ1S6CErxhiSv23gSci2r007KJNR3nK5b-zf-Ey0NT79KV3iPuT1UPZPp-dThZCKvtsg"

# Функция для декодирования строк
def decode_unicode_strings(data):
    if isinstance(data, str):
        return bytes(data, "utf-8").decode("unicode_escape")
    elif isinstance(data, dict):
        return {key: decode_unicode_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [decode_unicode_strings(item) for item in data]
    else:
        return data  # Если это не строка, возвращаем как есть



# Настройки для доступа к Google Sheets
def setup_google_sheets(sheet_name):
    credentials_file = "credentials/gst.json"  # Убедитесь, что путь к файлу указан правильно
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(credentials_file, scopes=scope)

    # Авторизация в Google Sheets
    gc = gspread.authorize(credentials)
    return gc.open(sheet_name)

# Функция для выполнения первого запроса и получения taskId
def get_task_id():
    url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"  # Исправлено: добавлен Bearer
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Выводим полный ответ для отладки
        print("Ответ первого запроса:", data)

        report_id = data.get("data", {}).get("taskId")
        if report_id:
            print(f"Создан отчет с ID: {report_id}")
            return report_id
        else:
            print("Не удалось получить ID отчета.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при создании отчета: {e}")
        return None


# Функция для выполнения второго запроса и получения данных
def get_task_data(task_id):
    if not task_id:
        raise ValueError("Task ID отсутствует. Невозможно выполнить запрос данных.")

    url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
    headers = {
        "Authorization": f"Bearer {api_token}"  # Исправлено: добавлен Bearer
    }

    max_retries = 5  # Максимальное количество попыток
    retry_delay = 30  # Задержка между попытками (в секундах)

    for attempt in range(max_retries):
        try:
            time.sleep(15)
            response = requests.get(url, headers=headers)

            # Если код ответа 404 или 429, ждём и повторяем запрос
            if response.status_code == 404 or response.status_code == 429:
                print(
                    f"Попытка {attempt + 1}: Получен код {response.status_code}. Ждем {retry_delay} секунд перед повторной попыткой...")
                time.sleep(retry_delay)
                continue

            # Если статус-код успешный (например, 200), обрабатываем данные
            response.raise_for_status()

            # Проверяем, является ли ответ JSON
            try:
                data = response.json()
                print("Данные успешно получены.")
                return data
            except ValueError:
                print("Ответ не является JSON. Возможно, это файл.")
                raise

        except requests.exceptions.RequestException as e:
            print(f"Попытка {attempt + 1}: Ошибка при выполнении запроса: {e}")
            if attempt < max_retries - 1:  # Если это не последняя попытка
                print(f"Ждем {retry_delay} секунд перед повторной попыткой...")
                time.sleep(retry_delay)
            else:
                print("Превышено максимальное количество попыток. Завершаем выполнение.")
                raise

    # Если после всех попыток данные не были получены, выбрасываем исключение
    raise RuntimeError("Не удалось получить данные после нескольких попыток.")


# Функция для записи данных в Google Sheets
def upload_data_to_google_sheets(data, service_account_file, spreadsheet_name):
    """
    Загружает данные из переменной data в Google Sheets.

    :param data: Данные в формате JSON (список словарей)
    :param service_account_file: Путь к JSON-файлу с ключами для Google API
    :param spreadsheet_name: Название Google Sheets таблицы
    """
    # Устанавливаем соединение с Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, scope)
    client = gspread.authorize(credentials)

    # Открываем таблицу
    spreadsheet = client.open(spreadsheet_name)

    # Выбираем первый лист в таблице
    sheet = spreadsheet.sheet1

    # Очищаем таблицу
    sheet.clear()

    # Если данные — это список словарей, записываем их в таблицу
    if isinstance(data, list):
        # Добавляем заголовки (ключи словаря)
        headers = list(data[0].keys())
        sheet.append_row(headers)  # Добавляем заголовки в таблицу

        # Преобразуем данные в список списков (только значения)
        rows_to_append = []
        for row in data:
            processed_row = []
            for key in headers:
                if key == "warehouses":
                    # Преобразуем вложенные данные о складах в строку
                    warehouses_data = row[key]
                    warehouses_str = "; ".join(
                        [f"{w['warehouseName']} ({w['quantity']})" for w in warehouses_data]
                    )
                    processed_row.append(warehouses_str)
                else:
                    processed_row.append(row[key])
            rows_to_append.append(processed_row)

        # Добавляем все строки за один запрос
        sheet.append_rows(rows_to_append, value_input_option="RAW")
    else:
        print("Формат данных не поддерживается")

# Пример использования функции
if __name__ == "__main__":
    # Путь к JSON-файлу с ключами для доступа к Google API
    SERVICE_ACCOUNT_FILE = 'credentials/gst.json'

    # Название таблицы в Google Sheets
    SPREADSHEET_NAME = 'Wildberries Data'

    # Пример данных (замените на свои данные)
   # with open(data = response.json() , encoding='utf-8') as f:
    data = json.load(da)  # Загружаем JSON как Python-объект (список или словарь)

    # Вызов функции
    upload_data_to_google_sheets(data, SERVICE_ACCOUNT_FILE, SPREADSHEET_NAME)


# Основная функция
def main():
    try:
        sheet_name = "Wildberries Data"  # Укажите название вашей таблицы

        # Получаем Task ID
        task_id = get_task_id()
        if not task_id:
            print("Не удалось получить Task ID. Завершение работы.")
            return

        # Получаем данные по Task ID
        data = get_task_data(task_id)

        # Декодируем данные
        decoded_data = decode_unicode_strings(data)

        # Подключаемся к Google Sheets
        sheet = setup_google_sheets(sheet_name)

        # Записываем данные в таблицу
        write_to_google_sheets(sheet, decoded_data)

    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()
