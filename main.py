import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time


def get_task_id(api_token):
    """
    Отправляет запрос к API Wildberries и возвращает значение task_id из ответа.

    :param api_token: str - Токен для авторизации в API.
    :return: str - Значение task_id или None, если task_id не найден.
    """
    url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"

    params = {
        "locale": "ru",
        "groupByBrand": "false",
        "groupBySubject": "false",
        "groupBySa": "true",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
        "filterPics": "0",
        "filterVolume": "0"
    }

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        task_id = data.get("data", {}).get("taskId")

        if task_id:
            print(f"task_id успешно получен: {task_id}")
            return task_id
        else:
            print("task_id не найден в ответе.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None


def send_api_request(task_id, api_token, handle_response):
    """
    Отправляет API-запрос к Wildberries API для загрузки данных по task_id.

    :param task_id: str - Уникальный идентификатор задачи.
    :param api_token: str - Токен API для аутентификации.
    :param handle_response: function - Функция для обработки ответа API.
    """
    if not task_id:
        print("task_id отсутствует. Невозможно выполнить запрос.")
        return

    url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"

    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    max_retries = 5  # Максимальное количество попыток
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            if response.status_code == 200:
                print("Данные успешно получены. Обработка...")
                handle_response(response.json())  # Передаем данные в функцию обработки
                return  # Успешный запрос, завершаем функцию
            else:
                print(f"Ошибка: статус-код {response.status_code}, текст: {response.text}")
                return

        except requests.exceptions.HTTPError as e:
            if response.status_code in [404, 429]:
                retries += 1
                print(f"Получена ошибка {response.status_code}. Ожидание 15 секунд перед повтором попытки ({retries}/{max_retries})...")
                time.sleep(15)
            else:
                print(f"HTTP ошибка: {e}")
                return

        except requests.exceptions.RequestException as e:
            print(f"Ошибка соединения: {e}")
            return

    print(f"Не удалось выполнить запрос после {max_retries} попыток.")


def upload_data_to_google_sheets(data, service_account_file, spreadsheet_name, sheet_name):
    """
    Загружает данные в указанный лист Google Sheets и очищает его перед записью.

    :param data: list - Данные в формате JSON (список словарей).
    :param service_account_file: str - Путь к JSON-файлу с ключами для Google API.
    :param spreadsheet_name: str - Название Google Sheets таблицы.
    :param sheet_name: str - Название листа в таблице.
    """
    try:
        # Проверяем наличие файла учетных данных
        with open(service_account_file, 'r') as f:
            pass  # Проверяем, что файл существует

        # Устанавливаем соединение с Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, scope)
        client = gspread.authorize(credentials)

        # Открываем таблицу
        spreadsheet = client.open(spreadsheet_name)

        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Лист '{sheet_name}' не найден. Создаём новый.")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=26)

        # Очищаем содержимое листа
        sheet.clear()
        print(f"Лист '{sheet_name}' очищен.")

        if isinstance(data, list) and data:
            # Добавляем заголовки
            headers = list(data[0].keys())
            sheet.append_row(headers)

            # Преобразуем данные в список списков
            rows_to_append = []
            for row in data:
                processed_row = []
                for key in headers:
                    if key == "warehouses":
                        warehouses_data = row[key]
                        warehouses_str = "; ".join(
                            [f"{w['warehouseName']} ({w['quantity']})" for w in warehouses_data]
                        )
                        processed_row.append(warehouses_str)
                    else:
                        processed_row.append(row[key])
                rows_to_append.append(processed_row)

            # Добавляем строки в таблицу
            sheet.append_rows(rows_to_append, value_input_option="RAW")
            print(f"Данные успешно добавлены в лист '{sheet_name}' таблицы '{spreadsheet_name}'.")
        else:
            print("Данные отсутствуют или их формат не поддерживается.")

    except FileNotFoundError:
        print(f"Файл учетных данных не найден: {service_account_file}. Проверьте путь.")
    except gspread.exceptions.APIError as e:
        print(f"Ошибка Google API: {e}")
    except Exception as e:
        print(f"Ошибка при загрузке данных в Google Sheets: {e}")


def handle_response(data):
    """
    Обрабатывает ответ от API и загружает данные в Google Sheets.
    :param data: dict - Данные, полученные из API.
    """
    print("Данные получены. Отправляем в Google Sheets...")

    # Указываем путь к JSON-файлу с ключами
    SERVICE_ACCOUNT_FILE = 'credentials/animated-graph-443514-a5-27e379b134ca.json'

    # Название Google Sheets таблицы и листа
    SPREADSHEET_NAME = 'Wildberries Data'
    SHEET_NAME = 'Stocks'

    # Вызываем функцию загрузки
    upload_data_to_google_sheets(data, SERVICE_ACCOUNT_FILE, SPREADSHEET_NAME, SHEET_NAME)
    print("Данные успешно загружены в Google Sheets!")


if __name__ == "__main__":
    api_token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMTE4djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc0ODk5MzcyNiwiaWQiOiIwMTkzOGM0ZC1jMTVhLTczMTAtYmZjYS03NDM3NGI2YzY3NzEiLCJpaWQiOjI3ODc5NDgwLCJvaWQiOjUwMTM4LCJzIjoxMDczNzQ1MDIyLCJzaWQiOiJlYWMyYjNjNy1iZTA5LTVkYzYtOWE2MS01NTRlYWU3ZDgyNTEiLCJ0IjpmYWxzZSwidWlkIjoyNzg3OTQ4MH0.d8xSaX6q4raxU5Dk_1xQ1S6CErxhiSv23gSci2r007KJNR3nK5b-zf-Ey0NT79KV3iPuT1UPZPp-dThZCKvtsg"

    # Первый запрос для получения task_id
    task_id = get_task_id(api_token)

    # Второй запрос для получения данных и загрузки их в Google Sheets
    if task_id:
        send_api_request(task_id, api_token, handle_response)
