import requests


base_url="http://localhost:9621"
url = f"{base_url}/documents"

# Заголовки запроса
headers = {
    "accept": "application/json",
}

file_paths = None

try:
    # Выполняем POST-запрос
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Вызовет исключение для HTTP ошибок

    # Парсим JSON-ответ
    data = response.json()
    file_paths = [chunk['file_path'] for chunk in data['statuses']['processed']]
except requests.exceptions.RequestException as e:
    print(f"Ошибка при выполнении запроса к RAG-сервису: {e}")
    # Возвращаем пустые значения в случае ошибки
    raise

print(file_paths)
