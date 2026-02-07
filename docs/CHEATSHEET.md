# Шпаргалка по командам Parser Maps Pro

## Запуск приложения

python run_ui.py
# Откройте http://localhost:8787

## Git команды

git status                    # Проверить статус
git add .                     # Добавить все изменения
git commit -m 'сообщение'    # Создать коммит
git push origin main         # Отправить на GitHub

## Зависимости

pip install -r requirements.txt    # Установить зависимости
pip freeze > requirements.txt      # Обновить список зависимостей

## База данных

# SQLite (по умолчанию)
DB_KIND=sqlite
DB_PATH=./output/progress.sqlite

# PostgreSQL
DB_KIND=postgres
DB_DSN=postgresql://user:pass@host/db

## Структура config.xlsx

1. Cities - города для парсинга
2. Requests - поисковые запросы
3. Categories - категории Яндекс.Карт
4. Excludes - фразы для исключения

## Полезные ссылки

- GitHub: https://github.com/klimgaranin/parser_maps_pro
- Документация: https://github.com/klimgaranin/parser_maps_pro/tree/main/docs
