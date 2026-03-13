ИТОГОВОЕ ЗАДАНИЕ ПО МОДУЛЮ 3. Евсеева Полина
В этом задании я реализовала полный ETL-пайплайн: генерация данных в MongoDB (user_sessions ~100, support_tickets ~50), репликация с трансформацией в PostgreSQL, 2 аналитические витрины (user_activity_mart, support_stats_mart) через Airflow DAGs.

Итоги: БД развернуты, данные чистые (без дублей), пайплайны работают (docker-compose up → localhost:8080), проверено psql (\dt: 4 таблицы, COUNT>0)
