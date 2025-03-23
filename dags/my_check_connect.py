import logging
import os
import inspect
import datetime
import psycopg2
import duckdb
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()
logger = logging.getLogger()

# Получаем переменные
doc_md = """
# ddddd
## is_access_granted_task - Проверка доступов и разрешений 
"""
DAG_NAME = os.path.basename(__file__).replace('.py', '')
CONN_ADB = 'postgresql://postgres:123456@host.docker.internal:5432/postgres'
USERNAME = 'airflow2'  # 'airflow2'
SCHEDULER = "@daily"

table_list_1 = {
    'db': {
        'adb': {
            'operator': {
                'INSERT': {  # Список таблиц в которые надо вставлять информцию
                    'schema': {
                        'description': ['data_asset', 'data_model', 'data_table', 'data_value', 'desc_all', ],
                        'test': ['products', ],
                    },
                },
                'SELECT': {  # Список таблиц из которых надо отбирать информацию
                    'schema': {
                        'description': ['data_asset', 'data_model', 'data_table', 'data_value', 'desc_all', ],
                        'test': ['products', ],
                    },
                },
            }
        },
    },
}


def get_conn_postgresql(connect: str) -> psycopg2.extensions.connection:
    """
    Подключение к базе данных PostgreSQL.
    :connect: Строка подключения формата "postgresql://postgres:123456@localhost:5432/postgres"
    :return: Возвращаем коннект к базе данных.
    """
    # Получить имя базы данных.
    db = urlparse(connect).path[1:].upper()
    try:
        conn = psycopg2.connect(connect)
        logger.info(f"{db}=True Подключение к базе данных успешное.")
        return conn
    except Exception as error:
        logger.error(f"{db}=False Ошибка подключения к базе данных: {error}.")
        return None


def get_conn_duckdb() -> duckdb.DuckDBPyConnection:
    """
    Создание подключения к базе данных DuckD.
    Данные, которые хранятся будут потеряны,как только соединение закроется или программа завершится.
    """
    try:
        conn = duckdb.connect(database=':memory:')
        logger.info("DuckDB подключение успешно.")
        return conn
    except Exception as error:
        logger.error(f"Ошибка подключения к DuckDB: {error}.")
        return None


def is_user_exist(conn: psycopg2.extensions.connection) -> bool:
    """
    Проверка наличия пользователя в базе данных.
    :CONN_ADB: строка подключения.
    :username: имя пользователя.
    :return: bool
    """

    sql = f"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) AS user_exists"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (USERNAME,))
            exists = cur.fetchone()[0]
            logger.info(f'Пользователь базы данных {"существует" if exists else "отсутствует"}.')
            return exists
    except Exception as error:
        logger.error(f'Ошибка при проверке пользователя {USERNAME}: {error}')
        return False


def is_table_access_rights(conn: psycopg2.extensions.connection, schema_table: dict) -> bool:
    """
    Проверка доступов к таблицам в базе данных PostgreSQL.
    :conn: Подключение к целевой базе данных
    :schema_table: список схем которых надо проверить
    :return: Если доступ есть возвращаем True
    """
    # Используем глобальную переменную USERNAME
    username = USERNAME
    # таблица привилегий из json
    privilege_json = 'privilege_json'
    # таблица привилегий из db
    privilege_db = 'privilege_db'
    # Список допустимых значений допустимых для
    pr_list_base = {'DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'}
    # Хранение списка схем баз данных
    set_schema = set()
    # Сохраняем список данных для последующей вставки
    insert_data_json = []
    # Запрос для целевой базы данных для получения списка разрешений
    sql_select = """
                SELECT table_schema, table_name, privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = %s AND table_schema = %s;
                """
    try:
        function_name = inspect.currentframe().f_code.co_name

        # Создаем временную таблицу в DuckDB
        db_db = get_conn_duckdb()
        if db_db is None:
            return False

        db_db.execute(f"CREATE TABLE {privilege_json} (shem VARCHAR, tab VARCHAR, pr VARCHAR);")
        db_db.execute(f"CREATE TABLE {privilege_db}   (shem VARCHAR, tab VARCHAR, pr VARCHAR);")

        for i_pr, privilege_list in schema_table.items():
            if i_pr not in pr_list_base:
                logger.error(f"{function_name}: {i_pr} не в {pr_list_base}")
                continue

            for i_schema, tab in privilege_list.get('schema', {}).items():
                set_schema.add(i_schema)
                for i_tab in tab:
                    insert_data_json.append((i_schema, i_tab, i_pr))

        if insert_data_json:
            db_db.executemany(f"INSERT INTO {privilege_json} VALUES(?, ?, ?);", insert_data_json)

        for i_schema in set_schema:
            with conn.cursor() as cur:
                cur.execute(sql_select, (username, i_schema))
                results_sql = cur.fetchall()
                if results_sql:
                    db_db.executemany(f"INSERT INTO {privilege_db} VALUES(?, ?, ?);", results_sql)
        # Получение разницы данных
        except_data = f"SELECT * FROM {privilege_json} EXCEPT SELECT * FROM {privilege_db};"
        info = db_db.execute(except_data).fetchall()
        db_db.close()  # Закрываем соединение с DuckDB
        for i in info:
            logger.error(f"No access to {i[2]} table {i[0]}.{i[1]}.")
        return False if info else True  # Возвращаем True, если проверка прошла успешно

    except Exception as error:
        logger.error(f"Ошибка при проверке прав доступа для пользователя {username}: {error}")
        return False


def preliminary_check(table_list: dict) -> bool:
    """
    Итеративно обходим всю базу данных и проверяем разрешения.
    1) Проверка пользователя и подключения к базе данных
    2) Проверка доступов пользователя
    :return: Возвращает False если хотя бы одно условие не пройдено.
    """
    conditions = []  # Сохраняем в список проверок

    for db_key, db_value in table_list.get('db').items():
        logger.info(f'========== START = Проверка наличия доступов в базе данных {db_key}. ==========')

        # 1) Проверка пользователя и подключения к базе данных
        conn = get_conn_postgresql(CONN_ADB)
        if conn is None:
            logger.warning("Не удалось подключиться к базе данных.")
            return False

        is_user = is_user_exist(conn)
        conditions.append(is_user)

        # 2) Проверка доступов пользователя
        is_table = is_table_access_rights(conn, db_value.get('operator'))
        conditions.append(is_table)

        logger.info(f'========== STOP = Проверка наличия доступов в базе данных {db_key}. ==========')

    if all(conditions):
        logger.info('Все разрешения выполнены.')
        return True
    else:
        logger.warning('Одно или несколько условий не выполнены.')
        return False


def is_access_granted():
    """
    Задача по проверке предоставленных прав.
    Создана для стандартной проверки и логирования подключений к базам данных.
    """

    preliminary_check(table_list_1)  # Проверка доступов


# Параметры по умолчанию
default_args = {
    "owner": "stravkin",
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 3, 22),
    "retries": 1,
}

# Создание DAG
dag = DAG(
    dag_id=os.path.basename(__file__).replace('.py', ''),  # Задаем имя DAG
    doc_md='Пример DAG с проверкой доступа.',  # Подключаем описательную документацию
    catchup=False,  # Установите в False, если не хотите обрабатывать пропущенные запуски
    schedule_interval='@daily',  # Задаем время запуска
    default_args=default_args,
)

# Задача 1: Начало процесса
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Задача 2: Проверка доступа
access_granted_task = PythonOperator(
    task_id="is_access_granted",
    python_callable=is_access_granted,
    dag=dag,
)

# Задача 3: Конец процесса
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Задаем порядок выполнения
start_task >> access_granted_task >> end_task
