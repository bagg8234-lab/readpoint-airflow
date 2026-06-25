from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
import psycopg2
import requests
import os

FUNCTIONS_URL_META = os.environ.get("AZURE_FUNCTIONS_URL_META")
FUNCTIONS_KEY_META = os.environ.get("AZURE_FUNCTIONS_KEY_META")
FUNCTIONS_URL = os.environ.get("AZURE_FUNCTIONS_URL")
FUNCTIONS_KEY = os.environ.get("AZURE_FUNCTIONS_KEY")

PG_HOST = os.environ.get("PG_HOST")
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode='prefer'
    )

def call_function(func_name: str, payload: dict):
    url = f"{FUNCTIONS_URL}/api/{func_name}"
    headers = {"x-functions-key": FUNCTIONS_KEY, "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=payload, timeout=3600)
    response.raise_for_status()
    print(f"[{func_name}] 완료 - status: {response.status_code}")
    return response.json() if response.text else {}

def metadata_parser(**context):
    file_url = context["dag_run"].conf.get("file_url")
    admin_id = context["dag_run"].conf.get("admin_id")
    url = f"{FUNCTIONS_URL_META}/api/metadata_parser"
    headers = {"x-functions-key": FUNCTIONS_KEY_META, "Content-Type": "application/json"}
    payload = {"file_url": file_url, "admin_id": admin_id}
    response = requests.post(url, headers=headers, json=payload, timeout=600)
    response.raise_for_status()
    result = response.json()
    books_id = result.get("books_id")
    context["ti"].xcom_push(key="books_id", value=books_id)
    print(f"[metadata_parser] 완료 - books_id: {books_id}")

# 1차 파이프라인
def chapter_split(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    call_function("chapter_split", {"books_id": books_id})

def openai_extract_chapter(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chapter_id FROM chapter WHERE books_id = %s", (books_id,))
    chapter_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    print(f"[openai_extraxt_chapter] {len(chapter_ids)}개 챕터 처리 시작")
    for chapter_id in chapter_ids:
        call_function("openai_extract_chapter", {"books_id": books_id, "chapter_id": chapter_id})
        print(f"[openai_extraxt_chapter] chapter_id={chapter_id} 완료")

def normalize_characters(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    call_function("normalize_characters", {"books_id": books_id})

def save_normalized_analysis(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    call_function("save_normalized_analysis", {"books_id": books_id})

def book_graph_refine(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    call_function("book_graph_refine", {"books_id": books_id})

# 2차 파이프라인
def migrate_graph(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")
    call_function("migrate_graph", {"books_id": books_id})

def generate_progress_summary_event(**context):
    books_id = context["ti"].xcom_pull(task_ids="metadata_parser", key="books_id")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT event_id FROM event WHERE books_id = %s", (books_id,))
    event_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    print(f"[generate_progress_summary_event] {len(event_ids)}개 이벤트 처리 시작")
    for event_id in event_ids:
        call_function("generate_progress_summary_event", {"books_id": books_id, "event_id": event_id})
        print(f"[generate_progress_summary_event] event_id={event_id} 완료")

with DAG(
    dag_id="readpoint_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["readpoint"],
    params={
        "file_url": Param("", type="string", description="epub Blob URL"),
        "admin_id": Param("1", type="string", description="관리자 ID"),
    }
) as dag:

    t_metadata = PythonOperator(task_id="metadata_parser", python_callable=metadata_parser)

    # 1차
    t_chapter_split = PythonOperator(task_id="chapter_split", python_callable=chapter_split)
    t_openai_extract_chapter = PythonOperator(task_id="openai_extract", python_callable=openai_extract_chapter)
    t_normalized = PythonOperator(task_id="normalize_characters", python_callable=normalize_characters)
    t_save_normalized = PythonOperator(task_id="save_normalize", python_callable=save_normalized_analysis)

    # 2차
    t_graph_refine = PythonOperator(task_id="book_graph_refine", python_callable=book_graph_refine)
    t_migrate_graph = PythonOperator(task_id="migrate_graph", python_callable=migrate_graph)
    t_progress_summary_event = PythonOperator(task_id="generate_progress_summary", python_callable=generate_progress_summary_event)

    # 의존성
    t_metadata >> t_chapter_split >> t_openai_extract_chapter >> t_normalized >> t_save_normalized >> t_graph_refine
    t_graph_refine >> t_migrate_graph >> t_progress_summary_event