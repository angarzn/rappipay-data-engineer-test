from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Configuración base del DAG
default_args = {
    'owner': 'Oscar Andrés Garzón',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG (Se ejecuta cada 15 minutos)
with DAG(
    'meetup_analytics_etl',
    default_args=default_args,
    description='ETL pipeline para transformar datos de Meetup en Snowflake',
    schedule='*/15 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rappipay', 'meetup', 'etl', 'master'],
) as dag:

    # Mide el nivel de actividad de cada grupo. Expone cuántos eventos históricos han organizado y la fecha de su evento más reciente,
    # lo que permite identificar rápidamente comunidades líderes frente a grupos inactivos.
    query_groups = """
    CREATE OR REPLACE TABLE MEETUP_DB.MASTER.GROUPS_EVENTS_SUMMARY AS
    SELECT 
        g.GROUP_ID,
        g.GROUP_NAME,
        g.CITY,
        COUNT(e.EVENT_ID) AS total_events,
        MAX(e.CREATED) AS last_event_date
    FROM MEETUP_DB.RAW.RAW_GROUPS g
    LEFT JOIN MEETUP_DB.RAW.RAW_EVENTS e 
        ON g.GROUP_ID = e.GROUP_ID
    GROUP BY 
        g.GROUP_ID,
        g.GROUP_NAME,
        g.CITY;
    """

    # Muestra el volumen total de grupos y eventos por ciudad y país, información clave para decidir dónde enfocar campañas de marketing o expansión.
    query_cities = """
    CREATE OR REPLACE TABLE MEETUP_DB.MASTER.CITY_ACTIVITY_METRICS AS
    SELECT 
        c.CITY, c.COUNTRY, 
        COUNT(DISTINCT g.GROUP_ID) AS total_groups, 
        COUNT(DISTINCT e.EVENT_ID) AS total_events
    FROM MEETUP_DB.RAW.RAW_CITIES c
    LEFT JOIN MEETUP_DB.RAW.RAW_GROUPS g ON c.CITY = g.CITY
    LEFT JOIN MEETUP_DB.RAW.RAW_EVENTS e ON g.GROUP_ID = e.GROUP_ID
    GROUP BY c.CITY, c.COUNTRY;
    """

    # Revela la popularidad de las temáticas en toda la plataforma. Contabiliza cuántos usuarios reales siguen cada tópico,
    # vital para entender la demanda macro y predecir qué tipos de eventos tendrán más asistencia.
    query_interests = """
    CREATE OR REPLACE TABLE MEETUP_DB.MASTER.MEMBER_INTERESTS_SUMMARY AS
    SELECT 
        t.TOPIC_NAME, 
        COUNT(mt.MEMBER_ID) AS total_interested_members
    FROM MEETUP_DB.RAW.RAW_TOPICS t
    JOIN MEETUP_DB.RAW.RAW_MEMBERS_TOPICS mt ON t.TOPIC_ID = mt.TOPIC_ID
    GROUP BY t.TOPIC_NAME;
    """

    # Identifica los espacios físicos más recurrentes, permite descubrir locaciones clave
    query_venues = """
    CREATE OR REPLACE TABLE MEETUP_DB.MASTER.VENUE_USAGE_STATS AS
    SELECT 
        v.VENUE_ID, v.VENUE_NAME, v.CITY, 
        COUNT(e.EVENT_ID) AS total_events_hosted
    FROM MEETUP_DB.RAW.RAW_VENUES v
    JOIN MEETUP_DB.RAW.RAW_EVENTS e ON v.VENUE_ID = e.VENUE_ID
    GROUP BY v.VENUE_ID, v.VENUE_NAME, v.CITY;
    """

    # Nodos flag de inicio y fin 
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Tarea 1
    task_groups = SnowflakeOperator(
        task_id='create_groups_summary',
        snowflake_conn_id='snowflake_default', # La conexión que creamos en la interfaz
        sql=query_groups,
    )

    # Tarea 2
    task_cities = SnowflakeOperator(
        task_id='create_city_metrics',
        snowflake_conn_id='snowflake_default',
        sql=query_cities
    )

    # Tarea 3
    task_interests = SnowflakeOperator(
        task_id='create_interests_summary',
        snowflake_conn_id='snowflake_default',
        sql=query_interests
    )

    # Tarea 4
    task_venues = SnowflakeOperator(
        task_id='create_venue_stats',
        snowflake_conn_id='snowflake_default',
        sql=query_venues
    )

    # Flujo de ejecución
    start_pipeline >> [task_groups, task_cities, task_interests, task_venues] >> end_pipeline