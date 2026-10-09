import json

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
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
    CREATE TABLE IF NOT EXISTS MEETUP_DB.MASTER.GROUPS_EVENTS_SUMMARY AS
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
    CREATE TABLE IF NOT EXISTS MEETUP_DB.MASTER.CITY_ACTIVITY_METRICS AS
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
    CREATE TABLE IF NOT EXISTS MEETUP_DB.MASTER.MEMBER_INTERESTS_SUMMARY AS
    SELECT 
        t.TOPIC_NAME, 
        COUNT(mt.MEMBER_ID) AS total_interested_members
    FROM MEETUP_DB.RAW.RAW_TOPICS t
    JOIN MEETUP_DB.RAW.RAW_MEMBERS_TOPICS mt ON t.TOPIC_ID = mt.TOPIC_ID
    GROUP BY t.TOPIC_NAME;
    """

    # Identifica los espacios físicos más recurrentes, permite descubrir locaciones clave
    query_venues = """
    CREATE TABLE IF NOT EXISTS MEETUP_DB.MASTER.VENUE_USAGE_STATS AS
    SELECT 
        v.VENUE_ID, v.VENUE_NAME, v.CITY, 
        COUNT(e.EVENT_ID) AS total_events_hosted
    FROM MEETUP_DB.RAW.RAW_VENUES v
    JOIN MEETUP_DB.RAW.RAW_EVENTS e ON v.VENUE_ID = e.VENUE_ID
    GROUP BY v.VENUE_ID, v.VENUE_NAME, v.CITY;
    """

    # Crea una tabla temporal seleccionando 5 ciudades al azar y asignándoles entre 1 y 50 eventos nuevos usando funciones nativas.
    query_generate_synthetic = """
    CREATE OR REPLACE TABLE MEETUP_DB.MASTER.SYNTHETIC_ACTIVITY AS 
    SELECT CITY, UNIFORM(1, 50, RANDOM()) AS new_synthetic_events
    FROM MEETUP_DB.RAW.RAW_CITIES 
    SAMPLE (5 ROWS);
    """

    # Actualiza la tabla maestra sumando los eventos ficticios únicamente a las ciudades que hicieron match.
    query_merge_data = """
    MERGE INTO MEETUP_DB.MASTER.CITY_ACTIVITY_METRICS target
    USING MEETUP_DB.MASTER.SYNTHETIC_ACTIVITY source
    ON target.CITY = source.CITY
    WHEN MATCHED THEN 
        UPDATE SET target.TOTAL_EVENTS = target.TOTAL_EVENTS + source.new_synthetic_events;
    """

    # Exporta la tabla MASTER.CITY_ACTIVITY_METRICS a S3 usando el Stage de Snowflake creado
    query_export_s3 = """
    COPY INTO @MEETUP_DB.MASTER.s3_export_stage/city_metrics_export/city_metrics_{{ ts_nodash }}_
    FROM MEETUP_DB.MASTER.CITY_ACTIVITY_METRICS
    FILE_FORMAT = (TYPE = PARQUET);
    """

    # Nodos flag de inicio y fin 
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline', trigger_rule=TriggerRule.NONE_FAILED)

    # Tarea Snowflake 1
    task_groups = SnowflakeOperator(
        task_id='create_groups_summary',
        snowflake_conn_id='snowflake_default',
        sql=query_groups,
    )

    # Tarea Snowflake 2
    task_cities = SnowflakeOperator(
        task_id='create_city_metrics',
        snowflake_conn_id='snowflake_default',
        sql=query_cities
    )

    # Tarea Snowflake 3
    task_interests = SnowflakeOperator(
        task_id='create_interests_summary',
        snowflake_conn_id='snowflake_default',
        sql=query_interests
    )

    # Tarea Snowflake 4
    task_venues = SnowflakeOperator(
        task_id='create_venue_stats',
        snowflake_conn_id='snowflake_default',
        sql=query_venues
    )
    
    # Tarea Snowflake 5
    task_generate_synthetic = SnowflakeOperator(
        task_id='generate_synthetic_activity',
        snowflake_conn_id='snowflake_default',
        sql=query_generate_synthetic
    )
    
    # Tarea Snowflake 6
    task_merge_data = SnowflakeOperator(
        task_id='merge_synthetic_data',
        snowflake_conn_id='snowflake_default',
        sql=query_merge_data
    )

    # Tarea Slack - Alerta OK
    task_slack_alert = HttpOperator(
        task_id='send_slack_alert_success',
        http_conn_id='slack_conn',
        endpoint='T0C7P0SP19P/B0C7K51RP2A/95QpPToj03BAytQbyyFKdvcT', # Placeholder por seguridad 
        method='POST',
        data=json.dumps({
            "text": "🚀 *Pipeline ETL Exitoso*\n✅ Datos procesados y MERGE completado.\n✅ Exportación a S3 finalizada con éxito."
        }),
        headers={"Content-Type": "application/json"},
    )

    task_slack_alert_fail = HttpOperator(
        task_id='send_slack_alert_fail',
        http_conn_id='slack_conn',
        endpoint='T0C7P0SP19P/B0C7K51RP2A/95QpPToj03BAytQbyyFKdvcT',
        method='POST',
        data=json.dumps({
            "text": "🚨 *ALERTA CRÍTICA: Fallo en el Pipeline*\nEl proceso de MERGE en Snowflake falló. Revisar logs inmediatamente."
        }),
        headers={"Content-Type": "application/json"},
        trigger_rule=TriggerRule.ONE_FAILED # Condición para fallo
    )
    
    task_export_s3 = SnowflakeOperator(
        task_id='export_metrics_to_s3',
        snowflake_conn_id='snowflake_default',
        sql=query_export_s3
    )
    # Flujo de ejecución
    start_pipeline >> [task_groups, task_cities, task_interests, task_venues]
    
    task_cities >> task_generate_synthetic >> task_merge_data >> task_export_s3 >> [task_slack_alert, task_slack_alert_fail] >> end_pipeline
    task_groups >> end_pipeline
    task_interests >> end_pipeline
    task_venues >> end_pipeline