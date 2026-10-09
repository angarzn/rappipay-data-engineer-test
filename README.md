# Meetup Analytics ETL Pipeline

Este repositorio contiene la solución a la prueba técnica de RappiPay para Data Engineer. Se implementó un pipeline automatizado (Data Lakehouse) utilizando **Apache Airflow**, **Snowflake** y **AWS S3**.

El proyecto toma datos crudos de la plataforma Meetup (Kaggle) y los transforma en una capa analítica (MASTER) preparada para el consumo de BI, garantizando observabilidad, seguridad y persistencia histórica.

## Arquitectura y Características Principales

* **Orquestación Paralela (Airflow):** DAG programado con intervalos de 15 minutos que ejecuta la transformación de tablas maestras de forma concurrente, optimizando los tiempos de procesamiento.
* **Carga Incremental y Generación Sintética:** En lugar de sobreescrituras destructivas, se implementó un patrón de carga incremental utilizando la sentencia `MERGE` en Snowflake. Se combinan funciones nativas (`UNIFORM`, `RANDOM`, `SAMPLE`) para inyectar tráfico sintético y simular el crecimiento real de la plataforma.
* **Observabilidad y Manejo de Excepciones:** Integración nativa con Webhooks de **Slack**. El DAG utiliza ramificación condicional (`TriggerRule.ONE_FAILED`) para enviar alertas rojas críticas en caso de fallos y notificaciones verdes de éxito al finalizar correctamente.
* **Cross-Cloud Data Lake (AWS S3):** Exportación automatizada de los reportes analíticos a Amazon S3. Se implementó una arquitectura de seguridad mediante **Roles IAM y Storage Integrations** (evitando quemar credenciales en código). 
* **Optimización de Almacenamiento:** Los datos se exportan en formato columnar **Parquet** (compresión Snappy) y se versionan dinámicamente en cada ejecución, previniendo la sobreescritura y creando un histórico perfecto en el Data Lake.

## Evidencias
Dentro de la carpeta `evidences/` se encuentra el registro visual del funcionamiento de las bases de datos, las alertas de Slack, los grafos de Airflow y los archivos Parquet versionados en AWS S3.

## Despliegue del Proyecto Localmente

Inicia Airflow en tu máquina local ejecutando `astro dev start`.

Este comando levantará cinco contenedores de Docker en tu máquina, cada uno para un componente diferente de Airflow:

- **Postgres:** La base de datos de metadatos de Airflow.
- **Scheduler:** El componente de Airflow responsable de monitorear y disparar las tareas.
- **Dag Processor:** El componente de Airflow responsable de analizar e interpretar los DAGs.
- **API Server:** El componente de Airflow responsable de servir la interfaz de usuario (UI) y la API.
- **Triggerer:** El componente de Airflow responsable de ejecutar las tareas diferidas.

Cuando los cinco contenedores estén listos, el comando abrirá automáticamente tu navegador en la interfaz de Airflow en `http://localhost:8080/`. También deberías poder acceder a tu base de datos Postgres en `localhost:5432/postgres` con el usuario `postgres` y la contraseña `postgres`.

*Nota: Si ya tienes alguno de los puertos mencionados ocupados, puedes [detener tus contenedores de Docker existentes o cambiar el puerto](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).*

## Despliega tu Proyecto en Astronomer

Si tienes una cuenta de Astronomer, subir tu código a un entorno de producción en Astronomer es muy sencillo. Para obtener instrucciones de despliegue, consulta la documentación oficial: https://www.astronomer.io/docs/astro/deploy-code/

## Contacto

La CLI de Astronomer es mantenida con dedicación por el equipo de Astronomer. Para reportar un error (bug) o sugerir un cambio, comunícate con el equipo de soporte oficial.