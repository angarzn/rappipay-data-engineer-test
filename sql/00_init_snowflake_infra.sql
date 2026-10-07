-- Usa el rol de superusuario del sistema para crear infraestructura
USE ROLE ACCOUNTADMIN;

-- Crea el clúster de cómputo (Warehouse)
CREATE WAREHOUSE IF NOT EXISTS RAPPI_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse para orquestación con Airflow';

-- Crea la base de datos
CREATE DATABASE IF NOT EXISTS MEETUP_DB;

-- Crea los esquemas (Arquitectura Medallion simplificada)
CREATE SCHEMA IF NOT EXISTS MEETUP_DB.RAW;       
CREATE SCHEMA IF NOT EXISTS MEETUP_DB.MASTER;  

-- Valida el contexto
USE WAREHOUSE RAPPI_WH;
USE DATABASE MEETUP_DB;
USE SCHEMA RAW;