-- Crea las reglas de seguridad
CREATE STORAGE INTEGRATION s3_export_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXXXXXXXXXXX:role/Snowflake_S3_Export_Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://rappipay-meetup-snowflake-export/');

-- Obtiene el usuario de Snowflake para autorizarlo en AWS
  DESC INTEGRATION s3_export_int;

-- Construye el stage en MEET_UP.MASTER  para usar los permisos definidos en s3_export_int
  CREATE OR REPLACE STAGE MEETUP_DB.MASTER.s3_export_stage
  URL = 's3://rappipay-meetup-snowflake-export/'
  STORAGE_INTEGRATION = s3_export_int
  FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = '"');