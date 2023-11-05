CREATE DATABASE dbt_db;
CREATE USER dbt_user WITH PASSWORD '${DBT_PG_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE dbt_db TO dbt_user;
GRANT ALL ON SCHEMA public TO dbt_user;