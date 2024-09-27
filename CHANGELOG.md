# Changelog

### 0.1.1
- upgraded oracledb = "2.3.0", dbt-oracle = "1.8.1", google-cloud-secret-manager = "2.20.2", confluent-kafka = "2.5.3", fastavro = "1.9.7"
- log_etl() now "resets" inputsize in case it was set by binding names and no sql statement was executed before logging statement, as this causes DPY-2006

## 0.1.0
- upgraded google-cloud-secret-manager = "2.20.2"

## 0.0.99
- upgraded google-cloud-secret-manager = "2.20.1", confluent-kafka = "2.5.0", fastavro = "1.9.5"
- refactored typing in oracle module

## 0.0.98
- upgrade dbt to 1.8.0
- store last entry in changelog from SSB classification-version as "oppdatert_besk" in metadata record

## 0.0.97
- added bigquery api v1, reading rows from a user query
- removed dbt project execute

