# Changelog

### 0.1.95
- added numpy = "2.3.2 required by some parts of plotly

### 0.1.9
- removed old brreg api
- added plotly = "6.3.0"

### 0.1.8
- redid CodeChangeItem and added get_changes_between_versions_in_classification() to fetch them
- updated several packages:
    requests = "2.32.4", pendulum = "3.1.0", ijson = "3.4.0", oracledb = "3.1.1", dbt-oracle = "1.9.2",
    google-cloud-secret-manager = "2.24.0", confluent-kafka = "2.10.1", fastavro = "1.12.0",
    google-cloud-bigquery = "3.35.1", pydantic = "2.11.7"

### 0.1.7
- python 3.11 -> 3.12
- removed log_etl()
- yield only non-empty batches from kafka
- added CodeChangeItem
- upgraded dbt-oracle = "1.9.0", google-cloud-bigquery = "3.29.0", google-cloud-secret-manager = "2.22.1"

### 0.1.6
- upgraded oracledb = "2.5.1", dbt-oracle = "1.8.4", google-cloud-secret-manager = "2.22.0", confluent-kafka = "2.8.0", fastavro = "1.10.0"
- added KAFKA_VALUE_RAW containing message.value() before deserialization

### 0.1.5
- removed enable_etl_logging parameters and functionality  (etl_log function is kept as-is) 

### 0.1.4
- etl_log() now makes a copy of previous bindvars and reset to that copy instead
- fixed some typing issues in kafka api

### 0.1.3
- upgraded confluent-kafka = 2.6.0

### 0.1.2
- replaced all SSB API methods with: get_classification, get_classifcation_version, get_correspondence
- upgraded google-cloud-bigquery = "3.26.0"

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

