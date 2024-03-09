# VICTRA - Vietnamese Interconnected Climate & Traffic Relations Assessment

RMIT EEET2574 - Big Data for Engineers's Final Assignment

---

## Group members

Nguyen Vinh Quang – S3817788

Tran Nam Thai - S3891890

Hoan Minh Khoi – S3822041

Pham Anh Thu - S3878246

Nguyen Quoc Thang – S3796613

---

## Initiate date pipeline

**Except for the final model's `training_model_and_deploy` folder, which has its own `README.md` file, other folders are docker containers. Below are steps to run the pipeline correctly:**

Run these command in bash script

```bash
# Weather-bit producer
docker-compose -f "weatherbit_producer/docker-compose.yml"

# Traffice incidents producer
docker-compose -f "incidents_producer/docker-compose.yml"

# Weather ETL
docker-compose -f "weatherbit_etl/docker-compose.yml"

# Traffic ETL
docker-compose -f "incidents_etl/docker-compose.yml"

# Weather - Traffic Combination ETL
docker-compose -f "combination_etl/docker-compose.yml"
```
