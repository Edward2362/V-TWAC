# V-TWAC

Vietnamese Traffic weather analysis of correlation

## Except for training_model_and_deploy folder, which has its own README file, other folders are docker containers. Below are steps to run the pipeline correctly:

1. Running weather producer: docker-compose -f "weatherbit_producer/docker-compose.yml"
2. Running weather producer: docker-compose -f "incidents_producer/docker-compose.yml"
3. Running weather producer: docker-compose -f "weatherbit_etl/docker-compose.yml"
4. Running weather producer: docker-compose -f "incidents_etl/docker-compose.yml"
5. Running weather producer: docker-compose -f "combination_etl/docker-compose.yml"
