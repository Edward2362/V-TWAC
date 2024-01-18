# Steps to deploy the Neural Network model
### 1. Run the jupiter notebook `A3-model_final.ipynb` to create the output `pipeline.tar.gz` file
### 2. Upload the file to your *S3 Bucket* using AWS CLI or web interface

*Notes:*
To use the aws cli you need to install it and configure with your access credentials [AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
```shell
aws s3 cp /path/to/your/file.tar.gz s3://your-bucket-name/path/in/bucket/
```
### 3. Create and start a *JupiterNotebook* instance on AWS SageMaker
### 4. Upload the the following files to the notebook, make sure that they are in the same directory:

`deploy.ipynb` - Run this file for deploy the model after it has been uploaded to **S3 Bucket**

`inference.py` - Required file for entry point of the SageMaker endpoint, this file describe how the endpoint handle and process the requests from client

`invoke_model.ipynb` - Run this file after successfully deployed the model to endpoint to see the prediction results

`transformer.py` - Transformer classes added to the use in the pipeline when deployed

`requirements.txt` - List of required packages, libraries to install when building the container

`predictdatascript.py` - Script for prepare and preprocessing the input data

### 5. Modify the `predictdatascript.py`:
```python
server_uri = "YOUR_MONGODB_URI"
ACCESS_TOKEN = "YOUR_WEATHERBIT_ACCESS_TOKEN"
client = pymongo.MongoClient(server_uri)
db_name = "forecast"
collection_name = "weather"
collection_results =  client[db_name].results
```

### 6. Send a prediction request using the `invoke_model.ipynb`:
- Only perform this step after the endpoint status is `InService`
- Change the `city` and `district` variables in the script to fit your need
```python
district = "cau giay"
city = "Ha Noi"
```

*Notes: The script can also run on your local machine if you have already set up the AWS CLI credentials in the previous steps*



