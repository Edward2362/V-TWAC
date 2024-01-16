import os
import boto3
import json
import re

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime_client= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):
    
    print("Received event: " + json.dumps(event, indent=2))
    
    data = json.loads(json.dumps(event["body"]))
       
    try:    
        response = runtime_client.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="application/json",
        Accept="application/json",
        Body=data,
        )
        raw_data = response['Body'].read().decode()
    
    
        # Parse the JSON string
        data = json.loads(raw_data)
        
        # Combine words based on their entities and ## prefixes
        i = 0
        results = []
        while i < len(data):
            item = data[i]
            
            # Start the combination process if the entity is 'B-Symptom' or 'I-Symptom' not following a 'B-Symptom'
            if item['entity'] == 'B-Symptom' or (item['entity'] == 'I-Symptom' and (not results or (results and not results[-1].startswith('B-Symptom')))):
                combined_word = item['word']
                
                # Loop through the subsequent items to see if they should be combined
                i += 1
                while i < len(data) and (data[i]['entity'] in ('I-Symptom', 'E-Symptom') or data[i]['word'].startswith('##')):
                    # If the word starts with ##, just append without ##
                    if data[i]['word'].startswith('##'):
                        combined_word += data[i]['word'][2:]
                    else:
                        combined_word += ' ' + data[i]['word']
                    i += 1
                
                results.append(combined_word)
                
            # If the word starts with ##, concatenate with the previous word in results (if there's a previous word)
            elif item['word'].startswith('##') and results:
                results[-1] += item['word'][2:]
                i += 1
        
            else:
                i += 1
        
        result_dict = {"result": results}
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(result_dict)
        }
        
        
    except Exception as e: 
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "BAD REQUEST"
        };   