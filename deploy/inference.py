import os
import pandas as pd
import joblib
import pickle
import io
from datetime import datetime

# Install the required pandas version
os.system('pip install pandas==1.4.4')

# load the model from disk
def model_fn(model_dir):
    """Load the model from disk.
    
    This function is called by SageMaker to load the model.
    """
    model_file = os.path.join(model_dir, "pipeline.pkl")
    return joblib.load(model_file)

# load the input data from request body
def input_fn(request_body, request_content_type):
    if request_content_type == "application/python-pickle":
        data = pickle.loads(request_body)
        return data
    else:
        # Handle other content-types here or raise an Exception
        raise ValueError("This model only supports application/python-pickle input")

# feed the input data to the model
def predict_fn(input_data, model):
    """A predict_fn that predicts for the input data"""
    predictions = model.predict(input_data)
    return predictions

# return the pickle string to client
def output_fn(prediction, content_type):
    """An output_fn that outputs the prediction result"""
    if content_type == "application/python-pickle":
        output = io.BytesIO()
        pd.DataFrame(prediction).to_pickle(output)
        output.seek(0)
        return output.read()
    else:
        # Handle other content-types here or raise an Exception
        raise ValueError("This model only supports application/python-pickle output")
    