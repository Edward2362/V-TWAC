import pandas as pd
from category_encoders.one_hot import OneHotEncoder

class CustomColumnDropperTransformer():

    def __init__(self):
        self.selected_cols = ["district",  "precip", "rh","is_rain", "is_fog", "timeframe",  "app_temp", "uv" ] #"clouds",

    def select_columns(self, df):
        return_df = pd.DataFrame()
        for i in self.selected_cols:
            return_df[i] = df[i]
        return return_df
    
    def transform(self, df):
        return self.select_columns(df)

    def fit(self, df, y=None):
        return self 


class CustomOneHotEncoder():
    def __init__(self):
        self.ohe = OneHotEncoder(handle_unknown="ignore", 
                    cols=["timeframe", "district", "is_rain", "is_fog"]).set_output(transform='pandas')
    def transform(self, df):
        return self.ohe.transform(df)

    def fit(self, df, y=None):
        self.ohe.fit(df)
        return self 


class ExtractDateTransformer():
    def transform(self, df):
        return_df = df.copy()
        
        return_df["date"] = return_df["datetime"].apply(lambda x: x.split(":")[0])
        return_df["timeframe"] = return_df["datetime"].apply(lambda x: x.split(':')[-1])
        return return_df

    def fit(self, df, y=None):
        return self 
    
class DateLocationTransformer():
    def __init__(self):
        pass
        
    def transform(self, df):
        return_df = df.copy()
     
        # return_df = return_df.sort_values(by=['date', 'timeframe'], ascending=True)
        return_df = return_df.drop(['date', 'datetime'], axis=1)

        return return_df

    def fit(self, df, y=None):
        return self

    
