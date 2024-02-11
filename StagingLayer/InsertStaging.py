import pandas as pd

from FirebaseLayer.FirebaseClient import FirebaseClient
from PostgresLayer.DfToPostgres import DfToPostgres


class InsertStaging():
  
    def __init__(self):
        self.client = FirebaseClient()
        self.engine = DfToPostgres()
        
    
    def get_easyflux_data(self) -> pd.DataFrame:
        doc_list = self.client.get_collection("@EasyFlux:transactions_user:112300277810077836709")
        df = self.client.format_dataframe(doc_list=doc_list) 
        return df
      
      
    def split_budget_and_actual(self, df:pd.DataFrame) -> pd.DataFrame:
        df_budget, df_actual = self.engine.prepare_df_to_staging(df)
        return df_budget, df_actual
      
    
    def insert_to_staging(self, df_budget:pd.DataFrame, df_actual:pd.DataFrame) -> None:
        self.engine.create_engine(df=df_budget,table='stg_budget')
        self.engine.create_engine(df=df_actual,table='stg_actual')
    
 