""" Class for instantiating the DfToPostgres and FirebaseClient classes and 
executing their methods until data is inserted into the staging table in the postgres database.

This script requires that `pandas` be installed within the Python
environment you are running this script in.
"""


import pandas as pd

from FirebaseLayer.FirebaseClient import FirebaseClient
from PostgresLayer.DfToPostgres import DfToPostgres


class InsertStaging():
    """
      A class to get Firebase data and insert into Postgres staging table.

      ...

      Attributes
      ----------
      client : FirebaseClient object
          An object of the FirebaseClient class.
      engine : DfToPostgres object
          An object of the DfToPostgres class.
      
      Methods
      -------
      get_easyflux_data()
          Returns a dataframe with entries retrieved from firestore.   
      split_budget_and_actual(df:pd.DataFrame)
          Returns two dataframes divided into budget and actual figures.
      insert_to_staging(df_budget:pd.DataFrame, df_actual:pd.DataFrame)
          Create the engine with the postgres database and insert the data into the staging tables.
    """
  
    def __init__(self):
        """__init__

        Args:
            
        """
        self.client = FirebaseClient()
        self.engine = DfToPostgres()
        
    
    def get_easyflux_data(self) -> pd.DataFrame:
        """Returns a dataframe of entries retrieved from firestore database.

        Args:
        
        Returns:
            df:pd.DataFrame
        """
        doc_list = self.client.get_collection("{Collection string here}")
        df = self.client.format_dataframe(doc_list=doc_list) 
        return df
      
      
    def split_budget_and_actual(self, df:pd.DataFrame) -> pd.DataFrame:
        """Returns two dataframes divided into budget and actual figures.

        Args:
            df:pd.DataFrame
        
        Returns:
            df_budget:pd.DataFrame
            df_actual:pd.DataFrame
        """
        df_budget, df_actual = self.engine.prepare_df_to_staging(df)
        return df_budget, df_actual
      
    
    def insert_to_staging(self, df_budget:pd.DataFrame, df_actual:pd.DataFrame) -> None:
        """Create the engine with the postgres database and insert the data into the staging tables.

        Args:
            df_budget:pd.DataFrame
            df_actual:pd.DataFrame
        Returns:
            None
        """
        self.engine.create_engine(df=df_budget,table='stg_budget')
        self.engine.create_engine(df=df_actual,table='stg_actual')
    
 