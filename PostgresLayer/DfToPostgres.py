""" DataFrame to Postgres table class

The class script below allows you to save dataframe rows to postgres table.

This script requires that `sqlalchemy` and `pandas` be installed within the Python
environment you are running this script in.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


class DfToPostgres():
    """
      A class to create a postgres database conection and insert EasyFlux actual and budget values

      ...

      Attributes
      ----------
      host : string
          postgres database host
      database : string
          postgres database name
      user : string
          user name for access
      password : string
          user password
      port : int
          postgres database port running
      df : dataframe
          dataframe given to insert into postgres database table
      Methods
      -------
      prepare_df_to_staging(df)
          returns two dataframes. One for budget e other for actual    
      create_engine(df)
          create the postgres engine and insert data  
    """

    def __init__(self) -> None:
        """__init__

        Args:
            
        """
        self.env = load_dotenv()                 
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.schema = os.getenv('SCHEMA')
    
    
    def __str__(self) -> str:
        """__str__

        Returns:
            str: postgres database connection string
        """
        return f'postgresql://{self.user}:{self.password}@:{self.host}:{self.port}/{self.database}'
    
    
    def prepare_df_to_staging(self, df:pd.DataFrame) -> pd.DataFrame:
        """Split the dataframe given in two different dataframes. One for actual figures 
        and another for budget figures.

        Args:
            df (pd.DataFrame): 

        Returns:
            pd.DataFrame: 
        """
        df_budget = df.query('entryType == "budget"').set_index('id')
        df_actual = df.query('entryType == "actual"').set_index('id')
        return df_budget, df_actual
        
    
    
    def create_engine(self, table:str, df:pd.DataFrame) -> int | None:
        """ Create the engine to connect to Postgres database and insert the dataframe lines

        Args:
            df (pd.DataFrame)

        Returns:
            int | None: Number os rows affected
        """                 
        engine = create_engine(
          f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}',
            connect_args={'options': '-csearch_path={}'.format(self.schema)}
        )        
        try:
            result = df.to_sql(table, engine, if_exists='replace')
            return result
        except ValueError:
            raise "Cannot insert into {}".format(table)
    
      