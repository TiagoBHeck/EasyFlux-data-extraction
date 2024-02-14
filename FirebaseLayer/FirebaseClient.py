""" Class for extracting data from Firebase

The referred class allows you to extract data from the Firebase collection and prepares 
the data structure in a dataframe for subsequent processing.

This script requires that `sqlalchemy`, `pandas` and `firebase_admin` be installed within the Python
environment you are running this script in.
"""

import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore


class FirebaseClient():
    """
      A class to create a connection instance with Firebase

      ...

      Attributes
      ----------
      cred : Any
          Firebase database credentials.
      initializer : Any
          Firebase admin module to init the application that connects to database.
      db : Any
          Firestore database object.
      
      Methods
      -------
      get_collection(collection:str)
          returns a list of entries retrieved from firestore database  
      format_dataframe(df:pd.DataFram)
          create the dataframe from the given list.  
    """
  
    def __init__(self):
        """__init__

        Args:
            
        """
        self.cred = credentials.Certificate({"service account key here"})
        self.initializer = firebase_admin.initialize_app(self.cred)
        self.db = firestore.client()


    def get_collection(self, collection: str) -> list:
        """Returns a list of entries retrieved from firestore database.

        Args:
            collection:str 

        Returns:
            list 
        """
        doc_list = []       
      
        docs = (
          self.db.collection(collection).stream()
        )         
        
        for item in docs:
            item_data = {}          
            item_data['id'] = item.id            
            item_data['date'] = item._data['date'].strftime("%m/%d/%Y, %H:%M:%S")    
            item_data['entryType'] = item._data['entryType']  
            item_data['category'] = item._data['category']  
            item_data['type'] = item._data['type']  
            item_data['period'] = item._data['period']   
            item_data['description'] = item._data['name']
            item_data['amount'] = item._data['amount']           
            doc_list.append(item_data)
        
        return doc_list       
       
      
      
    def format_dataframe(self, doc_list:list) -> pd.DataFrame:
        """Create the dataframe from the given list.

        Args:
            doc_list:list 

        Returns:
            pd.DataFrame
        """               
        return pd.DataFrame.from_dict(doc_list)
            

