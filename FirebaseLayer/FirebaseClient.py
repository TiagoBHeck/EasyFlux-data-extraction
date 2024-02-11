import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore


class FirebaseClient():
  
    def __init__(self):
        self.cred = credentials.Certificate(r"ServiceAccount\pl-easyflux-35b6b-firebase-adminsdk-hr3mq-5dadc4259e.json")
        self.initializer = firebase_admin.initialize_app(self.cred)
        self.db = firestore.client()


    def get_collection(self, collection: str) -> list:
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
       
      
      
    def format_dataframe(self, doc_list:list) -> list:
               
        return pd.DataFrame.from_dict(doc_list)
            

