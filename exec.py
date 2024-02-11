from StagingLayer.InsertStaging import InsertStaging

exec = InsertStaging()

if __name__ == '__main__': 
    try:
        df = exec.get_easyflux_data()
        df_budget, df_actual = exec.split_budget_and_actual(df=df)
        exec.insert_to_staging(df_budget=df_budget, df_actual=df_actual)
    except Exception:
      raise "a problem occurred while executing the insertions"