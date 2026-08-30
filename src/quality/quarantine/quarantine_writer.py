from pyspark.sql import DataFrame

def write_quarantine_dataframe(dataframe:DataFrame,output_path:str)->None:
    (
        dataframe.write
        .format("delta")
        .mode("append")
        .save(output_path)
    )