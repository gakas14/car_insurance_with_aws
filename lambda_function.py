import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Hard-coded AWS Settings
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating DF from content
        df_raw = wr.s3.read_csv('s3://{}/{}'.format(bucket, key))

        # Function to remove 'z_' from columns
        def clean_z_(df_raw):
            df_raw['married']= df_raw['married'].str.replace('z_', '')
            df_raw['gender'] = df_raw['gender'].str.replace('z_', '')
            df_raw['education']= df_raw['education'].str.replace('z_', '')
            df_raw['occupation'] = df_raw['occupation'].str.replace('z_', '')
            df_raw['car_type'] = df_raw['car_type'].str.replace('z_', '')
            df_raw['urbanicity'] = df_raw['urbanicity'].str.replace('z_', '')
            return df_raw
        
        # Function to remove the '$'
        def remove_convert(df_raw):
            df_raw['income'] = df_raw['income'].str.replace('$','')
            df_raw['home_value'] = df_raw['home_value'].str.replace('$','')
            df_raw['old_claim'] = df_raw['old_claim'].str.replace('$','')
            df_raw['claim_amount'] = df_raw['claim_amount'].str.replace('$','')
    
            df_raw['income'] = df_raw['income'].astype('float')
            df_raw['home_value'] = df_raw['home_value'].astype('float')
            df_raw['old_claim'] = df_raw['old_claim'].astype('float')
            df_raw['claim_amount'] = df_raw['claim_amount'].astype('float')
    
            df_raw = df_raw[df_raw['age']> 0]
            return df_raw
        
        # Function to fill missing values
        def remove_nan(df_raw):
            df_raw.occupation = df_raw.occupation.fillna('No Occupation')
            df_raw = df_raw.fillna(0)
            return df_raw
            
        clean1 = remove_convert(df_raw)
        data = clean_z_(clean1)
        data = remove_nan(data)
        
        
        # Write to S3
        wr_response = wr.s3.to_parquet(
            #df=df_step_1,
            df=data,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation,
            
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise 
