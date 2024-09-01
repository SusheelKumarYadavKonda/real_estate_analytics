import pandas as pd 
import boto3 
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

def process_and_transform_files(**kwargs):
    bucket_name = 'zillow-raw'
    output_bucket_name = 'zillow-staging'

    s3_hook = S3Hook(aws_conn_id='zillow-aws')
    s3 = s3_hook.get_conn()

    response = s3.list_objects_v2(Bucket=bucket_name)
    file_keys = [obj['Key'] for obj in response.get('Contents', [])]

    value_name_dict = {
                    'Metro_invt_fs_uc_sfrcondo_week.csv':'for_sale_inventory',
            'Metro_market_temp_index_uc_sfrcondo_month.csv': 'market_heat_index'
            ,'Metro_mean_days_to_close_uc_sfrcondo_month.csv': 'mean_days_to_close'
            ,'Metro_mean_doz_pending_uc_sfrcondo_sm_month.csv': 'mean_days_to_pending',
            'Metro_mean_listings_price_cut_amt_uc_sfrcondo_sm_month.csv':'mean_price_cuts',
            'Metro_mean_sale_to_list_uc_sfrcondo_month.csv': 'mean_sale_to_list',
            'Metro_median_days_to_close_uc_sfrcondo_month.csv':'median_days_to_close',
            'Metro_median_sale_price_uc_sfrcondo_month.csv':'median_sale_price',
            'Metro_median_sale_to_list_uc_sfrcondo_month.csv':'median_sale_to_list',
            'Metro_med_doz_pending_uc_sfrcondo_sm_month.csv' : 'median_days_pending',
            'Metro_med_listings_price_cut_amt_uc_sfrcondo_sm_month.csv': 'median_price_cuts',
            'Metro_mlp_uc_sfrcondo_month.csv': 'median_list_price',
            'Metro_new_con_mean_sale_price_uc_sfrcondo_month.csv': 'new_construction_mean_sale_price',
            'Metro_new_con_median_sale_price_per_sqft_uc_sfrcondo_month.csv': 'new_contstruction_median_sale_price',
            'Metro_new_con_median_sale_price_uc_sfrcondo_month.csv':'new_construction_median_sale_price',
            'Metro_new_con_sales_count_raw_uc_sfrcondo_month.csv': 'new_construction_sales_count',
            'Metro_new_listings_uc_sfrcondo_month.csv':'new_listings',
            'Metro_new_pending_uc_sfrcondo_month.csv':'new_pending',
            'Metro_pct_sold_above_list_uc_sfrcondo_month.csv':'pct_sold_above_list',
            'Metro_pct_sold_below_list_uc_sfrcondo_month.csv' : 'pct_sold_below_list', 
            'Metro_perc_listings_price_cut_uc_sfrcondo_sm_month.csv':'perc_listings_price_cut', 
            'Metro_sales_count_now_uc_sfrcondo_month.csv':'sales_count_now',
            'Metro_total_transaction_value_uc_sfrcondo_month.csv': 'total_transaction_value', 
            'Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv':'zhvf_growth', 
            'Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv':'zhvi', 
            'Metro_zordi_uc_sfrcondomfr_month.csv':'zordi',
            'Metro_zori_uc_sfrcondomfr_sm_sa_month.csv':'zori'
    }

    
    for file_key in file_keys:
        file_name = file_key.split('/')[-1]
        value_name = value_name_dict.get(file_name, 'value')  

        # Read the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))

        # Debug: Print the first few rows of the dataframe to ensure it loaded correctly
        print(f"Data loaded from {file_key}:")
        print(df.head())

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        # Filter out rows where regiontype is 'country'
        df = df[df['regiontype'] != 'country']

        # Identifier columns that we don't want to melt
        id_vars = ['regionid', 'sizerank', 'regionname', 'regiontype', 'statename']

        # Melt the DataFrame from wide to long format
        df_long = df.melt(id_vars=id_vars, var_name='date', value_name=value_name)

        # Optionally, convert 'date' column to datetime format (if applicable)
        df_long['date'] = pd.to_datetime(df_long['date'], format='%Y-%m-%d', errors='coerce')

        # Debug: Print the transformed dataframe to ensure it is correctly structured
        print(f"Transformed data from {file_key}:")
        print(df_long.head())

        # Save the transformed DataFrame as CSV back to S3
        csv_buffer = io.StringIO()
        df_long.to_csv(csv_buffer, index=False)

        # Debug: Ensure the buffer contains data
        csv_content = csv_buffer.getvalue()
        if not csv_content.strip():  # Check if the buffer is empty
            print(f"CSV buffer is empty for {file_key}, something went wrong.")
            continue

        print(f"CSV content for {file_key} (first 500 characters):")
        print(csv_content[:500])

        # Define the key for the transformed CSV file in the output S3 bucket
        csv_key = f'processed-data/{file_key.split("/")[-1].replace(".csv", "_transformed.csv")}'

        # Upload the transformed CSV file to the S3 bucket
        s3.put_object(Bucket=output_bucket_name, Key=csv_key, Body=csv_buffer.getvalue())

        print(f"Processed and saved {file_key} to {csv_key} in {output_bucket_name}")

def combine_files(**kwargs):
    bucket_name = 'zillow-staging'
    output_bucket_name = 'zillow-staging'
    output_key = 'combined-data/combined_file.parquet'

    s3_hook = S3Hook(aws_conn_id='zillow-aws')
    s3 = s3_hook.get_conn()

    # List all transformed CSV files in the staging S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='processed-data/')
    file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    combined_df_list = []

    # Loop through each file, read it, and append to the list
    for file_key in file_keys:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))

        # Debug: Check if DataFrame is empty and print its structure
        if df.empty:
            print(f"DataFrame from {file_key} is empty!")
        else:
            print(f"DataFrame from {file_key} loaded successfully with shape {df.shape}")

        # Print the first few rows of the DataFrame for verification
        print(f"First few rows of {file_key}:")
        print(df.head())

        # Append the DataFrame to the list
        combined_df_list.append(df)

    # Concatenate all DataFrames in the list into a single DataFrame
    if combined_df_list:
        combined_df = pd.concat(combined_df_list, axis=0, ignore_index=True)
    else:
        print("No files were found or no DataFrames were loaded.")
        return

    # Debug: Print the first few rows of the combined dataframe
    print("Combined DataFrame:")
    print(combined_df.head())

    # Save the combined DataFrame back to S3
    parquet_buffer = io.BytesIO()
    combined_df.to_parquet(parquet_buffer, index=False)
    s3.put_object(Bucket=output_bucket_name, Key=output_key, Body=parquet_buffer.getvalue())

    print(f"Combined Parquet file saved to {output_bucket_name}/{output_key}")




def load_data_into_redshift(**kwargs):
    redshift_hook = RedshiftSQLHook(redshift_conn_id='')
    copy_sql = """
        COPY your_table_name
        FROM 's3://zillow-staging/combined-data/'
        IAM_ROLE 'arn:aws:iam::590183781257:role/service-role/AmazonRedshift-CommandsAccessRole-20240831T204556'
        FORMAT AS PARQUET;
    """
    redshift_hook.run(copy_sql)