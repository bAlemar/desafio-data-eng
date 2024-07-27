from prefect import task
import pandas as pd
import requests
import csv
import re
import os
from utils.extract_data import Extract_data
from utils.loading_to_postgre import LoadingToPosgre
from utils.att_data import Att_Quadrimestral
from datetime import datetime
extract_data = Extract_data()

@task
def generate_urls(start_ano, end_ano, meses, urls_base):
    return extract_data.generate_urls(start_ano, end_ano, meses, urls_base)

@task
def download_and_process(urls):
    return extract_data.download_and_process(urls)

@task
def save_urls_to_txt(urls, filename):
    extract_data.save_urls_to_txt(urls, filename)

@task
def save_all_csv(all_data):
    return extract_data.save_all_csv(all_data)


loading_to_postgres = LoadingToPosgre()
@task
def load_dataframes_from_csv():
    return loading_to_postgres.extract_all_dataframes_and_join()

@task
def load_dataframe_to_postgres(df):
    loading_to_postgres.loading_dataframe_to_postgres(df)


# att = Att_Quadrimestral()

# @task
# def get_csv_from_url():
#     dados = att.download_and_process(url)
#     att.transform_bytes_to_xlsx_and_save()

# @task
# def save_new_data_in_db():
#     att.
#     pass