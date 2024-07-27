import prefect
from prefect import task, Flow
import requests
import pandas as pd

@task
def extract_data():
    url_teste = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados202405.xlsx"
    response = requests.get(url_teste)
    return response.content

@task
def pass_to_csv_and_save(data):
    df = pd.read_excel(data)
    df.to_excel('Testando.xlsx')
    return df
#Bronze Layer
with Flow("Extract_data") as flow:
    # Task
    data = extract_data()
    pass_to_csv_and_save(data)



if __name__ == '__main__':
    with Flow("Extract_data") as flow:
        # Task
        data = extract_data()
        pass_to_csv_and_save(data)
        flow.run()