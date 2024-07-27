# flow.py
from prefect import flow
from task import generate_urls, download_and_process, save_urls_to_txt, save_all_csv, load_dataframes_from_csv, load_dataframe_to_postgres
from datetime import datetime
import os

@flow(name="First Run Flow")
def first_run_flow(start_ano: int, end_ano: int, meses: list, urls_base: list, file_txt: str):
    """
    Basicamente gera url para baixar o csv e xlsx
    Executa o fluxo do método first_run da classe Extract_data. 
    """
    urls = generate_urls(start_ano, end_ano, meses, urls_base)
    urls,all_data = download_and_process(urls)
    save_urls_to_txt(urls, file_txt)
    save_all_csv(all_data)
    return urls, all_data

@flow(name="Run Flow")
def run_flow(file_txt):
    """
    Executa o fluxo do método run da classe Extract_data.
    """
    # Recuperar dados de urls do arquivo txt
    with open(file_txt, 'r') as file:
        urls = [line.strip() for line in file.readlines()]
    urls, all_data = download_and_process(urls)
    save_all_csv(all_data)
    return urls, all_data

@flow(name="Loading Data to PostgreSQL")
def loading_to_postgres_flow():
    """
    Fluxo para carregar dados CSV em uma tabela PostgreSQL.
    """
    df_final = load_dataframes_from_csv()
    load_dataframe_to_postgres(df_final)


from prefect_dbt.cli.commands import DbtCoreOperation
@flow(name="DBT")
def dbt_process():
    """
    Criação de view seguindo arquitetura bronze,silver e gold
    """
    result = DbtCoreOperation(
        commands=["dbt build -t dev"],
        project_dir="dbt_prefeitura",
        profiles_dir="~/.dbt"
    ).run()
    return result

@flow(name="Att_Quadrimestral")
def att_quadrimestral():
    """
    executa att
    """
    # Obtém a data e hora atual
    now = datetime.now()
    # Formata o ano e o mês no formato desejado
    ano = now.strftime("%Y")
    mes = now.strftime("%m")
    url = rf"https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.xlsx"
    url, all_data = download_and_process([url])
    df = save_all_csv(all_data)
    load_dataframe_to_postgres(df)



if __name__ == "__main__":
    meses = ['01', '05', '09']
    urls_base = [
        'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados{ano}{mes}.xlsx',
        'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.xlsx',
        'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados{ano}{mes}.csv',
        'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.csv'
        ]
    start_ano = 2019
    end_ano = datetime.today().year
    file_txt = 'urls_tercerizados.txt'
    if os.path.exists(file_txt):
        run_flow(file_txt)
    else:
        first_run_flow(start_ano, end_ano, meses, urls_base,file_txt)

    loading_to_postgres_flow()
    dbt_process()