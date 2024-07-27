
from datetime import datetime
from extract_data import Extract_data
import requests
import pandas as pd

class Att_Quadrimestral:

    def __init__(self) -> None:
        pass
    
    def run(self):
        """
        Assumindo que api vai manter um padrão
        """
        # Obtém a data e hora atual
        now = datetime.now()

        # Formata o ano e o mês no formato desejado
        ano = now.strftime("%Y")
        mes = now.strftime("%m")
        url = rf"https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.xlsx"
        dados = self.download_and_process(url)
        self.transform_bytes_to_xlsx_and_save(dados)

    
    def download_and_process(self,url):
        dados = {
            'url':[],
            'data_content':[],
            'tipo':[]
        }
        response = requests.get(url)
        tipo = url.split(".")[-1]
        response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
        data = response.content
        # Detecta o tipo de arquivo com base na extensão e usa o leitor apropriado
        dados['url'].append(url)
        dados['data_content'].append(data)   
        dados['tipo'].append(tipo)
        return dados
     
    def transform_bytes_to_xlsx_and_save(self,data,file_xlsx):
        df = pd.read_excel(data)
        df.to_csv(file_xlsx,index=False)