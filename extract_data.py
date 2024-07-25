#1min 28s
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
import csv
import re

class Extract_Urls:
    """
    Gera arquivo dataset_url.txt com as urls válidas além de gerar todas as tabelas.
    """

    def __init__(self) -> None:
            self.meses = ['01', '05', '09']
            self.urls_base = [
                'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados{ano}{mes}.xlsx',
                'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.xlsx',
                'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados{ano}{mes}.csv',
                'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados_{ano}{mes}.csv'
            ]
            self.start_ano = 2019
            self.end_ano = datetime.today().year
            self.file_txt = 'urls_tercerizados.txt'

    # talvez tirar (?) é só para pegar url(api)
    def first_run(self):
        urls_para_teste = self.__generate_urls(self.start_ano,self.end_ano,self.meses,self.urls_base)
        urls, dados = self.__download_and_process(urls_para_teste)
        self.__save_urls_to_txt(urls,self.file_txt)
        self.__save_all_csv(dados)

        return urls,dados
    
    def run(self):
        urls = self.__extract_url(self.file_txt)
        urls,all_data = self.__download_and_process(urls)
        self.__save_all_csv(all_data)
        return urls,all_data
    
    def __save_all_csv(self,all_data):
        """
        Salva tanto csv quanto xlsx
        """
        for pos in range(len(all_data['url'])):
            type_file = all_data['tipo'][pos]
            file_csv = all_data['url'][pos].split('.')[-2].split('/')[-1] + '.' + type_file
            data = all_data['data_content'][pos]
            if type_file == 'csv':
                self.__transform_bytes_to_csv(data,file_csv)
            elif type_file == 'xlsx':
                 self.__transform_bytes_to_xlsx(data,file_csv)     

    def __transform_bytes_to_csv(self,data,file_csv):
        """
        transform to utf-8 and save in csv
        """
        try:            
            data = data.decode('utf-8').splitlines()
        except UnicodeDecodeError:
            data = data.decode('latin-1').splitlines()

        with open(file_csv, "w") as data_to_insert:
            writer = csv.writer(data_to_insert, delimiter = '\t')
            for line in data:
                writer.writerow(re.split('\s+',line))


    def __transform_bytes_to_xlsx(self,data,file_xlsx):
        df = pd.read_excel(data)
        df.to_csv(file_xlsx,index=False)


    def __extract_url(self,file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
        lines = [line.strip() for line in lines]
        return lines


    def __save_urls_to_txt(self,urls, filename):
        with open(filename, 'w') as file:
            for url in urls:
                file.write(url + '\n')


    # Função para gerar URLs
    def __generate_urls(self,start_ano, end_ano, meses, urls_base):
        urls = []
        for ano in range(start_ano, end_ano + 1):
            for mes in meses:
                for url_base in urls_base:
                    url = url_base.format(ano=ano, mes=mes)
                    urls.append(url)
        return urls
    
    # Função para baixar e processar os arquivos
    def __download_and_process(self,urls):
        dados = {
            'url':[],
            'data_content':[],
            'tipo':[]
        }
        lista_urls = []
        for url in urls:
            try:
                response = requests.get(url)
                tipo = url.split(".")[-1]
                response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
                data = response.content
                # Detecta o tipo de arquivo com base na extensão e usa o leitor apropriado
                dados['url'].append(url)
                dados['data_content'].append(data)   
                dados['tipo'].append(tipo)
                lista_urls.append(url)
                       
            except Exception as e:
                print(f"Erro ao baixar ou processar o arquivo {url}: {e}")
        return lista_urls,dados
    

if __name__ == "__main__":
    script = Extract_Urls()
    script.run()