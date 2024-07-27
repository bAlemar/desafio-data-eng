import os
import pandas as pd
import sys
import os

# Adiciona o diretório raiz do projeto ao PYTHONPATH

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from infra.db.repositories.tercerizados_repository import TercerizadosRepository


class LoadingToPosgre:
    """
    Load csv to postgres
    """
    def __init__(self) -> None:
        self.columns = ['id_terc', 'sg_orgao_sup_tabela_ug', 'cd_ug_gestora', 'nm_ug_tabela_ug',
            'sg_ug_gestora', 'nr_contrato', 'nr_cnpj', 'nm_razao_social', 'nr_cpf',
            'nm_terceirizado', 'nm_categoria_profissional', 'nm_escolaridade',
            'nr_jornada', 'nm_unidade_prestacao', 'vl_mensal_salario',
            'vl_mensal_custo', 'Num_Mes_Carga', 'Mes_Carga', 'Ano_Carga',
            'sg_orgao', 'nm_orgao', 'cd_orgao_siafi', 'cd_orgao_siape']
        self.path_csv = 'csv'
        self.repositorio = TercerizadosRepository()
    def loading_dataframe_to_postgres(self,df_final):
        self.repositorio.load_dataframe_to_postgres(df_final,'tercerizados')

    def extract_all_dataframes_and_join(self):
        lista_df = []
        for arquivo in os.listdir(self.path_csv):
            if arquivo.endswith('.csv'):
                try:
                    df = pd.read_csv(rf"{self.path_csv}/{arquivo}",sep=';',on_bad_lines='skip')
                    df.columns = self.columns #perda de dados (?)
                except:
                    df = pd.read_csv(rf"{self.path_csv}/{arquivo}",sep=',',on_bad_lines='skip')
                    df.columns = self.columns                
                lista_df.append(df)
        df_final = pd.concat(lista_df)
        return df_final
    