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
        self.path_csv = 'teste'
        self.repositorio = TercerizadosRepository()
    def run(self):
        df_final = self.loading_joing_all_dataframes()
        print('len_df_final',len(df_final))
        self.repositorio.load_dataframe_to_postgres(df_final,'tercerizados')

    def loading_joing_all_dataframes(self):
        lista_df = []
        for arquivo in os.listdir(self.path_csv):
            if arquivo.endswith('.csv'):
                df = pd.read_csv(rf"{self.path_csv}/{arquivo}",sep=';',on_bad_lines='skip')
                df.columns = self.columns #perda de dados (?)
                lista_df.append(df)
        df_final = pd.concat(lista_df)
        return df_final
    
if __name__ == "__main__":
    script = LoadingToPosgre()
    script.run()