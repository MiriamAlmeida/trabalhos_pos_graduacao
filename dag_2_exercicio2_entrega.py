import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime


default_args = {
    'owner': "Miriam",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_2_exercicio2_entrega():
    #Ler a tabela única de indicadores feitos na Dag1 (/tmp/tabela_unica.csv)
    #Produzir médias para cada indicador considerando o total
    #Printar a tabela nos logs
    #Escrever o resultado em um arquivo csv local no container (/tmp/resultados.csv)
    @task
    def media_final():
        NOME_DO_ARQUIVO_FINAL = '/tmp/resultados.csv'
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=';')
        result = df.agg({'PassengerId': 'mean', 'Fare': 'mean', 'Family': 'mean'}).reset_index()
        print(result)
        return NOME_DO_ARQUIVO_FINAL

    media = media_final()
    media

execucao = dag_2_exercicio2_entrega()

