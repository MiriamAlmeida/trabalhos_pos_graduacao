import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"
default_args = {
    'owner': "Miriam",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_1_exercicio2_entrega():

    #Ler os dados e escrever localmente dentro do container numa pasta /tmp
    @task
    def ingestao(url):
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(url, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    #Quantidade de passageiros por sexo e classe (produzir e escrever)
    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA1 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "PassengerId": "count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA1, index=False, sep=";")
        return NOME_TABELA1

    
    #Preço médio da tarifa pago por sexo e classe (produzir e escrever)
    @task
    def preco_medio_tarifa(nome_do_arquivo):
        NOME_TABELA2 = "/tmp/preco_medio_tarifa.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "Fare": "mean"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    #Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
    @task
    def qtd_sibsp_parch(nome_do_arquivo):
        NOME_TABELA3 = "/tmp/qtd_sibsp_parch_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['Family'] = df['SibSp'] +  df['Parch']
        res = df.groupby(['Sex', 'Pclass'])['Family'].sum().reset_index()
        print(res)
        res.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3
   

    #Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
    @task
    def uniao_tabelas(nome_arquivo_1, nome_arquivo_2, nome_arquivo_3):
        NOME_FINAL = "/tmp/tabela_unica.csv"
        df_arq_1 = pd.read_csv(nome_arquivo_1,sep=";")
        df_arq_2 = pd.read_csv(nome_arquivo_2,sep=";")
        df_arq_3 = pd.read_csv(nome_arquivo_3,sep=";")
        result = df_arq_1.merge(df_arq_2, how='inner', on=['Sex', 'Pclass'])
        result = result.merge(df_arq_3, how='inner', on=['Sex', 'Pclass'])
        print(result)
        result.to_csv(NOME_FINAL, index=False, sep=";")
        return NOME_FINAL

  
    ingdadourl = ingestao(URL)
    ind_pass =  ind_passageiros(ingdadourl)
    ind_preco_medio =  preco_medio_tarifa(ingdadourl)
    ind_qtd_sibsp_parch =  qtd_sibsp_parch(ingdadourl)
    uniao = uniao_tabelas(ind_pass,  ind_preco_medio,  ind_qtd_sibsp_parch)

    triggerdag = TriggerDagRunOperator(
        task_id="chama_dag_2",
        trigger_dag_id="dag_2_exercicio2_entrega"
    )
    
    uniao >> triggerdag
   
execucao = dag_1_exercicio2_entrega()