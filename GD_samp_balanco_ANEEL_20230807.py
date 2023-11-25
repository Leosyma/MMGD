# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 08:45:10 2023

@author: 2018459
"""

#%% Bibliotecas
import pandas as pd
import numpy as np
import keyring
import cx_Oracle
import os
import glob
import math
import datetime as dt
import re
import wget


#%%Download do arquivo
#Site de onde serão extraídos os dados
url = "https://dadosabertos.aneel.gov.br/dataset/3193ebab-81b3-406e-be0e-f968a4a21689/resource/9f03a034-fb01-4daa-b6a6-e25a84d979ed/download/samp-balanco.csv"

#mydir
pasta_download = r'W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados'

#Função que limpa a pasta_download onde serão feitos os downloads
def limpar_pasta_download_download():
    filelist = [ f for f in os.listdir(pasta_download) if f.endswith(".csv") and f.startswith('samp') ]
    for f in filelist:
        os.remove(os.path.join(pasta_download, f))


def download_arquivo():
    wget.download(url,out = pasta_download )

# Roda as funções
limpar_pasta_download_download()
download_arquivo()


#%% Leitura dos arquivos
# Diretorio
pasta = r'W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados'
arquivo = r'samp-balanco.csv'

tabela_oracle = r'GD_SAMP_ANEEL'


# Leitura do arquivo
df = pd.read_csv(os.path.join(pasta,arquivo),sep=';'
                                            ,decimal=','
                                            ,encoding='ANSI'
                                            ,dtype={'NumCPFCNPJ':str})

df = df.astype('str')


# Tratamento e Limpeza dos dados
# Converte a coluna de data
df['DatGeracaoConjuntoDados'] = pd.to_datetime(df['DatGeracaoConjuntoDados'],yearfirst=True,infer_datetime_format=True).dt.strftime('%d-%m-%Y')


# Converte a coluna 'VlrEnergia' para int
df['VlrEnergia'] = df['VlrEnergia'].astype('int64')



#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df.values.tolist()


#Definir as variáveis para conexão no banco de dados
aplicacao_usuario = "USER_IRA"
aplicacao_senha = "BD_IRA"
aplicacao_dsn = "DSN"
usuario = "IRA"


#Definir conexão com o banco de dados     
try:
    connection = cx_Oracle.connect(user = keyring.get_password(aplicacao_usuario, usuario),
                                   password = keyring.get_password(aplicacao_senha,usuario),
                                   dsn= keyring.get_password(aplicacao_dsn, usuario),
                                   encoding="UTF-8")

#Se der erro na conexão com o banco, irá aparecer a mensagem abaixo
except Exception as err:
    print('Erro na Conexao:', err)    

#Se estiver tudo certo na conexão, irá aparecer a mensagem abaixo
else:
    print('Conexao com o Banco de Dados efetuada com sucesso. Versao da conexao: ' + connection.version)
    
    #O cursor abaixo irá executar o insert de cada uma das linhas da base editada no Banco de Dados Oracle
    try:
        cursor = connection.cursor()
        cursor.execute('''TRUNCATE TABLE ''' + tabela_oracle) #Limpar a tabela antes de executar o insert
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list,batcherrors=True)

    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()



