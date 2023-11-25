# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 11:19:57 2023

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

#%% Leitura dos arquivos
# Diretorio
pasta = r'W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados'
arquivo = r'QTDE UCs por munipio e concessão - BDGD 2021.xlsx'

tabela_oracle = r'GD_UCS_POR_MUNICIPIO'

#Leitura do arquivo
df = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Total UCs por municipio - BDGD')

#Conversão dos dados
df = df.astype('str')
df['QTDE'] = df['QTDE'].astype('int')


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list,batcherrors=True)

    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()
    





