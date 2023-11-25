# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 10:30:53 2023

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
import unidecode


#%% Leitura do arquivo
# Diretorio
pasta = r'C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\Projeto GD\Dados'
arquivo = r'subsidios-tarifarios.csv'

tabela_oracle = 'GD_SUBSIDIOS_ANEEL'

# Leitura arquivo
df = pd.read_csv(os.path.join(pasta,arquivo),sep=';'
                                            ,decimal=','
                                            ,encoding='ANSI'
                                            ,low_memory=False)


#%% Tratamento e Limpeza dos dados
# Colunas String
colunas_str = ['NumCPFCNPJ','SigAgente','NomAgente','DscTipoMontante','DscTipoSubsidio','DscAtoNormativo','NumAto']

# Colunas Float
colunas_float=['VlrSubsidio']

# Colunas Data
colunas_data=['DatGeracaoConjuntoDados','DatSubsidio','DatAssinaturaAto','DatInicioVigenciaAto','DatFimVigenciaAto','DthPublicacaoAto']

# Conversão dos dados
df = df.astype('str')
# String
for coluna_str in colunas_str:
    df[coluna_str] = df[coluna_str].replace('nan','')
    
# Float
for coluna_float in colunas_float:
    df[coluna_float] = df[coluna_float].replace('nan',0).astype('float').replace('.',',')

# Data
for coluna_data in colunas_data:
    df[coluna_data] = pd.to_datetime(df[coluna_data],yearfirst=True,infer_datetime_format=True).dt.strftime('%d-%m-%Y')



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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)

    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()

   





