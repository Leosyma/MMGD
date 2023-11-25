# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 16:32:24 2023

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

#%% Dados de entrada
#Origem dos dados

# Caminho referencia
pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados\samp\*"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
arquivos = glob.glob(pasta)


#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'GD_SAMP_ANEEL'


#%% Leitura dos arquivos e consolidação
df_samp = pd.DataFrame()
for arquivo in arquivos:
    df = pd.read_csv(arquivo,sep=';',decimal=',',encoding='ANSI',dtype={'NumCNPJAgenteDistribuidora':'str'})
    df_samp = pd.concat([df_samp,df])




#%% Tratamento dos dados
df_samp = df_samp.astype('str')
for coluna in df_samp.columns:
    df_samp[coluna] = df_samp[coluna].replace('nan',None)
    
    
df_samp['VlrMercado'] = df_samp['VlrMercado'].astype('float').replace('.',',')


#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_samp.values.tolist()


#Definir as variáveis para conexão no banco de dados
aplicacao_usuario = "USER_IRA"
aplicacao_senha = "BD_IRA"
aplicacao_dsn = "DSN"
usuario = "IRA"

# Fazemos o insert em blocos, pois temos muitas linhas
start_pos = 0
batch_size = 50000

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
        cursor.execute('''TRUNCATE TABLE ''' + tabela_oracle) #Limpa os dados da tabela
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        i = 0
        while start_pos < len(dados_list):
            data = dados_list[start_pos:start_pos + batch_size]
            start_pos += batch_size
            cursor.executemany(sql, data) 
            connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
            i += 1
            print(i)
        
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()






