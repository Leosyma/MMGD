# -*- coding: utf-8 -*-
"""
Created on Thu May 18 15:07:28 2023

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
pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\Projeto GD"


#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
arquivo ='PCAT_TA_Aplicacao_concessao_consolidado.xlsx'


#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'GD_PCAT'


#%% Leitura do arquivo
df_gd_pcat = pd.read_excel(os.path.join(pasta,arquivo),header = 1)
    



#%%Tratamento e Limpeza dos Dados
colunas_float = ["Total TUSD","Total TE","TUSD_CDE_COVID","TUSD_RGR","TUSD_TFSEE","TUSD_PeD","TUSD_ONS","TUSD_CCC","TUSD_CDE","TUSD_PROINFA","Liminar1","TUSD_RB","TUSD_FR","TUSD_CCT","TUSD_CCD","TUSD_CUSD","TUSDG_T","TUSDG_ONS","TUSD_FioB","TUSD_SUBSIDIO","TUSD_OUTROS","TUSD_PT","TUSD_Per_RB/D","TUSD_PNT","TUSD_RI","TE_CDE_COVID","TE_PeD","TE_ESSERR","TE_CFURH","TE_CDE_E","TE_Energia","TE_TranItaipu","TE_TUST_ITAIPU","TE_TUST_CI","TE_SUBSIDIO","TE_Per_RB","TUSD Encargos","TUSD Fio A","TUSD Fio B","TUSD Perdas","TE Encargos","TE Transporte","TE Perdas","TE Energia","TUSD_Subvenção <350","TE_Subvenção <350"]
df_gd_pcat = df_gd_pcat.astype('str')
for coluna_float in colunas_float:
    df_gd_pcat[coluna_float] = df_gd_pcat[coluna_float].replace('nan',0).astype('float')
    

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_gd_pcat.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)

    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()

   





