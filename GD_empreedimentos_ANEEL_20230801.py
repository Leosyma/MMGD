# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
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
url = "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"

#mydir
pasta_download = r'C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\Projeto GD\Dados'

#Função que limpa a pasta_download onde serão feitos os downloads
def limpar_pasta_download_download():
    filelist = [ f for f in os.listdir(pasta_download) if f.endswith(".csv") and f.startswith('empreendimento') ]
    for f in filelist:
        os.remove(os.path.join(pasta_download, f))


def download_arquivo():
    response = wget.download(url,out = pasta_download )

# Roda as funções
limpar_pasta_download_download()
download_arquivo()

#%% Leitura dos arquivos
# Diretorio
pasta = r'W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados'
arquivo = r'empreendimento-geracao-distribuida.csv'

tabela_oracle = r'GD_EMPREENDIMENTOS_ANEEL'


# Leitura do arquivo
df = pd.read_csv(os.path.join(pasta,arquivo),sep=';'
                                            ,decimal=','
                                            ,encoding='ANSI'
                                            ,dtype={'NumCNPJDistribuidora':str}
                                            ,low_memory=False)

df_dist = pd.read_excel(r'W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\MMGD\Dados\CODIGOS_DIST_ATUAL.xlsx',dtype={'CNPJ_AGENTE':str})


#Converte para string
df = df.astype('str')
df_dist = df_dist.astype('str')


# Tratamento e Limpeza dos Dados
colunas_str = ['DatGeracaoConjuntoDados','AnmPeriodoReferencia','NumCNPJDistribuidora','SigAgente','NomAgente','DscClasseConsumo','DscSubGrupoTarifario','codUFibge','SigUF','codRegiao','NomRegiao','NomMunicipio','CodCEP','SigTipoConsumidor','NumCPFCNPJ','NomeTitularEmpreendimento','CodEmpreendimento','DthAtualizaCadastralEmpreend','SigModalidadeEmpreendimento','DscModalidadeHabilitado','SigTipoGeracao','DscFonteGeracao','DscPorte','NomSubEstacao']


# Converte as colunas para string
for coluna_str in colunas_str:
    df[coluna_str] = df[coluna_str].replace('nan',None)
df['NomSubEstacao'] = df['NomSubEstacao'].replace('nan','-')
    
    
# Converte as colunas para int
df['CodClasseConsumo'] = df['CodClasseConsumo'].replace('nan',0).replace('<NA>',0).astype('int').replace(0,np.nan)
df['CodSubGrupoTarifario'] = df['CodSubGrupoTarifario'].replace('nan',0).replace('<NA>',0).astype('int').replace(0,np.nan)
df['CodMunicipioIbge'] = pd.to_numeric(df['CodMunicipioIbge'],errors='coerce',downcast='integer').replace(0,None).replace(np.nan,None)
df['NumCNPJDistribuidora'] = df['NumCNPJDistribuidora'].replace('0',None).replace(np.nan,None)
df['codRegiao'] = pd.to_numeric(df['codRegiao'],errors='coerce',downcast='integer').replace(0,None).replace(np.nan,None)   
df['codUFibge'] = pd.to_numeric(df['codUFibge'],errors='coerce',downcast='integer').replace(0,None).replace(np.nan,None) 
        
      
# Converte as colunas para float
df['QtdUCRecebeCredito'] = pd.to_numeric(df['QtdUCRecebeCredito'],errors='coerce',downcast='float').replace(np.nan,None)
df['MdaPotenciaInstaladaKW'] = pd.to_numeric(df['MdaPotenciaInstaladaKW'],errors='coerce',downcast='float').replace(np.nan,None).replace('.',',')
df['NumCoordNEmpreendimento'] = df['NumCoordNEmpreendimento'].replace('nan',0)
df['NumCoordNEmpreendimento'] = pd.to_numeric(df['NumCoordNEmpreendimento'],errors='coerce',downcast='float').replace('nan',None).replace('nan',np.nan).replace(np.nan,None).replace('.',',')
df['NumCoordEEmpreendimento'] = df['NumCoordEEmpreendimento'].replace('nan',0)
df['NumCoordEEmpreendimento'] = pd.to_numeric(df['NumCoordEEmpreendimento'],errors='coerce',downcast='float').replace('nan',None).replace('nan',np.nan).replace(np.nan,None).replace('.',',')
df['NumCoordESub'] = df['NumCoordESub'].replace('nan',0)
df['NumCoordESub'] = pd.to_numeric(df['NumCoordESub'],errors='coerce',downcast='float').replace('nan',None).replace('nan',np.nan).replace(np.nan,None).replace('.',',')
df['NumCoordNSub'] = df['NumCoordNSub'].replace('nan',0)
df['NumCoordNSub'] = pd.to_numeric(df['NumCoordNSub'],errors='coerce',downcast='float').replace('nan',None).replace('nan',np.nan).replace(np.nan,None).replace('.',',')



#Cruzo os CNPJ dos dois dataframe para trazer o código da Distribuidora
df = df.merge(df_dist,how='left',left_on='NumCNPJDistribuidora',right_on='CNPJ_AGENTE')

#Dropo as colunas que não importam
df = df.drop(columns=['CNPJ_AGENTE','AGENTE','NOME_AGENTE'])

df = df.reindex(columns=['DIST','DatGeracaoConjuntoDados','AnmPeriodoReferencia','NumCNPJDistribuidora','SigAgente','NomAgente','CodClasseConsumo','DscClasseConsumo','CodSubGrupoTarifario','DscSubGrupoTarifario','codUFibge','SigUF','codRegiao','NomRegiao','CodMunicipioIbge','NomMunicipio','CodCEP','SigTipoConsumidor','NumCPFCNPJ','NomeTitularEmpreendimento','CodEmpreendimento','DthAtualizaCadastralEmpreend','SigModalidadeEmpreendimento','DscModalidadeHabilitado','QtdUCRecebeCredito','SigTipoGeracao','DscFonteGeracao','DscPorte','MdaPotenciaInstaladaKW','NumCoordNEmpreendimento','NumCoordEEmpreendimento','NomSubEstacao','NumCoordESub','NumCoordNSub'])



df['DIST'] = df['DIST'].astype('str')




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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list,batcherrors=True)

    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()
    





