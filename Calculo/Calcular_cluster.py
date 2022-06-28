from ToolKitDB.Veiculo_legal import Veiculo_legal
import sqlite3
import pandas as pd
from Objetos.Tela import Tela
from ToolKitDB.Curvas_juros import Curvas_juros
import math
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Cea_globais import Cea_globais
from Capital.Capital import *
from ToolKitDB.Visao_carteira import Visao_carteira 
from ToolKitDB.Regua_pd import Regua_pd
from ToolKitDB.Matriz_migracao import Matriz_migracao
from ToolKitDB.Parametros import *
from ToolKitDB.Vetor_operacao_df import *
#from ToolKitDB.Recalibrar_cea import recalibrar_cea
from Objetos.Estoque_df import Estoque_df
from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Vetor_operacao_df import *
from ToolKitDB.MRC import MRC
from Objetos.Vertices_operacao import *
from Cluster.ConsolidarCluster import *
from Cluster.Tabela_cluster import *
import numpy as np
import json


def calcula_cluster(tela, meta, cluster):
    meta = json.loads(meta)   
    #tela = json.loads(tela) 
    
    connDB = sqlite3.connect("Bases/Banco_dados_att.db")
    conn_fpr = sqlite3.connect("Bases/fpr.db")

    indexador_garantia = tela["indexadorGarantia"]
    if tela["indexadorGarantia"] == "":
            indexador_garantia = tela["indexadorGarantia"] = "BRL"
    else:
            indexador_grantia = tela["indexadorGarantia"]  

    veiculo_legas = Veiculo_legal(connDB)
    curvas_juros = Curvas_juros(connDB,int(tela["prazo_total"]), tela["indexador"],indexador_garantia)
    tela = Tela(tela, connDB, conn_fpr)
    regua = Regua_pd(connDB)
    cea_globais = Cea_globais(connDB)
    matriz_migracao = Matriz_migracao(connDB, tela)
    moeda_conversao = Moeda_conversao(connDB, "BRL")
    alfas_betas = Alfas_betas(connDB,tela.get_indexador(), tela.get_indexador_garantia())
    produto_modalidade = Produto_modalidade(tela.get_id_segmento_risco_grupo(), tela.get_produto(), tela.get_modalidade(), connDB)
    tkf = Toolkit_finance(curvas_juros)
    visao_carteira = Visao_carteira(Tela.get_id_grupo(tela), connDB)
    operacoes = get_operacoes_df_cluster(tela.cnpj_cabeca, tela.volatilidade, tela, connDB)
    nova = gerar_nova_op_cluster(tela.get_id_grupo(), tela.get_lgd(), tela.get_modalidade(), tela.get_produto(), tela.get_cnpj_cabeca(), tela.get_rating_grupo(), tela.get_id_segmento_risco_grupo(), tela.get_id_segmento_risco_cabeca(), tela.get_lgd(), tela.get_notional(), tela.get_prazo_total(), tela.get_spread(), tela.get_volatilidade(), regua)
    estoque_df = Estoque_df(tela,produto_modalidade, moeda_conversao,tkf, matriz_migracao, regua,cea_globais, curvas_juros,operacoes,tela.cnpj_cabeca, tela.rating_grupo, tela.rating_operacao, tela.get_id_segmento_risco_grupo(),tela.id_grupo, tela.id_cenario, tela.volatilidade, tela.prazo_total)
    flag = cluster
    mrc = MRC(connDB)
    try:
        resultado = consolidar_cluster(connDB, curvas_juros, cea_globais, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legas, tkf, estoque_df, tela, nova, flag, operacoes, mrc, 1, 1)
    except:
            return {"EAD_CLUSTER": "-", "CEA_CLUSTER": "-", "MRC":"-", "PZR": "-", "LGD": "-", "NOME": "-"}
    return resultado