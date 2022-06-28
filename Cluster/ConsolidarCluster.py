from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Curvas_juros import Curvas_juros
from ToolKitDB.Veiculo_legal import Veiculo_legal
from Capital.Capital import *
from ToolKitDB.Cea_globais import Cea_globais
from ToolKitDB.Regua_pd import Regua_pd
from ToolKitDB.Recalibrar_cea import *
from ToolKitDB.Parametros import *
from ToolKitDB.Produto_modalidade import Produto_modalidade
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Matriz_migracao import Matriz_migracao
from ToolKitDB.Moeda_conversao import Moeda_conversao
from Objetos.Estoque_df import Estoque_df
from Objetos.Vertices_operacao import Vertice_operacao
from Capital.Capital import *
from ToolKitDB.Vetor_operacao_df import *
from ToolKitDB.Explodir_fluxo_vertices import *
from Cluster.Tabela_cluster import *
import json
import sqlite3
import math
import time
import numpy as np
import pandas as pandas

def consolidar_cluster(connDB, curvas_juros, cea_globais, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance ,estoque_grupo, tela, nova_operacao, flag, operacoes_grupo, mrc, delta_lgd, delta_prazo):
    
    #INICIAR VARIAVEIS
    cronograma_fluxo = Tela.get_fluxo(tela)

    if(flag == 2): 
        Estoque_df.ajustar_lgd(estoque_grupo, delta_lgd)
        Estoque_df.ajustar_prazo(estoque_grupo, delta_prazo)
        vertices = explodir_fluxo_vertice_mes(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_grupo, tela, operacoes_grupo)
        df_ead_novo = Estoque_df.gerar_df_ead_novo(estoque_grupo, vertices, Tela.get_indexador(tela))
        df_setup = Estoque_df.get_setup_raroc_cluster(estoque_grupo, vertices, nova_operacao, df_ead_novo)
        estoque_completo_array = Estoque_df.gerar_estoque_completo_cluster(estoque_grupo, nova_operacao, df_ead_novo, vertices, df_setup[1], df_setup[0], flag)
        estoque_completo = estoque_completo_array[0]
        estoque_pe = estoque_completo_array[1]
    
    is_corp = (Tela.get_quebra(tela) == "CORP")

    [Vertice_operacao.calcula_capital_cluster(vertice, estoque_completo) for vertice in vertices]
    [Vertice_operacao.calcula_pe_cluster(vertice, estoque_pe) for vertice in vertices]
        
    #Inicializar variaveis     
    prazo_total = Tela.get_prazo_total(tela)

    remuneracao_cea = 0
    custo_n2_cea = 0

    despesas_tributarias_cea = 0
    custo_full = 0

    ead_cluster = 0
    cea_cluster = 0
    kreg_cluster = 0
    mfb_estoque = 0
    mfb_x_sell = 0
    pdd_estoque = 0
    ir_estoque = 0
    jcp_estoque = 0
    rgo_estoque = 0
    kreg_estoque = 0

    for i in range(len(vertices)):
        ead_cluster += vertices[i].ead_cluster * Vertice_operacao.get_dias_ate_proximo_vertice(vertices[i]) * Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i]))
        pdd_estoque += vertices[i].pdd_estoque * Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i]))

        if(not(Vertice_operacao.is_termination_vertex(vertices[i]))):

            cea_cluster += vertices[i].cea_cluster * Vertice_operacao.get_dias_ate_proximo_vertice(vertices[i]) * Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i])) / (Moeda_conversao.get_spot(moeda_conversao) * Toolkit_finance.forward_curency_dc(toolkit_finance, "BRL", vertices[i].indexador, Vertice_operacao.get_prazo_decorrido(vertices[i])))
            kreg_estoque += vertices[i].kreg_estoque* Vertice_operacao.get_dias_ate_proximo_vertice(vertices[i]) * Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i])) / (Moeda_conversao.get_spot(moeda_conversao) * Toolkit_finance.forward_curency_dc(toolkit_finance, "BRL", vertices[i].indexador, Vertice_operacao.get_prazo_decorrido(vertices[i])))
        
        if (not(Vertice_operacao.is_genesis_vertex(vertices[i]))): 

            mfb_estoque += vertices[i].mfb_estoque * Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i]))
            mfb_x_sell += vertices[i].mfb_x_sell *Toolkit_finance.fator_pv_dc(toolkit_finance, Tela.get_indexador(tela), Vertice_operacao.get_prazo_decorrido(vertices[i]))

        
    cea_estoque = (cea_cluster * 2.1 * 4.5 / 12) + (kreg_estoque * 7.5 / 12)
    perc_icea = cea_estoque / ead_cluster
    perc_kreg = kreg_estoque / ead_cluster
    remuneracao_cea = Toolkit_finance.get_rem_cea(toolkit_finance, cea_estoque, prazo_total)
    custo_n2_cea = Toolkit_finance.get_custo_n2(toolkit_finance, cea_estoque, prazo_total)
    despesas_tributarias_cea = (mfb_estoque +(remuneracao_cea - custo_n2_cea)) * Tela.get_aliquota_pis_cofins(tela)
    custo_full = get_ie_cred() * ((mfb_estoque) - ((mfb_estoque) * (Tela.get_aliquota_pis_cofins(tela))))
    custo_x_sell = mfb_x_sell * get_ie_x_sell()
    lair_cea_estoque = (mfb_estoque + mfb_x_sell + remuneracao_cea - custo_n2_cea - despesas_tributarias_cea - custo_full - custo_x_sell - pdd_estoque);
    ir_estoque = lair_cea_estoque * Veiculo_legal.get_aliquota_ir_cs(veiculo_legal, Tela.get_id_veiculo_legal(tela))
    jcp_estoque = Toolkit_finance.get_jcp(toolkit_finance, cea_estoque, prazo_total)
    rgo_estoque = lair_cea_estoque - ir_estoque + jcp_estoque
    raroc_estoque = pow(1 + (rgo_estoque / cea_estoque), 360) - 1

    #variaveis aux
    prz = vertices[0].prz_estoque
    lgd = vertices[0].lgd_estoque
    spread_x_sell = get_spread_x_sell()
    ie_x_sell = get_ie_x_sell()
    ie_cred = get_ie_cred()
    valor_mrc = cea_cluster / ead_cluster
    nome = tabela_cluster(valor_mrc, mrc, tela, Tela.get_rating_grupo(tela), is_corp)
    
    #FORMATO DE RESPOSTA PADRAO
    #resultado = {

            #"EAD_CLUSTER": round(vertices[0].ead_cluster),
            #"CEA_CLUSTER": round(vertices[0].cea_cluster),
            #"MRC": cea_cluster / ead_cluster,
            #"PZR": prz,
            #"LGD": lgd,
            #"NOME": nome,                   #calculaCluster(cea_cluster / ead_cluster, Tela.getRatingOperacao(tela), isCorp)
            #"IRAROC": raroc_estoque,
            #"RGO": rgo_estoque,
            #"CUSTO_CRED": custo_full,
            #"CUSTO_X_SELL": custo_x_sell,
            #"MFB_CRED": mfb_estoque,
            #"MFB_X_SELL": mfb_x_sell,
            #"SPREAD_X_SELL": spread_x_sell,
            #"IE_X_SELL": ie_x_sell,
            #"IE_CRED": ie_cred,
            #"PERC_ICEA": perc_icea,
            #"PERC_KREG": perc_kreg
        #}
    
    resultado = {"EAD_CLUSTER": round(vertices[0].ead_cluster), "CEA_CLUSTER": round(vertices[0].cea_cluster), "MRC": cea_cluster / ead_cluster, "PZR": prz, "LGD": lgd, "NOME": nome}
    return resultado

        
