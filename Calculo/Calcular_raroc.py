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
from Objetos.Estoque_df import Estoque_df
from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Vetor_operacao_df import *
from Raroc.Consolidar_pnl import * 
from Objetos.Vertices_operacao import *
import numpy as np
import json

def calcula_raroc(tela, meta):
        

        meta = json.loads(meta)   
        connDB = sqlite3.connect("Bases/Banco_dados_att.db")
        conn_fpr = sqlite3.connect("Bases/fpr.db")    
        #conn_seguro = sqlite3.connect("Bases/base_seguro.db")

        indexador_garantia = tela["indexadorGarantia"]
        if (tela["indexadorGarantia"] == "" or tela["indexadorGarantia"] == "CDI") :
                indexador_garantia = tela["indexadorGarantia"] = "BRL"
        else:
                indexador_grantia = tela["indexadorGarantia"]  

        veiculo_legas = Veiculo_legal(connDB)
        curvas_juros = Curvas_juros(connDB,int(tela["prazo_total"]), tela["indexador"],indexador_garantia)
        tela = Tela(tela, connDB, conn_fpr)
        regua = Regua_pd(connDB)
        cea_globais = Cea_globais(connDB)
        matriz_migracao = Matriz_migracao(connDB, tela)
        moeda_conversao = Moeda_conversao(connDB, Tela.get_indexador(tela))
        alfas_betas = Alfas_betas(connDB,tela.get_indexador(), tela.get_indexador_garantia())
        produto_modalidade = Produto_modalidade(tela.get_id_segmento_risco_grupo(), tela.get_produto(), tela.get_modalidade(), connDB)
        tkf = Toolkit_finance(curvas_juros)
        visao_carteira = Visao_carteira(Tela.get_id_grupo(tela), connDB)
        operacoes = get_operacoes_df(tela.cnpj_cabeca, tela.volatilidade, tela, connDB)
        operacoes_vazio = get_operacoes_vazio(connDB)
        nova = gerar_nova_op(tela.get_id_grupo(), tela.get_lgd(), tela.get_modalidade(), tela.get_produto(), tela.get_cnpj_cabeca(), tela.get_rating_grupo(), tela.get_id_segmento_risco_grupo(), tela.get_id_segmento_risco_cabeca(), tela.get_lgd_mitigada(), tela.get_notional(), tela.get_prazo_total(), tela.get_spread(), tela.get_volatilidade(), regua)
        estoque_df = Estoque_df(tela, produto_modalidade, moeda_conversao,tkf, matriz_migracao, regua,cea_globais, curvas_juros,operacoes,tela.cnpj_cabeca, tela.rating_grupo, tela.rating_operacao, tela.get_id_segmento_risco_grupo(),tela.id_grupo, tela.id_cenario, tela.volatilidade, tela.prazo_total)
        estoque_vazio = Estoque_df(tela, produto_modalidade, moeda_conversao,tkf, matriz_migracao, regua,cea_globais, curvas_juros,operacoes_vazio,tela.cnpj_cabeca, tela.rating_grupo, tela.rating_operacao, tela.get_id_segmento_risco_grupo(),tela.id_grupo, tela.id_cenario, tela.volatilidade, tela.prazo_total)
        resultado = consolidar_pnl(connDB, nova, estoque_df, estoque_vazio, alfas_betas, produto_modalidade, meta, tela, tela.get_volatilidade(), matriz_migracao, regua, cea_globais, curvas_juros, tkf, moeda_conversao, veiculo_legas, visao_carteira, operacoes, operacoes_vazio)
        return resultado