from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Curvas_juros import Curvas_juros
from Capital.Capital import *
from ToolKitDB.CVA import *
from ToolKitDB.FEPF import *
from ToolKitDB.Cea_globais import Cea_globais
from ToolKitDB.Regua_pd import Regua_pd
from ToolKitDB.Parametros import *
from ToolKitDB.Produto_modalidade import Produto_modalidade
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Matriz_migracao import Matriz_migracao
from ToolKitDB.Moeda_conversao import Moeda_conversao
from Capital.Capital import *
from ToolKitDB.Vetor_operacao_df import *
from ToolKitDB.FPR import *

import sqlite3
import math
import time
import numpy as np
import pandas as pandas
pandas.options.mode.chained_assignment = None  # default='warn'
import timeit

class Estoque_df:
    def __init__(self, tela, produto_modalidade, moeda_conversao, toolkit_finance,matriz_migracao, regua_pd, cea_globais, curvas_juros, estoque_grupo, cnpj_cabeca, rating_grupo, rating_operacao, id_segmento, id_grupo, id_cenario_migracao_global, id_volatilidade, prazo):   
        
        self.tela = tela
        self.produto_modalidade = produto_modalidade
        self.moeda_conversao = moeda_conversao
        self.toolkit_finance = toolkit_finance
        self.matriz_migracao = matriz_migracao
        self.regua_pd = regua_pd
        self.cea_globais = cea_globais
        self.curvas_juros = curvas_juros
        self.estoque_grupo = estoque_grupo
        self.id_cenario_migracao_global = id_cenario_migracao_global
        self.cnpj_cabeca = cnpj_cabeca
        self.id_grupo = int(id_grupo)
        self.id_segmento = int(id_segmento)
        self.indexador = Tela.get_indexador(self.tela)
        self.rating_original_grupo = rating_grupo.upper()
        self.rating_grupo = rating_grupo.upper()
        self.rating_operacao = rating_operacao
        self.peridiocidade_juros_dias = 180
        self.k_grupo = 1
        self.id_volatilidade = id_volatilidade
        self.prazo = int(prazo)
        self.df_estoque = pd.DataFrame()
        
        #Variaveis aux de calculo globais
        self.k1 = Cea_globais.get_k1_port(self.cea_globais)
        self.k2 = Cea_globais.get_k2_port(self.cea_globais)
        self.k3 = Cea_globais.get_k3_port(self.cea_globais)

###########################

    def get_has_migracao(self):
        if self.id_volatilidade == '':
            return False
        else:
            return True
    
    def get_id_volatilidade(self):
        return self.id_volatilidade
    
    def ajustar_lgd(self, multiplicador):
            self.estoque_grupo.mudar_lgd(multiplicador)
    
    def ajustar_prazo(self, multiplicador):
        self.estoque_grupo.mudar_prazo(multiplicador)

#############################3          

    def atualiza_estoque(self, dc):
        quo_cliente = math.floor(dc/self.peridiocidade_juros_dias)
        dia_ultimo_pagamentos_juros = quo_cliente * 180
        res = Curvas_juros.get_curva_dc(self.curvas_juros, self.indexador, dc) / Curvas_juros.get_curva_dc(self.curvas_juros, self.indexador, dia_ultimo_pagamentos_juros)
        return res

###############################

    def get_pd_roe_ponderada(self, dias): 
        self.estoque_grupo.is_vencida_df(dias)
        self.estoque_grupo.df_operacoes["pdxeadxlgd"] = self.estoque_grupo.df_operacoes["pd_roe"] * Operacao_df.get_ead(self.estoque_grupo) * Operacao_df.get_lgd(self.estoque_grupo)
        self.estoque_grupo.df_operacoes["eadxlgd"]= Operacao_df.get_ead(self.estoque_grupo) * Operacao_df.get_lgd(self.estoque_grupo)
        soma_pd_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["pdxeadxlgd"].sum() 
        soma_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["eadxlgd"].sum() 
        return (soma_pd_ead_lgd / soma_ead_lgd)

    def get_prazo_poderada_cluster(self, dias):
        self.df_estoque["is_vencida"] = self.df_estoque["duration"].le(dias)
        self.df_estoque["prazoxeadxlgd"] = (self.df_estoque["duration"] - dias) * self.df_estoque["ead"]
        self.df_estoque["eadxlgd"] = self.df_estoque["ead"]
        soma_prazo_ead_lgd = self.df_estoque[(self.df_estoque.is_vencida == False)]["prazoxeadxlgd"].sum() 
        soma_ead_lgd = self.df_estoque[(self.df_estoque.is_vencida == False)]["eadxlgd"].sum() 
        if(soma_ead_lgd == 0):
            return 0
        else:
            return (soma_prazo_ead_lgd/soma_ead_lgd)

    def get_prazo_ponderada(self, dias):
        self.estoque_grupo.is_vencida_df(dias)
        self.estoque_grupo.df_operacoes["prazoxeadxlgd"] =  (self.estoque_grupo.df_operacoes["duration"] - dias) * self.estoque_grupo.df_operacoes["ead"]
        self.estoque_grupo.df_operacoes["eadxlgd"] = self.estoque_grupo.df_operacoes["ead"]
        soma_prazo_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["prazoxeadxlgd"].sum() 
        soma_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["eadxlgd"].sum() 
        if(soma_ead_lgd == 0):
            return 0
        else:
            return (soma_prazo_ead_lgd / soma_ead_lgd)
    
    def get_pd_modelo_ponderada(self, dias):
        self.estoque_grupo.is_vencida_df(dias)
        self.estoque_grupo.df_operacoes["pdxeadxlgd"]=  self.estoque_grupo.df_operacoes["pd_modelo"] * self.estoque_grupo.df_operacoes["ead"] * self.estoque_grupo.df_operacoes["lgd"]
        self.estoque_grupo.df_operacoes["eadxlgd"]= self.estoque_grupo.df_operacoes["ead"] * self.estoque_grupo.df_operacoes["lgd"]
        soma_pd_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["pdxeadxlgd"].sum() 
        soma_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["eadxlgd"].sum() 
        return (soma_pd_ead_lgd / soma_ead_lgd)
    
    def get_lgd_ponderada(self, dias):
        self.estoque_grupo.is_vencida_df(dias)
        self.estoque_grupo.df_operacoes["eadxlgd"]= self.estoque_grupo.df_operacoes["ead"] * self.estoque_grupo.df_operacoes["lgd"]
        soma_ead = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["ead"].sum()   
        soma_lgd_ead = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["eadxlgd"].sum() 
        if(soma_ead == 0):
            return 0
        else:
            return (soma_lgd_ead/soma_ead) 
    
    def get_lgd_ponderada_cluster(self, dias): 
        self.df_estoque["is_vencida"] = self.df_estoque["duration"].le(dias)
        self.df_estoque["eadxlgd"]= self.df_estoque["ead"] * self.df_estoque["lgd"]
        soma_ead = self.df_estoque[(self.df_estoque.is_vencida == False)]["ead"].sum()   
        soma_lgd_ead = self.df_estoque[(self.df_estoque.is_vencida == False)]["eadxlgd"].sum() 
        if(soma_ead == 0):
            return 0
        else:
            return (soma_lgd_ead/soma_ead)

    def get_wi_ponderada(self, dias):
        self.estoque_grupo.is_vencida_df(dias)
        self.estoque_grupo.df_operacoes["wixeadxlgd"]=  self.estoque_grupo.df_operacoes["wi"] * self.estoque_grupo.df_operacoes["ead"] * self.estoque_grupo.df_operacoes["lgd"]
        self.estoque_grupo.df_operacoes["eadxlgd"]= self.estoque_grupo.df_operacoes["ead"] * self.estoque_grupo.df_operacoes["lgd"]
        soma_wi_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["wixeadxlgd"].sum() 
        soma_ead_lgd = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["eadxlgd"].sum() 
        return (soma_wi_ead_lgd / soma_ead_lgd)
    
    def get_wi_ponderada_cluster(self, dias):
        self.df_estoque["is_vencida"] = self.df_estoque["duration"].le(dias)
        self.df_estoque["wixeadxlgd"] =  self.df_estoque["wi"] * self.df_estoque["ead"] * self.df_estoque["lgd"]
        self.df_estoque["eadxlgd"] = self.df_estoque["ead"] * self.df_estoque["lgd"]
        soma_wi_ead_lgd = self.df_estoque[((self.df_estoque.is_vencida == False) and (self.df_estoque.rating_original == self.rating_grupo) and (self.df_estoque.incluir_no_cluster == True) and (self.df_estoque.id_grupo == self.id_grupo))]["wixeadxlgd"].sum() 
        soma_ead_lgd = self.df_estoque[((self.df_estoque.is_vencida == False) and (self.df_estoque.rating_original == self.rating_grupo) and (self.df_estoque.incluir_no_cluster == True) and (self.df_estoque.id_grupo == self.id_grupo) )]["eadxlgd"].sum() 
        return (soma_wi_ead_lgd / soma_ead_lgd)

    def get_ead_somado(self, dias):
        self.estoque_grupo.is_vencida_df(dias)
        soma_ead = self.estoque_grupo.df_operacoes[self.estoque_grupo.df_operacoes.is_vencida == False]["ead"].sum()   
        soma_ead = soma_ead * self.atualiza_estoque(dias)
        return soma_ead    
    
    def get_ead_somado_cluster(self, dias):
        self.df_estoque["is_vencida"] = self.df_estoque["duration"].le(dias)
        soma_ead = self.df_estoque[self.df_estoque.is_vencida == False]["ead"].sum()        
        soma_ead = soma_ead * self.atualiza_estoque(dias)
        return soma_ead    
    
    def get_pe_cluster(self, np_estoque, dias):    
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, np_estoque[:,5], 0) 
        return np_filtrado_prazo.sum()   

    def get_mfb_estoque(self, dias, prazo_anterior, prazo): 
        tkf = Toolkit_finance(self.curvas_juros)
        soma_mfb = 0
        Operacao_df.is_vencida_df(self.estoque_grupo, dias)

        ajuste_ead = self.atualiza_estoque(dias) * Operacao_df.get_ead(self.estoque_grupo)
        self.estoque_grupo.df_operacoes["mfb"] = ajuste_ead * (np.vectorize(tkf.get_accrual)("LIN/360", "BRL", prazo_anterior, prazo,self.estoque_grupo.df_operacoes["spread"], 0) - 1) - (tkf.get_accrual("LIN/360", "BRL", prazo_anterior, prazo, 0, 0) - 1)
        
        mfb_validos = self.estoque_grupo.df_operacoes["mfb"].where( ((self.estoque_grupo.df_operacoes["incluir_no_cluster"] == True) & (self.estoque_grupo.df_operacoes["is_vencida"] == False) & (self.estoque_grupo.df_operacoes["rating"] == self.rating_grupo)), 0)
        soma_mfb = mfb_validos.sum()

        return soma_mfb    
    
####################################

    def calcula_kreg(self,ead, fpr, perc_n1, perc_bis):
        res = min((ead * fpr * perc_n1 * perc_bis), ead)
        return res    
        
    def get_kreg_cluster(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, np_estoque[:,6], 0) 
        return np_filtrado_prazo.sum()
    
    def cea_estoque_marginal_migracao_cluster(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, np_estoque[:,1], 0) 
        return np_filtrado_prazo.sum()
    
    def cea_marginal_sem_pe(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, (np_estoque[:,9] - np_estoque[:,7]), 0) 
        return np_filtrado_prazo.sum()
    
    def cea_marginal(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, (np_estoque[:,8] - np_estoque[:,6]), 0) 
        return np_filtrado_prazo.sum()

    def cea_estoque_marginal_migracao(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, (np_estoque[:,3] - np_estoque[:,1]), 0) 
        return np_filtrado_prazo.sum()

    def cea_estoque_marginal_migracao_sem_pe(self, np_estoque, dias):
        np_filtrado_prazo = np.where(np_estoque[:,0] == dias, (np_estoque[:,4] - np_estoque[:,2]), 0)
        return np_filtrado_prazo.sum()

    def is_vencida_df_lote (self, df, prazo):
        df["is_vencida"] = np.where(df["esta_na_carteira"] == True, df["duration"].le(prazo), False)       #se o prazo for zero considerar vencida tambem 

    def mudar_rating_nivel_grupo_lote(self, df, rating_op_nova, rating_grupo):
        if(rating_op_nova != rating_grupo):
            df['condicao_mudar_rtg'] = False
            df.loc[(df['esta_na_carteira'] == False), 'condicao_mudar_rtg'] = True
            df.loc[df['condicao_mudar_rtg'], 'rating'] = df['rtg_mig']
        else:
            df['condicao_mudar_rtg'] = True
            #df.loc[(df['id_grupo'] == self.id_grupo), 'condicao_mudar_rtg'] = True  
            df.loc[df['condicao_mudar_rtg'], 'rating'] = df['rtg_mig']

    def get_cea_puro_op_lote(self, array_pd_modelo,array_lgd,array_ead,array_wi,array_lgd_dt,array_lgd_be,array_default,array_is_vencida,dias):
        ead_ajustado = array_ead * self.atualiza_estoque(dias)
        cea_n_default = np.vectorize(cr_com_concentracao)(array_pd_modelo,array_lgd,ead_ajustado,array_wi,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
        cea_default = np.vectorize(default_pi)(ead_ajustado,array_lgd_dt,array_lgd_be)
                
        cea_puro = np.where(array_default=='TRUE', cea_default, cea_n_default)
        cea_puro  = np.vectorize(min)(cea_puro, ead_ajustado)
        soma_cea_puro = np.where(array_is_vencida == False, cea_puro, 0)
        soma_cea_puro = soma_cea_puro.sum()

        return soma_cea_puro

    def get_k_grupo_sem_nova_op(self,dias): 
        df_filtrado = self.estoque_grupo.df_operacoes.loc[:,['ead', 'duration', 'lgd', 'pd_modelo', 'wi', 'lgd_be', 'lgd_dt', 'default', 'esta_na_carteira']] #filtrar colunas a serem pegas do df original
        self.is_vencida_df_lote(df_filtrado, dias)  # marcar as operacoes como vencidas ou nao vencidas
        df_np = df_filtrado.values   # passar o df para numpy

        #####################VARIAVEIS DO ARRAY#############################
        array_ead_original = df_np[:,0] 
        array_duration = df_np[:,1]
        array_is_vencida = df_np[:,9]  #sempre o ultimo
        array_lgd = df_np[:,2]
        array_pd_modelo = df_np[:,3]
        array_wi = df_np[:,4]
        array_lgd_be = df_np[:,5]
        array_lgd_dt = df_np[:,6]
        array_default = df_np[:,7]
        array_esta_na_carteira = df_np[:,8]
        array_ead = np.where(array_esta_na_carteira == False, (array_ead_original/self.atualiza_estoque(dias)), array_ead_original)
        #####################LOGICA DA EAD_SOMADO###########################
        soma_ead = np.where(array_is_vencida == False, array_ead, 0)

        soma_ead = soma_ead.sum() * self.atualiza_estoque(dias)

        ead_grupo = soma_ead 

        #######################LOGICA DO PD_MODELO_PONDERADA################
        pdxeadxlgd_aux = array_pd_modelo * array_ead * array_lgd
        pdxeadxlgd = np.where(array_is_vencida == False, pdxeadxlgd_aux, 0)
        soma_pd_ead_lgd = pdxeadxlgd.sum()

        eadxlgd_aux = array_ead * array_lgd
        eadxlgd = np.where(array_is_vencida == False, eadxlgd_aux, 0)
        soma_ead_lgd = eadxlgd.sum()
        if(soma_ead_lgd == 0):
            pd_grupo = 0
        else:
            pd_grupo = (soma_pd_ead_lgd/soma_ead_lgd)

        ################################LOGICA LGD PONDERADA###############
        soma_ead_sem_ajuste = np.where(array_is_vencida == False, array_ead, 0)
        soma_ead_sem_ajuste = soma_ead_sem_ajuste.sum()
        if(soma_ead_sem_ajuste == 0):
            lgd_grupo = 0
        else:
            lgd_grupo = (soma_ead_lgd/soma_ead_sem_ajuste)
        ################################LOGICA WI PONDERADA################
        wixeadxlgd_aux = array_wi * array_ead * array_lgd
        wixeadxlgd = np.where(array_is_vencida == False, wixeadxlgd_aux, 0)
        soma_wi_ead_lgd =  wixeadxlgd.sum()
        if(soma_wi_ead_lgd == 0):
            wi_grupo = 0
        else:
            wi_grupo = (soma_wi_ead_lgd/soma_ead_lgd)
        #############################LOGICA PARA SOMA_CEA_PURO##########
            
        if(pd_grupo == 1):
            ead_ajustado = array_ead * self.atualiza_estoque(dias)
            cea_n_default = np.vectorize(cr_com_concentracao)(array_pd_modelo,array_lgd,ead_ajustado,array_wi,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
            cea_default = np.vectorize(default_pi)(ead_ajustado,array_lgd_dt,array_lgd_be)
                
            cea_puro = np.where(array_default=='TRUE', cea_default, cea_n_default)
            cea_puro  = np.vectorize(min)(cea_puro, ead_ajustado)
            soma_cea_puro_grupo = np.where(array_is_vencida == False, cea_puro, 0)
            soma_cea_puro_grupo = soma_cea_puro_grupo.sum()
        else:
            soma_cea_puro_grupo = cr_com_concentracao(pd_grupo,lgd_grupo,ead_grupo,wi_grupo,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
        
        
        cea_grupo = min(soma_cea_puro_grupo, ead_grupo)
  
        #############################CEA_OPERACOES##########################################
        cea_operacoes = self.get_cea_puro_op_lote(array_pd_modelo, array_lgd, array_ead, array_wi, array_lgd_dt, array_lgd_be, array_default,array_is_vencida,dias) 
        if(cea_operacoes != 0):
            res = max((cea_grupo/cea_operacoes), 1)
            return res
        else:
            return 1

    def get_k_grupo_lote_sem_pe(self, dias): 
        #print(df.columns)
        df_filtrado = self.df_estoque.loc[:,['ead', 'duration', 'lgd', 'pd_modelo', 'wi', 'lgd_be', 'lgd_dt', 'default', 'esta_na_carteira']] #filtrar colunas a serem pegas do df original
        self.is_vencida_df_lote(df_filtrado, dias)  # marcar as operacoes como vencidas e nao vencidas
        df_np = df_filtrado.values  # passar o df para numpy
 
        #####################VARIAVEIS DO ARRAY#############################
        array_ead_original = df_np[:,0] 
        array_duration = df_np[:,1]
        array_is_vencida = df_np[:,9]  #sempre o ultimo
        array_lgd = df_np[:,2]
        array_pd_modelo = df_np[:,3]
        array_wi = df_np[:,4]
        array_lgd_be = df_np[:,5]
        array_lgd_dt = df_np[:,6]
        array_default = df_np[:,7]
        array_esta_na_carteira = df_np[:,8]
        #array_ead_op_novas = df_np[:,9]
        ajuste = self.atualiza_estoque(dias)
        #array_ead = np.where(array_esta_na_carteira == False, array_ead_original/self.atualiza_estoque(dias), array_ead_original)
        array_ead = np.where(array_esta_na_carteira == False, array_ead_original, array_ead_original)
        #####################LOGICA DA EAD_SOMADO###########################
        soma_ead = np.where(array_is_vencida == False, array_ead, 0)
        #print(soma_ead)
        
        soma_ead = soma_ead.sum() * self.atualiza_estoque(dias)
        #print(soma_ead)
        
        ead_grupo = soma_ead 
        #print(ead_grupo)    #esta certo
        #######################LOGICA DO PD_MODELO_PONDERADA################
        pdxeadxlgd_aux = array_pd_modelo * array_ead * array_lgd
        pdxeadxlgd = np.where(array_is_vencida == False, pdxeadxlgd_aux, 0)
        soma_pd_ead_lgd = pdxeadxlgd.sum()
        #print(pdxeadxlgd)
        
        eadxlgd_aux = array_ead * array_lgd
        eadxlgd = np.where(array_is_vencida == False, eadxlgd_aux, 0)
        soma_ead_lgd = eadxlgd.sum()
        
        if(soma_ead_lgd == 0):
            pd_grupo = 1
        else:
            pd_grupo = (soma_pd_ead_lgd/soma_ead_lgd)
        #print(pd_grupo)

        ################################LOGICA LGD PONDERADA###############
        soma_ead_sem_ajuste = np.where(array_is_vencida == False, array_ead, 0)
        soma_ead_sem_ajuste = soma_ead_sem_ajuste.sum()

        if(soma_ead_sem_ajuste == 0):
            lgd_grupo = 1
        else:
            lgd_grupo = (soma_ead_lgd/soma_ead_sem_ajuste)
        #print(lgd_grupo)
        ################################LOGICA WI PONDERADA################
        wixeadxlgd_aux = array_wi * array_ead * array_lgd
        wixeadxlgd = np.where(array_is_vencida == False, wixeadxlgd_aux, 0)
        soma_wi_ead_lgd =  wixeadxlgd.sum()

        if(soma_wi_ead_lgd == 0):
            wi_grupo = 1
        else:
            wi_grupo = (soma_wi_ead_lgd/soma_ead_lgd)
        #print(wi_grupo)
        #############################LOGICA PARA SOMA_CEA_PURO##########
            
        if(pd_grupo == 1):
            ead_ajustado = array_ead * self.atualiza_estoque(dias)
            cea_n_default = np.vectorize(cr_com_concentracao)(array_pd_modelo,array_lgd,ead_ajustado,array_wi,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
            cea_default = np.vectorize(default_pi)(ead_ajustado,array_lgd_dt,array_lgd_be)
                
            cea_puro = np.where(array_default=='TRUE', cea_default, cea_n_default)
            cea_puro  = np.vectorize(min)(cea_puro, ead_ajustado)
            soma_cea_puro_grupo = np.where(array_is_vencida == False, cea_puro, 0)
            soma_cea_puro_grupo = soma_cea_puro_grupo.sum()
        else:
            soma_cea_puro_grupo = cr_com_concentracao(pd_grupo,lgd_grupo,ead_grupo,wi_grupo,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
        
        
        cea_grupo = min(soma_cea_puro_grupo, ead_grupo)
        #print(cea_grupo)    

        #############################CEA_OPERACOES##########################################
        cea_operacoes = self.get_cea_puro_op_lote(array_pd_modelo, array_lgd, array_ead, array_wi, array_lgd_dt, array_lgd_be, array_default,array_is_vencida,dias)
        #print(cea_operacoes)    
        if(cea_operacoes != 0):
            res = max((cea_grupo/cea_operacoes), 1)
            return res
        else:
            return 1

    def get_k_grupo_lote(self,dias): 
        df_filtrado = self.df_estoque.loc[:,['ead', 'duration', 'lgd', 'pd_modelo', 'wi', 'lgd_be', 'lgd_dt', 'default', 'esta_na_carteira']] #filtrar colunas a serem pegas do df original
        self.is_vencida_df_lote(df_filtrado, dias)  # marcar as operacoes como vencidas e nao vencidas
        df_np = df_filtrado.values   # passar o df para numpy
 
        #####################VARIAVEIS DO ARRAY#############################
        array_ead_original = df_np[:,0] 
        array_duration = df_np[:,1]
        array_is_vencida = df_np[:,9]  #sempre o ultimo
        array_lgd = df_np[:,2]
        array_pd_modelo = df_np[:,3]
        array_wi = df_np[:,4]
        array_lgd_be = df_np[:,5]
        array_lgd_dt = df_np[:,6]
        array_default = df_np[:,7]
        array_esta_na_carteira = df_np[:,8]
        ajuste = self.atualiza_estoque(dias)
        array_ead = np.where(array_esta_na_carteira == False, array_ead_original/self.atualiza_estoque(dias), array_ead_original)
        #####################LOGICA DA EAD_SOMADO###########################
        soma_ead = np.where(array_is_vencida == False, array_ead, 0)
       
        
        soma_ead = soma_ead.sum() * self.atualiza_estoque(dias)
        
        
        ead_grupo = soma_ead 
        
        #######################LOGICA DO PD_MODELO_PONDERADA################
        pdxeadxlgd_aux = array_pd_modelo * array_ead * array_lgd
        pdxeadxlgd = np.where(array_is_vencida == False, pdxeadxlgd_aux, 0)
        soma_pd_ead_lgd = pdxeadxlgd.sum()
        
        
        eadxlgd_aux = array_ead * array_lgd
        eadxlgd = np.where(array_is_vencida == False, eadxlgd_aux, 0)
        soma_ead_lgd = eadxlgd.sum()
        
        if(soma_ead_lgd == 0):
            pd_grupo = 1
        else:
            pd_grupo = (soma_pd_ead_lgd/soma_ead_lgd)
        

        ################################LOGICA LGD PONDERADA###############
        soma_ead_sem_ajuste = np.where(array_is_vencida == False, array_ead, 0)
        soma_ead_sem_ajuste = soma_ead_sem_ajuste.sum()

        if(soma_ead_sem_ajuste == 0):
            lgd_grupo = 1
        else:
            lgd_grupo = (soma_ead_lgd/soma_ead_sem_ajuste)
        
        ################################LOGICA WI PONDERADA################
        wixeadxlgd_aux = array_wi * array_ead * array_lgd
        wixeadxlgd = np.where(array_is_vencida == False, wixeadxlgd_aux, 0)
        soma_wi_ead_lgd =  wixeadxlgd.sum()

        if(soma_wi_ead_lgd == 0):
            wi_grupo = 1
        else:
            wi_grupo = (soma_wi_ead_lgd/soma_ead_lgd)
        
        #############################LOGICA PARA SOMA_CEA_PURO##########
            
        if(pd_grupo == 1):
            ead_ajustado = array_ead * self.atualiza_estoque(dias)
            cea_n_default = np.vectorize(cr_com_concentracao)(array_pd_modelo,array_lgd,ead_ajustado,array_wi,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
            cea_default = np.vectorize(default_pi)(ead_ajustado,array_lgd_dt,array_lgd_be)
                
            cea_puro = np.where(array_default=='TRUE', cea_default, cea_n_default)
            cea_puro  = np.vectorize(min)(cea_puro, ead_ajustado)
            soma_cea_puro_grupo = np.where(array_is_vencida == False, cea_puro, 0)
            soma_cea_puro_grupo = soma_cea_puro_grupo.sum()
        else:
            soma_cea_puro_grupo = cr_com_concentracao(pd_grupo,lgd_grupo,ead_grupo,wi_grupo,Cea_globais.get_pewi_port(self.cea_globais), Cea_globais.get_sigma_port(self.cea_globais), Cea_globais.get_var_port(self.cea_globais), Cea_globais.get_pe_port(self.cea_globais), Cea_globais.get_desvio_port(self.cea_globais))
        
        
        cea_grupo = min(soma_cea_puro_grupo, ead_grupo)
            

        #############################CEA_OPERACOES##########################################
        cea_operacoes = self.get_cea_puro_op_lote(array_pd_modelo, array_lgd, array_ead, array_wi, array_lgd_dt, array_lgd_be, array_default,array_is_vencida,dias)
    
        if(cea_operacoes != 0):
            res = max((cea_grupo/cea_operacoes), 1)
            return res
        else:
            return 1

    def adiciona_op_estoque(self, nova_operacao):
        self.estoque_grupo.df_operacoes =  self.estoque_grupo.df_operacoes.append(nova_operacao)
        self.estoque_grupo.df_operacoes.index = self.estoque_grupo.df_operacoes.index + 1
        self.estoque_grupo.df_operacoes = self.estoque_grupo.df_operacoes.sort_index() 
        return self.estoque_grupo.df_operacoes

    def retira_op_nova_estoque(self):
        df_remove = self.estoque_grupo.df_operacoes.loc[(self.estoque_grupo.df_operacoes['esta_na_carteira'] == False)]
        self.estoque_grupo.df_operacoes = self.estoque_grupo.df_operacoes.drop(df_remove.index)
        self.estoque_grupo.df_operacoes.index = self.estoque_grupo.df_operacoes.index - 1
        self.estoque_grupo.df_operacoes = self.estoque_grupo.df_operacoes.sort_index()
    
    def retira_op_na_carteira(self, df_estoque):    
        df_remove = df_estoque.loc[(df_estoque['esta_na_carteira'] == True)]
        df_estoque = df_estoque.drop(df_remove.index)
        df_estoque.index = df_estoque.index - 1
        df_estoque = df_estoque.sort_index()
        return df_estoque

    def filtro_cluster(self):
        self.df_estoque["incluir_no_cluster"] = np.where(self.df_estoque["esta_na_carteira"], True, False)
        df_cluster = self.df_estoque.loc[(self.df_estoque['esta_na_carteira'] == True) & (self.df_estoque['incluir_no_cluster'] == True) & (self.df_estoque['rating_original'] == Tela.get_rating_grupo(self.tela)) & (self.df_estoque['is_vencida'] == False) & (self.df_estoque['id_grupo'] == Tela.get_id_grupo(self.tela))]
        return df_cluster

    def filtro_cluster_operacao(self):
        df_cluster = self.df_estoque.loc[(self.df_estoque['esta_na_carteira'] == False)]
        return df_cluster
    
    def gerar_array_prazo(self, array_vertice):
        array_prazo = []
        prazo_aux = 0
        i = 0

        while array_vertice[i].get_prazo_decorrido() < self.prazo:
            array_prazo.append(array_vertice[i].get_prazo_decorrido())
            i += 1

        if (array_vertice[i].get_prazo_decorrido() >= self.prazo):
            array_prazo.append(self.prazo)

        return array_prazo
    
    def gerar_df_ead_novo(self,array_vertice, indexador_ativo):
        np_ead_prazo = np.empty(shape = [0,2])
        for i in range(len(array_vertice)):
            array_vertice[i].calcula_posicao_retorno()
            array_vertice[i].calcula_posicao_risco()
            ead_ajustado = array_vertice[i].get_ead_cea()/self.atualiza_estoque(array_vertice[i].get_prazo_decorrido())
            ead_convertido = ead_ajustado * Moeda_conversao.get_spot(self.moeda_conversao) * Toolkit_finance.forward_curency_dc(self.toolkit_finance, "BRL", indexador_ativo, array_vertice[i].get_prazo_decorrido())
            np_ead_prazo = np.append(np_ead_prazo, [[array_vertice[i].get_prazo_decorrido(), ead_convertido]], axis = 0)
            df_ead_prazo = pd.DataFrame(np_ead_prazo, columns=["prazo","ead_op_novas"])
        return df_ead_prazo
    
    def gerar_duration_vertice(self, array_vertice):
        np_duration = np.empty(shape= [0,2])
        for i in range(len(array_vertice)):
            duration = array_vertice[i].obter_duration()
            np_duration = np.append(np_duration, [[array_vertice[i].get_prazo_decorrido(), duration]], axis = 0)
            df_duration = pd.DataFrame(np_duration, columns=["prazo", "prazo_medio"])
        return df_duration

    def gerar_array_fpr(self, array_vertice):
        array_fpr = []
        
        for i in range(len(array_vertice)):
            array_fpr.append(array_vertice[i].get_fpr_vertice())
        
        return array_fpr
    
    def gerar_lgd_mitigado(self, array_vertice):
        np_lgd = np.empty(shape= [0,2])
        for i in range(len(array_vertice)):
            lgd = array_vertice[i].get_lgd_mitigada()
            np_lgd = np.append(np_lgd, [[array_vertice[i].get_prazo_decorrido(), lgd]], axis = 0)
            df_lgd = pd.DataFrame(np_lgd, columns=["prazo", "lgd_mitigada"])
        return df_lgd
    
    ######INICIO FUNCOES PARA CALCULO DE CAPITAL######   
    def calcula_capital_nao_default(self, array_pd_modelo, array_lgd, array_ead_corrigido, array_wi):
        pewi = Cea_globais.get_pewi_port(self.cea_globais)
        sigma = Cea_globais.get_sigma_port(self.cea_globais)
        var = Cea_globais.get_var_port(self.cea_globais)
        pe = Cea_globais.get_pe_port(self.cea_globais)
        desvio = Cea_globais.get_desvio_port(self.cea_globais)
        return np.vectorize(cr_com_concentracao)(array_pd_modelo,array_lgd,array_ead_corrigido, array_wi, pewi,sigma, var, pe, desvio)
    
    def calcula_capital_default(self, array_ead_corrigido, array_lgd_dt, array_lgd_be):
        return np.vectorize(default_pi)(array_ead_corrigido,array_lgd_dt,array_lgd_be)

    def calcula_capital_final_nao_default(self, k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default):
        return np.vectorize(max)(pe_liq, np.vectorize(min)((np.vectorize(recalibrar_cea_nao_default)(k1, k2, k3, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration-array_prazo_simulado , array_k_grupo)) + pe_liq, array_ead_corrigido))
    
    def calcula_capital_final_nao_default_sem_pe(self, k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default):
        return np.vectorize(min)((np.vectorize(recalibrar_cea_nao_default)(k1, k2, k3, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration-array_prazo_simulado , array_k_grupo)), array_ead_corrigido)
    
    def calcula_capital_final_default(self, k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default):
        return np.vectorize(min)((np.vectorize(recalibrar_cea_default)(k1, k2, k3, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration-array_prazo_simulado , array_k_grupo) + pe_liq), array_ead_corrigido)

    def calcula_capital_final_cluster_nao_default_sem_pe(self,pe_liq, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_ead_corrigido):
        return np.vectorize(max)(pe_liq, np.vectorize(min)(np.vectorize(recalibrar_cea_cluster_nao_default)(cea_puro, array_pd_modelo, array_duration-array_prazo_simulado , array_k_grupo), array_ead_corrigido)) 
    
    def calcula_capital_final_cluster_nao_default(self,pe_liq, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_ead_corrigido):
        return np.vectorize(max)(pe_liq, np.vectorize(min)((np.vectorize(recalibrar_cea_cluster_nao_default)(cea_puro, array_pd_modelo, array_duration-array_prazo_simulado , array_k_grupo)) + pe_liq, array_ead_corrigido)) 
    
    def calcula_capital_final_cluster_default(self, pe_liq, cea_puro, array_ead_corrigido):
        return np.vectorize(min)((np.vectorize(recalibrar_cea_cluster_default)(cea_puro) + pe_liq), array_ead_corrigido)   
    
    def get_setup_raroc(self, vertices, nova, df_ead):
        df_monstro = pd.DataFrame()
        df_prazo = pd.DataFrame()
        array_prazos = self.gerar_array_prazo(vertices)
        matriz_fpr = self.gerar_array_fpr(vertices)
        df_prazo_medio = self.gerar_duration_vertice(vertices)
        df_lgd = self.gerar_lgd_mitigado(vertices)

        if(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 71):
            vetor_rating = get_vetor_rating_private_pf()
        elif(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 72):
            vetor_rating = get_vetor_rating_private_pj()
        elif(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 73):
            vetor_rating = get_vetor_rating_private_offshore()
        else:
            vetor_rating = get_vetor_rating() 
        
        array_rtg = Regua_pd.get_array_np(self.regua_pd, self.id_segmento)   #rating, pd_modelo, wi, lgd_dt, lgd_be
        df_rtg = pd.DataFrame(array_rtg, columns = ['Rating','Pd_modelo','Wi','Lgd_dt','Lgd_be', 'Default'])
        
        #Salva uma copia do estoque original com a operacao nova
        self.df_estoque = self.adiciona_op_estoque(nova) 
        
        #Retira a operacao do estoque original, fazendo voltar ao original
        self.retira_op_nova_estoque()          
        
        if(self.estoque_grupo.df_operacoes.empty and Tela.get_tratamento_prazo_medio(self.tela)):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = 1
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = ead_novo
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                prazo_medio = df_prazo_medio.loc[df_prazo_medio['prazo'] == prazo, 'prazo_medio'].values
                if(prazo_medio != 0):
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = (prazo_medio + prazo)
                else:
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = prazo_medio
                
                self.df_estoque['k_grupo_alterado'] = 1
                self.df_estoque['k_grupo_alterado_sem_pe'] = 1

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)  

        elif(not(self.estoque_grupo.df_operacoes.empty) and Tela.get_tratamento_prazo_medio(self.tela)):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = self.get_k_grupo_sem_nova_op(prazo)
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                prazo_medio = df_prazo_medio.loc[df_prazo_medio['prazo'] == prazo, 'prazo_medio'].values
                if(prazo_medio != 0):
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = (prazo_medio + prazo)
                else:
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = prazo_medio
                
                self.df_estoque['k_grupo_alterado'] = self.get_k_grupo_lote(prazo)
                self.df_estoque['k_grupo_alterado_sem_pe'] = self.get_k_grupo_lote_sem_pe(prazo)

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)

        elif(self.estoque_grupo.df_operacoes.empty and not(Tela.get_tratamento_prazo_medio(self.tela))):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = 1
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = ead_novo

                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                self.df_estoque['k_grupo_alterado'] = 1
                self.df_estoque['k_grupo_alterado_sem_pe'] = 1
                #self.df_estoque["fpr"] = np.where(matriz_fpr[i][0] == prazo, matriz_fpr[i][1], 0)
                #i += 1
                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)
        
        elif(not(self.estoque_grupo.df_operacoes.empty) and not(Tela.get_tratamento_prazo_medio(self.tela))):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = self.get_k_grupo_sem_nova_op(prazo)
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                self.df_estoque['k_grupo_alterado'] = self.get_k_grupo_lote(prazo)
                self.df_estoque['k_grupo_alterado_sem_pe'] = self.get_k_grupo_lote_sem_pe(prazo)

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)     

        #Pegar info de todos os ratings
        for rtg in vetor_rating:
            df_prazo['rtg_mig'] = rtg
            df_monstro = df_monstro.append(df_prazo,ignore_index = True)

        return [df_monstro, array_prazos]

    def get_setup_raroc_cluster(self, vertices, nova, df_ead):
        df_monstro = pd.DataFrame()
        df_prazo = pd.DataFrame()
        array_prazos = self.gerar_array_prazo(vertices)
        matriz_fpr = self.gerar_array_fpr(vertices)
        df_prazo_medio = self.gerar_duration_vertice(vertices)
        df_lgd = self.gerar_lgd_mitigado(vertices)

        if(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 71):
            vetor_rating = get_vetor_rating_private_pf()
        elif(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 72):
            vetor_rating = get_vetor_rating_private_pj()
        elif(int(Tela.get_id_segmento_risco_grupo(self.tela)) == 73):
            vetor_rating = get_vetor_rating_private_offshore()
        else:
            vetor_rating = get_vetor_rating() 
        
        array_rtg = Regua_pd.get_array_np(self.regua_pd, self.id_segmento)   #rating, pd_modelo, wi, lgd_dt, lgd_be
        df_rtg = pd.DataFrame(array_rtg, columns = ['Rating','Pd_modelo','Wi','Lgd_dt','Lgd_be', 'Default'])
        
        #Salva uma copia do estoque original com a operacao nova
        self.df_estoque = self.estoque_grupo.df_operacoes #self.adiciona_op_estoque(nova) 
        self.df_estoque = self.filtro_cluster()
        
        #Retira a operacao do estoque original, fazendo voltar ao original
        #self.retira_op_nova_estoque()          
        
        if(self.estoque_grupo.df_operacoes.empty and Tela.get_tratamento_prazo_medio(self.tela)):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = 1
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = ead_novo
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                prazo_medio = df_prazo_medio.loc[df_prazo_medio['prazo'] == prazo, 'prazo_medio'].values
                if(prazo_medio != 0):
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = (prazo_medio + prazo)
                else:
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = prazo_medio
                
                self.df_estoque['k_grupo_alterado'] = 1
                self.df_estoque['k_grupo_alterado_sem_pe'] = 1

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)  

        elif(not(self.estoque_grupo.df_operacoes.empty) and Tela.get_tratamento_prazo_medio(self.tela)):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = self.get_k_grupo_sem_nova_op(prazo)
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                prazo_medio = df_prazo_medio.loc[df_prazo_medio['prazo'] == prazo, 'prazo_medio'].values
                if(prazo_medio != 0):
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = (prazo_medio + prazo)
                else:
                    self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'duration'] = prazo_medio
                
                self.df_estoque['k_grupo_alterado'] = self.get_k_grupo_lote(prazo)
                self.df_estoque['k_grupo_alterado_sem_pe'] = self.get_k_grupo_lote_sem_pe(prazo)

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)

        elif(self.estoque_grupo.df_operacoes.empty and not(Tela.get_tratamento_prazo_medio(self.tela))):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = 1
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = ead_novo

                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                self.df_estoque['k_grupo_alterado'] = 1
                self.df_estoque['k_grupo_alterado_sem_pe'] = 1
                #self.df_estoque["fpr"] = np.where(matriz_fpr[i][0] == prazo, matriz_fpr[i][1], 0)
                #i += 1
                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)
        
        elif(not(self.estoque_grupo.df_operacoes.empty) and not(Tela.get_tratamento_prazo_medio(self.tela))):
            for prazo in array_prazos:
                self.df_estoque['prazo_sim'] = prazo
                self.df_estoque['k_grupo'] = self.get_k_grupo_sem_nova_op(prazo)
                self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
                ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
                
                lgd_mitigado = df_lgd.loc[df_lgd['prazo'] == prazo, 'lgd_mitigada'].values
                self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'lgd'] = lgd_mitigado

                self.df_estoque['k_grupo_alterado'] = self.get_k_grupo_lote(prazo)
                self.df_estoque['k_grupo_alterado_sem_pe'] = self.get_k_grupo_lote_sem_pe(prazo)

                df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)     

        #Pegar info de todos os ratings
        for rtg in vetor_rating:
            df_prazo['rtg_mig'] = rtg
            df_monstro = df_monstro.append(df_prazo,ignore_index = True)

        return [df_monstro, array_prazos]
    ######FINAL FUNCOES PARA CALCULO DE CAPITAL######

    def gerar_estoque_completo(self, nova, df_ead, vertices, array_prazos, df_monstro):
        array_rtg = Regua_pd.get_array_np(self.regua_pd, self.id_segmento)   #rating, pd_modelo, wi, lgd_dt, lgd_be
        df_rtg = pd.DataFrame(array_rtg, columns = ['Rating','Pd_modelo','Wi','Lgd_dt','Lgd_be', 'Default'])

        rating_op_nova = df_monstro.loc[(df_monstro['esta_na_carteira'] == False), 'rating_original'].values[0]

        self.mudar_rating_nivel_grupo_lote(df_monstro, rating_op_nova,self.rating_grupo)
                
        self.is_vencida_df_lote(df_monstro, df_monstro['prazo_sim'])

        df_monstro = pd.merge(df_monstro, df_rtg, how = 'left', left_on = ['rating'], right_on = ['Rating'])    #Informacoes vindas dos ratings

        df_monstro['prazo_mes'] = np.floor(df_monstro['prazo_sim']/30)  #arredondar para baixo o prazo_mes para linkar na prob de migracao
        df_monstro.loc[(df_monstro['prazo_mes'] > 36), 'prazo_mes'] = 36
        
        df_matriz = self.matriz_migracao.df
        
        df_monstro = pd.merge(df_monstro, df_matriz, how = 'left', left_on = ['rating','prazo_mes'], right_on = ['rating_final','mes_ref'])
        df_monstro = df_monstro[['rating', 'Pd_modelo', 'Wi', 'Lgd_dt', 'Lgd_be', 'Default', 'probabilidade_migracao', 'lgd', 'ead', 'is_vencida', 'k_grupo','prazo_mes', 'prazo_sim', 'duration', 'k_grupo_alterado', 'esta_na_carteira', 'rating_original', 'fator_ead', 'condicao_mudar_rtg', 'id_grupo','k_grupo_alterado_sem_pe','cd_prod']]
        
        #DESCONSIDERAR DO CALCULO IDs DIFERENTES DO DA TELA
        df_monstro = df_monstro.drop(df_monstro[df_monstro["id_grupo"] != Tela.get_id_grupo(self.tela)].index)

        if(self.rating_grupo != rating_op_nova):
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == rating_op_nova), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != rating_op_nova), 'probabilidade_migracao'] = 0

        else:
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == self.rating_grupo), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != self.rating_grupo), 'probabilidade_migracao'] = 0

        
        np_monstro = df_monstro.values

        #Separacao das variaveis em arrays para calculos
    
        array_id_grupo = np_monstro[:,19].astype(float)
        
        array_pd_modelo = np_monstro[:,1].astype(float)

        array_lgd = np_monstro[:,7]
        array_lgd = array_lgd.astype(float)
                
        array_ead_original = np_monstro[:,8].astype(float)
                
        array_wi = np_monstro[:,2].astype(float)

        array_lgd_dt = np_monstro[:,3].astype(float) 
                    
        array_lgd_be = np_monstro[:,4].astype(float)
                      
        array_prazo_simulado = np_monstro[:,12].astype(float) 
      
        array_esta_na_carteira = np_monstro[:,15].astype(bool)

        array_duration = np_monstro[:,13].astype(float)
        
        if(Tela.get_prazo_indeterminado(self.tela)):
            if (array_duration[0] < 1800): 
                param_soma =  1800 - array_duration[0]
                array_duration = np.where(array_esta_na_carteira == False, (array_duration + param_soma), array_duration)
            else: 
                array_duration = np.where(array_esta_na_carteira == False, (array_duration + array_prazo_simulado), array_duration)

        array_default = np_monstro[:,5]
                
        array_is_vencida = np_monstro[:,9].astype(bool)
                
        array_probabilidade_migracao = np_monstro[:,6].astype(float)
        
        array_k_grupo = np_monstro[:,10].astype(float)

        array_k_grupo_alterado = np_monstro[:,14].astype(float)

        array_k_grupo_alterado_sem_pe = np_monstro[:,20].astype(float)

        array_prazo_mes = np_monstro[:,11].astype(int)
        
        array_fator_ead = np_monstro[:,17].astype(float)

        array_ead = np.where(array_esta_na_carteira == False,(array_ead_original/array_fator_ead),array_ead_original)
        array_ead = array_ead.astype(float)
        
        array_ead_corrigido = np.where(array_esta_na_carteira == False, (array_ead * array_fator_ead), array_ead * array_fator_ead)

        array_ead_corrigido_sem_pe = np.where(array_esta_na_carteira == False, (array_ead_original * array_fator_ead), array_ead * array_fator_ead)    
    
        array_rating = np_monstro[:,0]

        array_rating_original = np_monstro[:,16]        
        
        ####INICIO DO CALCULO DE CAPITAL####
 
        #SEPARACAO VARIAVEIS PARA CALCULO DE RECALIBRAGEM DO CAPITAL
        k1 = float(self.k1)
        k2 = self.k2
        k3 = self.k3

        #VERTENTE DO CALCULO DOS DEMAIS SEGMENTOS                        
        cea_puro = np.where(array_default == 'TRUE', self.calcula_capital_default(array_ead_corrigido,array_lgd_dt,array_lgd_be), self.calcula_capital_nao_default(array_pd_modelo,array_lgd,array_ead_corrigido, array_wi))
        cea_puro_sem_pe = np.where(array_default == 'TRUE', self.calcula_capital_default(array_ead_corrigido_sem_pe,array_lgd_dt,array_lgd_be), self.calcula_capital_nao_default(array_pd_modelo,array_lgd,array_ead_corrigido_sem_pe, array_wi))

        pe_liq = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido
        pe_liq_sem_pe = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido_sem_pe
            
        ####CALCULO COM PE####

        #CALCULO DO CAPITAL ALOCADO NA CARTEIRA SEM INCLUSAO DA NOVA OPERACAO
        cea_final_esta_na_carteira = np.where(array_default == 'FALSE', self.calcula_capital_final_nao_default(k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default), self.calcula_capital_final_default(k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default))
        cea_final_esta_na_carteira = np.where(array_esta_na_carteira == True, cea_final_esta_na_carteira, 0)    

        #CALCULO DO CAPITAL CONSIDERANDO A NOVA OPERACAO NA CARTEIRA
        cea_final_nao_esta_na_carteira = np.where(array_default == 'FALSE', self.calcula_capital_final_nao_default(k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo_alterado, array_default), self.calcula_capital_final_default(k1, k2, k3, pe_liq, array_ead_corrigido, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo_alterado, array_default))

        #CAPITAL CONSIDERANDO PROBABILIDADE DE MIGRACAO DO GRUPO
        cea_final_esta_na_carteira_migracao = cea_final_esta_na_carteira * array_probabilidade_migracao
        cea_final_nao_esta_na_carteira_migracao = cea_final_nao_esta_na_carteira * array_probabilidade_migracao
            
        #GERACAO DO VETOR COM PERDA ESPERADA 
        array_cea_esta_na_carteira_migracao = np.where(array_is_vencida == False, cea_final_esta_na_carteira_migracao, 0)
        array_cea_nao_esta_na_carteira_migracao = np.where(array_is_vencida == False, cea_final_nao_esta_na_carteira_migracao, 0)

        ####CALCULOS SEM PE####

        #CALULO DO CAPITAL ALOCADO NA CARTEIRA SEM INCLUSAO DA NOVA OPERACAO
        cea_final_esta_na_carteira_sem_pe = np.where(array_default == 'FALSE', self.calcula_capital_final_nao_default_sem_pe(k1, k2, k3, 0, array_ead_corrigido_sem_pe, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default), self.calcula_capital_final_default(k1, k2, k3, pe_liq_sem_pe, array_ead_corrigido_sem_pe, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_default))
        cea_final_esta_na_carteira_sem_pe = np.where(array_esta_na_carteira == True, cea_final_esta_na_carteira_sem_pe, 0)  
         
        #CALCULO DO CAPITAL CONSIDERANDO A NOVA OPERACAO NA CARTEIRA
        cea_final_nao_esta_na_carteira_sem_pe = np.where(array_default == 'FALSE', self.calcula_capital_final_nao_default_sem_pe(k1, k2, k3, 0, array_ead_corrigido_sem_pe, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo_alterado_sem_pe, array_default), self.calcula_capital_final_default(k1, k2, k3, pe_liq_sem_pe, array_ead_corrigido_sem_pe, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo_alterado_sem_pe, array_default))

        #CAPITAL CONSIDERANDO PROBABILIDADE DE MIGRACAO DO GRUPO EM QUESTAO 
        cea_final_esta_na_carteira_migracao_sem_pe = cea_final_esta_na_carteira_sem_pe * array_probabilidade_migracao
        cea_final_nao_esta_na_carteira_migracao_sem_pe = cea_final_nao_esta_na_carteira_sem_pe * array_probabilidade_migracao
            
        #GERACAO DO VETOR SEM PERDA ESPERADA
        array_cea_esta_na_carteira_migracao_sem_pe = np.where(array_is_vencida == False, cea_final_esta_na_carteira_migracao_sem_pe, 0)
        array_cea_nao_esta_na_carteira_migracao_sem_pe = np.where(array_is_vencida == False, cea_final_nao_esta_na_carteira_migracao_sem_pe, 0)
                
        #INICIANDO VARIAVEIS PARA GUARDAR OS RESULTADOS
        vetor_soma_esta_na_carteira = []
        vetor_soma_esta_na_carteira_sem_pe = []
        vetor_soma_nao_esta_na_carteira = []
        vetor_soma_nao_esta_na_carteira_sem_pe = []

        np_esta_na_carteira = np.empty(shape = [0, 2])
        np_nao_esta_na_carteira = np.empty(shape = [0, 2])
        np_esta_na_carteira_sem_pe = np.empty(shape = [0, 2])
        np_nao_esta_na_carteira_sem_pe = np.empty(shape = [0, 2])
        np_completo = np.empty(shape = [0, 2])

        #INICIANDO CONTADOR           
        i = 0

        #LOOP PARA MONTAR VETOR DOS RESULTADOS POR PRAZO
        for prazo in array_prazos:   
            
            #Vetor com PE
            vetor_soma_esta_na_carteira.append(array_cea_esta_na_carteira_migracao[array_prazo_simulado == prazo].sum())
            np_esta_na_carteira = np.append(np_esta_na_carteira, [[prazo, vetor_soma_esta_na_carteira[i]]], axis=0)
            
            #Vetor sem PE
            vetor_soma_esta_na_carteira_sem_pe.append(array_cea_esta_na_carteira_migracao_sem_pe[array_prazo_simulado == prazo].sum())
            np_esta_na_carteira_sem_pe = np.append(np_esta_na_carteira_sem_pe, [[prazo, vetor_soma_esta_na_carteira_sem_pe[i]]], axis=0)
            
            #Vetor com PE   
            vetor_soma_nao_esta_na_carteira.append(array_cea_nao_esta_na_carteira_migracao[array_prazo_simulado == prazo].sum())
            np_nao_esta_na_carteira = np.append(np_nao_esta_na_carteira, [[prazo, vetor_soma_nao_esta_na_carteira[i]]], axis=0)

            #Vetor sem PE   
            vetor_soma_nao_esta_na_carteira_sem_pe.append(array_cea_nao_esta_na_carteira_migracao_sem_pe[array_prazo_simulado == prazo].sum())
            np_nao_esta_na_carteira_sem_pe = np.append(np_nao_esta_na_carteira_sem_pe, [[prazo, vetor_soma_nao_esta_na_carteira_sem_pe[i]]], axis=0)
            
            i += 1

        df_esta_na_carteira = pd.DataFrame(np_esta_na_carteira, columns = ['Prazo', 'Capital_na_carteira'])
        df_esta_na_carteira_sem_pe = pd.DataFrame(np_esta_na_carteira_sem_pe, columns = ['Prazo', 'Capital_na_carteira_sem_PE'])
        df_esta_na_carteira_completo = pd.merge(df_esta_na_carteira, df_esta_na_carteira_sem_pe, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        
        df_nao_esta_na_carteira = pd.DataFrame(np_nao_esta_na_carteira, columns = ['Prazo', 'Capital_nao_na_carteira'])
        df_nao_esta_na_carteira_sem_pe = pd.DataFrame(np_nao_esta_na_carteira_sem_pe, columns = ['Prazo', 'Capital_nao_na_carteira_sem_PE'])
        df_nao_esta_na_carteira_completo = pd.merge(df_nao_esta_na_carteira, df_nao_esta_na_carteira_sem_pe, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        
        df_completo = pd.merge(df_esta_na_carteira_completo, df_nao_esta_na_carteira_completo, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        np_completo = df_completo.values
        return [np_completo, np_monstro]

    def gerar_estoque_completo_cluster(self, nova, df_ead, vertices, array_prazos, df_monstro, flag):         
        try:
            id_segmento_risco_grupo = self.df_estoque.id_segmento_risco_grupo.values[0]
        except:
            return print("Calculo nao pode ser finalizado")

        array_rtg = Regua_pd.get_array_np(self.regua_pd, self.id_segmento)   #rating, pd_modelo, wi, lgd_dt, lgd_be
        df_rtg = pd.DataFrame(array_rtg, columns = ['Rating','Pd_modelo','Wi','Lgd_dt','Lgd_be', 'Default'])
        
        rating_op_nova = Tela.get_rating_grupo(self.tela)

        self.mudar_rating_nivel_grupo_lote(df_monstro, rating_op_nova,self.rating_grupo)
        
        self.is_vencida_df_lote(df_monstro, df_monstro['prazo_sim'])

        df_monstro = pd.merge(df_monstro, df_rtg, how = 'left', left_on = ['rating'], right_on = ['Rating'])    #informacoes vindas dos ratings

        df_monstro['prazo_mes'] = np.floor(df_monstro['prazo_sim']/30)
        df_monstro.loc[(df_monstro['prazo_mes'] > 36), 'prazo_mes'] = 36
        
        df_matriz = self.matriz_migracao.df
        
        df_monstro = pd.merge(df_monstro, df_matriz, how = 'left', left_on = ['rating','prazo_mes'], right_on = ['rating_final','mes_ref'])
        df_monstro = df_monstro[['rating', 'Pd_modelo', 'Wi', 'Lgd_dt', 'Lgd_be', 'Default', 'probabilidade_migracao', 'lgd', 'ead', 'is_vencida', 'k_grupo','prazo_mes', 'prazo_sim', 'duration', 'k_grupo_alterado', 'esta_na_carteira', 'rating_original', 'fator_ead', 'condicao_mudar_rtg', 'id_grupo','k_grupo_alterado_sem_pe','incluir_no_cluster']]
    
        if(self.rating_grupo != rating_op_nova):
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == rating_op_nova), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != rating_op_nova), 'probabilidade_migracao'] = 0
            
        else:
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == self.rating_grupo), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != self.rating_grupo), 'probabilidade_migracao'] = 0
        
        
        np_monstro = df_monstro.values
    
        array_rating = np_monstro[:,0]

        array_rating_original = np_monstro[:,16]

        array_id_grupo = np_monstro[:,19].astype(float)

        array_pd_modelo = np_monstro[:,1].astype(float)
        
        array_lgd = np_monstro[:,7].astype(float)

        array_ead_original = np_monstro[:,8].astype(float)
                
        array_wi = np_monstro[:,2].astype(float)

        array_lgd_dt = np_monstro[:,3].astype(float)
                       
        array_lgd_be = np_monstro[:,4].astype(float) 
 
        array_duration = np_monstro[:,13].astype(float)

        array_prazo_simulado = np_monstro[:,12].astype(float)

        array_esta_na_carteira = np_monstro[:,15].astype(bool)

        array_default = np_monstro[:,5]
                
        array_is_vencida = np_monstro[:,9].astype(bool)
                
        array_probabilidade_migracao = np_monstro[:,6].astype(float)
        
        array_k_grupo = np_monstro[:,10].astype(float)

        array_k_grupo_alterado = np_monstro[:,14].astype(float)

        array_k_grupo_alterado_sem_pe = np_monstro[:,20].astype(float)
 
        array_prazo_mes = np_monstro[:,11].astype(int)
        
        array_fator_ead = np_monstro[:,17].astype(float)

        array_ead = np.where(array_esta_na_carteira == False,(array_ead_original/array_fator_ead),array_ead_original)
        array_ead = array_ead.astype(float)

        array_ead_corrigido = np.where(array_esta_na_carteira == False, (array_ead * array_fator_ead), (array_ead * array_fator_ead))

        array_ead_corrigido_sem_pe = np.where(array_esta_na_carteira == False, (array_ead_original * array_fator_ead), (array_ead * array_fator_ead))   
    
        array_prazo_rem = array_duration-array_prazo_simulado

        #METODO DE CALCULO PARA OS DEMAIS SEGMENTOS
           
        #INICIO DO CALCULO DE CAPITAL
        cea_puro = np.where(array_default == 'TRUE', self.calcula_capital_default(array_ead_corrigido,array_lgd_dt,array_lgd_be), self.calcula_capital_nao_default(array_pd_modelo,array_lgd,array_ead_corrigido,array_wi))
        cea_puro_sem_pe = np.where(array_default == 'TRUE', self.calcula_capital_default(array_ead_corrigido_sem_pe,array_lgd_dt,array_lgd_be), self.calcula_capital_nao_default(array_pd_modelo,array_lgd,array_ead_corrigido_sem_pe, array_wi))
            
        #CALCULO DE PARAMETROS AUXILIARES 
        pe_liq = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido
        pe_liq_default = get_fatorpeliq() * array_pd_modelo * array_lgd_be * array_ead_corrigido
        pe_liq_sem_pe = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido_sem_pe
        pe_liq_migracao_sem_filtro = pe_liq * array_probabilidade_migracao
            
        fpr = get_fpr_cnpj(Tela.get_id_segmento_risco_grupo(self.tela)) #MUDAR FUNCAO PARA PEGAR FPR POR PRAZO E CNPJ
        n1 = get_fatorn1()
        bis= get_fatorbis()
        kreg_op_sem_filtro = np.vectorize(self.calcula_kreg)(array_ead_corrigido,fpr,n1 ,bis)

        ####CLUSTER CONSIDERANDO PERDA ESPERADA####

        #CALCULO DO CAPITAL ALOCADO NA CARTEIRA SEM INCLUSAO DA NOVA OPERACAO
        cea_final_esta_na_carteira = np.where(array_default == 'FALSE', self.calcula_capital_final_cluster_nao_default(pe_liq, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado, array_k_grupo, array_ead_corrigido), self.calcula_capital_final_cluster_default(pe_liq_default, cea_puro, array_ead_corrigido))
        cea_final_esta_na_carteira = np.where(array_esta_na_carteira == True, cea_final_esta_na_carteira, 0)    

        #CALCULO DO CAPITAL CONSIDERANDO A NOVA OPERACAO NA CARTEIRA
        cea_final_nao_esta_na_carteira = np.where(array_default == 'FALSE', self.calcula_capital_final_cluster_nao_default(pe_liq, cea_puro, array_pd_modelo, array_duration, array_prazo_simulado, array_k_grupo_alterado, array_ead_corrigido), self.calcula_capital_final_cluster_default(pe_liq_default, cea_puro, array_ead_corrigido))

        #CAPITAL CONSIDERANDO PROBABILIDADE DE MIGRACAO
        cea_final_esta_na_carteira_migracao = cea_final_esta_na_carteira * array_probabilidade_migracao
        cea_final_nao_esta_na_carteira_migracao = cea_final_nao_esta_na_carteira * array_probabilidade_migracao

        #VETOR DE RESULTA COM PERDA ESPERADA
        array_cea_esta_na_carteira_migracao = np.where(array_is_vencida == False, cea_final_esta_na_carteira_migracao, 0)
        array_cea_nao_esta_na_carteira_migracao = np.where(array_is_vencida == False, cea_final_nao_esta_na_carteira_migracao, 0)
            
        #-----------------------------------------------------------------------------------------------------------------------------#
            
        ####CLUSTER SEM PERDA ESPERADA####

        #CALULO DO CAPITAL ALOCADO NA CARTEIRA SEM INCLUSAO DA NOVA OPERACAO
        cea_final_esta_na_carteira_sem_pe = np.where(array_default == 'FALSE', self.calcula_capital_final_cluster_nao_default_sem_pe(0, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo, array_ead_corrigido_sem_pe), self.calcula_capital_final_cluster_default(pe_liq_sem_pe, cea_puro_sem_pe, array_ead_corrigido_sem_pe))
        cea_final_esta_na_carteira_sem_pe = np.where(array_esta_na_carteira == True, cea_final_esta_na_carteira_sem_pe, 0)  
         
        #CALCULO DO CAPITAL CONSIDERANDO A NOVA OPERACAO NA CARTEIRA
        cea_final_nao_esta_na_carteira_sem_pe = np.where(array_default == 'FALSE', self.calcula_capital_final_cluster_nao_default_sem_pe(0, cea_puro_sem_pe, array_pd_modelo, array_duration, array_prazo_simulado , array_k_grupo_alterado_sem_pe, array_ead_corrigido_sem_pe,), self.calcula_capital_final_cluster_default(pe_liq_sem_pe, cea_puro_sem_pe, array_ead_corrigido_sem_pe))

        #CAPITAL CONSIDERANDO PROBABILIDADE DE MIGRACAO
        cea_final_esta_na_carteira_migracao_sem_pe = cea_final_esta_na_carteira_sem_pe * array_probabilidade_migracao
        cea_final_nao_esta_na_carteira_migracao_sem_pe = cea_final_nao_esta_na_carteira_sem_pe * array_probabilidade_migracao
        
        #VETOR DE RESULTADOS SEM PERDA ESPERADA
        array_cea_esta_na_carteira_migracao_sem_pe = np.where(array_is_vencida == False, cea_final_esta_na_carteira_migracao_sem_pe, 0)
        array_cea_nao_esta_na_carteira_migracao_sem_pe = np.where(array_is_vencida == False, cea_final_nao_esta_na_carteira_migracao_sem_pe, 0)

        #CLUSTER
        kreg_op = np.where(array_is_vencida == False, kreg_op_sem_filtro, 0)
        pe_liq_migracao = np.where(array_is_vencida == False, pe_liq_migracao_sem_filtro, 0)

        #Iniciando os vetores para salvar as respostas
        vetor_soma_esta_na_carteira = []
        vetor_soma_esta_na_carteira_sem_pe = []
        vetor_soma_nao_esta_na_carteira = []
        vetor_soma_nao_esta_na_carteira_sem_pe = []
        vetor_perda_esperada = []
        vetor_kreg = []

        np_esta_na_carteira = np.empty(shape = [0, 2])
        np_nao_esta_na_carteira = np.empty(shape = [0, 2])
        np_esta_na_carteira_sem_pe = np.empty(shape = [0, 2])
        np_nao_esta_na_carteira_sem_pe = np.empty(shape = [0, 2])
        np_completo = np.empty(shape = [0, 2])
        np_pe = np.empty(shape = [0, 2])
        np_kreg = np.empty(shape = [0, 2])

        #Iniciando as variaveis auxiliares            
        soma_final = 0
        i = 0

        for prazo in array_prazos:   
            
            #Vetor com PE
            vetor_soma_esta_na_carteira.append(array_cea_esta_na_carteira_migracao[array_prazo_simulado == prazo].sum())
            np_esta_na_carteira = np.append(np_esta_na_carteira, [[prazo, vetor_soma_esta_na_carteira[i]]], axis=0)
            
            #Vetor sem PE
            vetor_soma_esta_na_carteira_sem_pe.append(array_cea_esta_na_carteira_migracao_sem_pe[array_prazo_simulado == prazo].sum())
            np_esta_na_carteira_sem_pe = np.append(np_esta_na_carteira_sem_pe, [[prazo, vetor_soma_esta_na_carteira_sem_pe[i]]], axis=0)
            
            #Vetor com PE   
            vetor_soma_nao_esta_na_carteira.append(array_cea_nao_esta_na_carteira_migracao[array_prazo_simulado == prazo].sum())
            np_nao_esta_na_carteira = np.append(np_nao_esta_na_carteira, [[prazo, vetor_soma_nao_esta_na_carteira[i]]], axis=0)

            #Vetor sem PE 
            vetor_soma_nao_esta_na_carteira_sem_pe.append(array_cea_nao_esta_na_carteira_migracao_sem_pe[array_prazo_simulado == prazo].sum())
            np_nao_esta_na_carteira_sem_pe = np.append(np_nao_esta_na_carteira_sem_pe, [[prazo, vetor_soma_nao_esta_na_carteira_sem_pe[i]]], axis=0)
            
            #PE
            #filtro_pe = np.where(array_rating == array_rating_original, pe_liq_migracao, 0)
            vetor_perda_esperada.append(pe_liq_migracao[array_prazo_simulado == prazo].sum())
            np_pe = np.append(np_pe, [[prazo, vetor_perda_esperada[i]]], axis=0)

            #KREG
            #filtro_kreg = np.where(array_prazo_simulado == prazo, pe_liq_migracao, 0)
            vetor_kreg.append((kreg_op[array_prazo_simulado == prazo].sum())/25)
            np_kreg = np.append(np_kreg, [[prazo, vetor_kreg[i]]], axis=0)           
            i += 1

        df_esta_na_carteira = pd.DataFrame(np_esta_na_carteira, columns = ['Prazo', 'Capital_na_carteira'])
        df_esta_na_carteira_sem_pe = pd.DataFrame(np_esta_na_carteira_sem_pe, columns = ['Prazo', 'Capital_na_carteira_sem_PE'])
        df_esta_na_carteira_completo = pd.merge(df_esta_na_carteira, df_esta_na_carteira_sem_pe, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        
        df_nao_esta_na_carteira = pd.DataFrame(np_nao_esta_na_carteira, columns = ['Prazo', 'Capital_nao_na_carteira'])
        df_nao_esta_na_carteira_sem_pe = pd.DataFrame(np_nao_esta_na_carteira_sem_pe, columns = ['Prazo', 'Capital_nao_na_carteira_sem_PE'])
        df_nao_esta_na_carteira_completo = pd.merge(df_nao_esta_na_carteira, df_nao_esta_na_carteira_sem_pe, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        
        df_perda_esperada = pd.DataFrame(np_pe, columns = ['Prazo', 'Perda_esperada'])

        df_kreg = pd.DataFrame(np_kreg, columns = ['Prazo', 'Kreg'])

        df_completo = pd.merge(df_esta_na_carteira_completo, df_nao_esta_na_carteira_completo, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        df_completo_com_pe = pd.merge(df_completo, df_perda_esperada, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        df_completo_kreg = pd.merge(df_completo_com_pe, df_kreg, how = 'left', left_on = ['Prazo'], right_on = ['Prazo'])
        np_completo = df_completo_kreg.values
        return [np_completo, np_monstro]