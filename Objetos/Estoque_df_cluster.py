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

class Estoque_df_cluster:
    def __init__(self, matriz_migracao, regua_pd, cea_globais, curvas_juros, estoque_grupo, cnpj_cabeca, rating_grupo, rating_operacao, id_segmento, id_grupo, id_cenario_migracao_global, id_volatilidade, prazo):   

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

    ######FINAL FUNCOES PARA CALCULO DE CAPITAL######
    def gerar_estoque_completo_cluster(self, nova, df_ead, vertices, flag): 
        df_monstro = pd.DataFrame()
        df_prazo = pd.DataFrame()
        df_k_grupo_prazo = pd.DataFrame()
        array_prazos = self.gerar_array_prazo(vertices)
        vetor_rating = get_vetor_rating() 
        matriz_fpr = self.gerar_array_fpr(vertices)
        self.df_estoque = self.estoque_grupo.df_operacoes
        
        if(flag == 2):  
            self.df_estoque = self.filtro_cluster() #Filtro de cluster (flag 2), desconsidera a operacao nova e calcula o cluster apenas para as operacoes da carteira
        
        try:
            id_segmento_risco_grupo = self.df_estoque.id_segmento_risco_grupo.values[0]
        except:
            return print("Calculo nao pode ser finalizado")

        array_rtg = Regua_pd.get_array_np(self.regua_pd, id_segmento_risco_grupo)   #rating, pd_modelo, wi, lgd_dt, lgd_be
        df_rtg = pd.DataFrame(array_rtg, columns = ['Rating','Pd_modelo','Wi','Lgd_dt','Lgd_be', 'Default'])
        
        #VARIAVEL AUXILIAR PARA ACESSAR INDICES DA MATRIZ DE FPR   
        i = 0

        for prazo in array_prazos:
            self.df_estoque['prazo_sim'] = prazo
            self.df_estoque['k_grupo'] = self.get_k_grupo_sem_nova_op(prazo)
            self.df_estoque['fator_ead'] = self.atualiza_estoque(prazo)
            ead_novo = df_ead.loc[df_ead['prazo'] == prazo, 'ead_op_novas'].values
            self.df_estoque.loc[self.df_estoque['esta_na_carteira'] == False, 'ead'] = ead_novo
            self.df_estoque['k_grupo_alterado'] = self.get_k_grupo_lote(prazo)
            self.df_estoque['k_grupo_alterado_sem_pe'] = self.get_k_grupo_lote_sem_pe(prazo)
            self.df_estoque["fpr"] = np.where(matriz_fpr[i][0] == prazo, matriz_fpr[i][1], 0) 
            i += 1
            df_prazo = df_prazo.append(self.df_estoque, ignore_index=True)
                    
        #pegar info de todos os ratings
        for rtg in vetor_rating:
            df_prazo['rtg_mig'] = rtg
            df_monstro = df_monstro.append(df_prazo,ignore_index = True)
        
        rating_op_nova = Tela.get_rating_grupo(self.tela)

        self.mudar_rating_nivel_grupo_lote(df_monstro, rating_op_nova,self.rating_grupo)
        
        self.is_vencida_df_lote(df_monstro, df_monstro['prazo_sim'])

        df_monstro = pd.merge(df_monstro, df_rtg, how = 'left', left_on = ['rating'], right_on = ['Rating'])    #informacoes vindas dos ratings

        df_monstro['prazo_mes'] = np.floor(df_monstro['prazo_sim']/30)
        df_monstro.loc[(df_monstro['prazo_mes'] > 36), 'prazo_mes'] = 36
        
        df_matriz = self.matriz_migracao.df
        
        df_monstro = pd.merge(df_monstro, df_matriz, how = 'left', left_on = ['rating','prazo_mes'], right_on = ['rating_final','mes_ref'])
        df_monstro = df_monstro[['rating', 'Pd_modelo', 'Wi', 'Lgd_dt', 'Lgd_be', 'Default', 'probabilidade_migracao', 'lgd', 'ead', 'is_vencida', 'k_grupo','prazo_mes', 'prazo_sim', 'duration', 'k_grupo_alterado', 'esta_na_carteira', 'rating_original', 'fator_ead', 'condicao_mudar_rtg', 'id_grupo','k_grupo_alterado_sem_pe','incluir_no_cluster', 'fpr']]
    
        if(self.rating_grupo != rating_op_nova):
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == rating_op_nova), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != rating_op_nova), 'probabilidade_migracao'] = 0
            
        else:
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] == self.rating_grupo), 'probabilidade_migracao'] = 1
            df_monstro.loc[(df_monstro['prazo_mes'] == 0) & (df_monstro['rating'] != self.rating_grupo), 'probabilidade_migracao'] = 0
            

        np_monstro = df_monstro.values
        
        array_fpr = np_monstro[:,22].astype(float)

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
        #teste = np.where((array_rating == rating_op_nova) & (array_prazo_mes == 0), cea_puro, 0)
        #res = teste.sum()
        cea_puro_sem_pe = np.where(array_default == 'TRUE', self.calcula_capital_default(array_ead_corrigido_sem_pe,array_lgd_dt,array_lgd_be), self.calcula_capital_nao_default(array_pd_modelo,array_lgd,array_ead_corrigido_sem_pe, array_wi))
            
        #CALCULO DE PARAMETROS AUXILIARES 
        pe_liq = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido
        pe_liq_default = get_fatorpeliq() * array_pd_modelo * array_lgd_be * array_ead_corrigido
        pe_liq_sem_pe = get_fatorpeliq() * array_pd_modelo * array_lgd * array_ead_corrigido_sem_pe
        pe_liq_migracao_sem_filtro = pe_liq * array_probabilidade_migracao
            
        fpr = get_fpr_cnpj(Tela.get_id_segmento_risco_grupo(self.tela)) #MUDAR FUNCAO PARA PEGAR FPR POR PRAZO E CNPJ
        #fpr = array_fpr    #CHECAR NECESSIDADE DE MUDAR PARA O CLUSTER
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