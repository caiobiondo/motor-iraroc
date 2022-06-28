from Objetos.Tela import Tela
from Objetos.Fluxo import Fluxo
from Objetos.Estoque_df import Estoque_df
from ToolKitDB.Parametros import *
from ToolKitDB.Veiculo_legal import Veiculo_legal
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Matriz_migracao import Matriz_migracao
from ToolKitDB.Regua_pd import Regua_pd
from ToolKitDB.Produto_modalidade import Produto_modalidade
from ToolKitDB.Alfas_betas import Alfas_betas
from ToolKitDB.Moeda_conversao import Moeda_conversao
from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Vetor_operacao_df import *
from ToolKitDB.Parametros import *
from ToolKitDB.CVA import *
from ToolKitDB.FEPF import *
from Capital.Capital import *
import numpy as np
import time
import datetime
import sqlite3
from datetime import *
from dateutil.relativedelta import relativedelta

class Vertice_operacao:
    def __init__(self,connDB, moeda_conversao, alfas_betas, produto_modalidade, regua_pd, matriz_migracao, veiculo_legal, toolkit_finance, vertice_anterior, fluxo, estoque, tela, genesis_vertex, termination_vertex, id_vertex, operacoes):
       
        #novos objetos criados
        self.moeda_conversao = moeda_conversao
        self.connDB = connDB
        self.alfas_betas = alfas_betas
        self.produto_modalidade = produto_modalidade
        self.regua_pd = regua_pd
        self.matriz_migracao = matriz_migracao
        self.veiculo_legal = veiculo_legal
        self.toolkit_finance = toolkit_finance
        self.fluxo = fluxo
        self.tela = tela
        self.estoque = estoque
        self.operacoes = operacoes
        #self.conn_seguro = sqlite3.connect("Bases/base_seguro.db")

        #Objetos que ja estavam na classe
        self.id_vertex = id_vertex
        self.id_cenario_migracao_global = Tela.get_id_cenario(self.tela)
        self.indexador = Tela.get_indexador(self.tela)
        self.formato_taxa = Tela.get_formato_taxa(self.tela)
        self.prazo_total = Tela.get_prazo_total(self.tela)
        self.tx_cliente = Tela.get_taxa_cliente(self.tela)
        self.tx_ref = Tela.get_taxa_ref(self.tela)
        self.commitment_spread = Tela.get_commitment_spread(self.tela)
        self.notional = Tela.get_notional(self.tela)
        self.valor_limite_linha = float(Tela.get_valor_limite_linha(self.tela))
        self.indexador_garantia = Tela.get_indexador_garantia(self.tela)
        self.rating_operacao = Tela.get_rating_operacao(self.tela).upper()
            
        if(Tela.is_spread_variavel(self.tela)):
            self.spread = Fluxo.get_spread(self.fluxo)/100
        else:
            self.spread = Tela.get_spread(self.tela)
        
        self.ccf_cea = 0
        self.ccf_kreg = 0

        self.mitigacao_cea = min(max(Fluxo.get_perc_mitigacao(self.fluxo), Tela.get_mitigacao_cea(self.tela)), 0.9)
        
        if(Tela.get_is_mitigacao_fluxo(self.tela)):
            self.mitigacao_cea_2 = min(max(Fluxo.get_perc_mitigacao(self.fluxo),0),1)
        else:
            self.mitigacao_cea_2 = min(max(Tela.get_mitigacao_cea_2(self.tela),0),1)
  
        self.lgd_mitigada = Tela.get_lgd(self.tela) * (1 - self.mitigacao_cea)
        self.lgd_mitigada_2 = Tela.get_lgd(self.tela) * (1 - self.mitigacao_cea_2)
        self.mitigacao_tela_kreg = Tela.get_mitigacao_kreg(self.tela)

        self.is_prospectivo = Tela.get_prospectivo(self.tela)
        self.id_segmento_grupo = Tela.get_id_segmento_risco_grupo(self.tela)
        self.produto = Tela.get_produto(self.tela)
        self.modalidade = Tela.get_modalidade(self.tela)
        self.prazo_decorrido = Fluxo.get_dc(self.fluxo)
        self.prazo_decorrer = self.prazo_total - self.prazo_decorrido
        self.dias_ate_proximo_vertice = 0
        self.indexador_ativo = self.indexador
        self.indexador_passivo = self.indexador

        if(not(Tela.is_fpr_variavel(self.tela))): 
            #TRATATIVA FPR VARIAVEL 1/1/2023
            data_limite = date(2023, 1, 1)
            data_inicio = date.today()
            data_nova = data_inicio + relativedelta(days=+self.prazo_decorrido)
            dif = data_limite - data_nova
            if(dif.days <= 0):
                self.fpr = Tela.get_fpr_pos(self.tela)
            elif(dif.days > 0):
                self.fpr = Tela.get_fpr_pre(self.tela)
            #FINAL DA TRATATIVA
        else:
            self.fpr = Fluxo.get_fpr(self.fluxo)

        self.saldo_nao_sacado_rotativo = 0

        self.saldo_ativo_abertura = 0
        self.juros_acumulados = 0
        self.saldo_ativo_fechamento = 0
        self.saldo_passivo_abertura = 0
        self.juros_acumulados_passivos = 0
        self.saldo_passivo_fechamento = 0
        self.saldo_risco_termo = 0
        self.pagamento_ativo = 0
        self.pagamento_passivo = 0
        self.pagamento_juros_ativo = 0
        self.pagamento_juros_passivo = 0

        self.mfb = 0
        self.receita_comissao = 0
        self.receita_termo = 0
        self.receita_comprometida_rotativos = 0

        self.ead_cea = 0
        self.ead_kreg = 0
        self.ead_cea_cluster = 0

        self.kreg = 0
        self.cea = 0
        self.cea_cluster = 0
        self.mfb_estoque = 0
        self.mfb_x_sell = 0

        self.pe = 0
        self.pe_cluster = 0

        self.id_vol = Estoque_df.get_id_volatilidade(self.estoque)
        self.has_migracao = Estoque_df.get_has_migracao(self.estoque)

        self.prazo_bis = max(self.prazo_total, 1800)

        self.genesis = genesis_vertex
        self.termination_vertex = termination_vertex
        self.vertice_anterior = vertice_anterior

        self.valor_desembolso = Fluxo.get_valor_desembolso(self.fluxo)
        self.valor_amortizacao = Fluxo.get_valor_amortizacao(self.fluxo)

        self.is_pagamento_juros = Fluxo.get_pagamento_juros(self.fluxo)

        self.pdd_estoque = 0
        self.prz_estoque = 0
        self.lgd_estoque = 0
        
        if(self.is_genesis_vertex()):
            self.valor_desembolso_acumulado = self.valor_desembolso
            self.valor_amortizacao_acumulado = self.valor_amortizacao
        else:
            self.valor_desembolso_acumulado = self.valor_desembolso + Vertice_operacao.get_valor_desembolso_acumulado(self.vertice_anterior)
            self.valor_amortizacao_acumulado = self.valor_amortizacao + Vertice_operacao.get_valor_amortizacao_acumulado(self.vertice_anterior)
        
    #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def calcula_posicao_retorno(self):  #OK
        if(Tela.is_credito_ativo(self.tela) or Tela.is_derivativo(self.tela) or Tela.is_rotativo(self.tela)):
            if(self.is_genesis_vertex()):
                self.saldo_ativo_abertura = self.valor_desembolso
                self.juros_acumulados_ativo = 0
                self.saldo_ativo_fechamento = self.valor_desembolso

                self.saldo_passivo_abertura = self.valor_desembolso_acumulado
                self.juros_acumulados_passivo = 0
                self.saldo_passivo_fechamento = self.saldo_passivo_abertura
            else:
                if(self.formato_taxa == "LIN/360"):
                    self.juros_ativos = (Vertice_operacao.get_valor_desembolso_acumulado(self.vertice_anterior) - Vertice_operacao.get_valor_amortizacao_acumulado(self.vertice_anterior)) * (Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, self.vertice_anterior.get_prazo_decorrido(), self.prazo_decorrido, self.spread,Tela.get_taxa_cliente(self.tela)) - 1)
                    self.juros_passivos = (Vertice_operacao.get_valor_desembolso_acumulado(self.vertice_anterior) - Vertice_operacao.get_valor_amortizacao_acumulado(self.vertice_anterior)) * (Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, self.vertice_anterior.get_prazo_decorrido(), self.prazo_decorrido, 0,Tela.get_taxa_cliente(self.tela)) - 1)

                    self.juros_acumulados_ativo = Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior) + self.juros_ativos
                    self.juros_acumulados_passivo = Vertice_operacao.get_juros_acumulados_passivo(self.vertice_anterior) + self.juros_passivos
                    
                    self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) + self.juros_ativos
                    self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) + self.juros_passivos

                    self.saldo_ativo_fechamento = self.saldo_ativo_abertura
                    self.saldo_passivo_fechamento = self.saldo_passivo_abertura

                    if(self.is_pagamento_juros):
                        self.mfb = self.juros_acumulados_ativo - self.juros_acumulados_passivo

                        self.saldo_ativo_fechamento -= self.juros_acumulados_ativo
                        self.saldo_passivo_fechamento -= self.juros_acumulados_passivo

                        self.juros_acumulados_ativo = 0
                        self.juros_acumulados_passivo = 0
                    
                elif(self.formato_taxa == "EXP/360" or self.formato_taxa == "EXP/252"):
                    self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, self.spread, Tela.get_taxa_cliente(self.tela))
                    self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, 0, Tela.get_taxa_ref(self.tela))

                    self.juros_acumulados_ativo = (self.saldo_ativo_abertura - Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)
                    self.juros_acumulados_passivo = (self.saldo_passivo_abertura - Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_passivo(self.vertice_anterior)
                    
                    self.saldo_ativo_fechamento = self.saldo_ativo_abertura
                    self.saldo_passivo_fechamento = self.saldo_passivo_abertura

                    if(self.is_pagamento_juros):
                        self.mfb = self.juros_acumulados_ativo - self.juros_acumulados_passivo
                        self.saldo_ativo_fechamento -= self.juros_acumulados_ativo
                        self.saldo_passivo_fechamento -= self.juros_acumulados_passivo

                        self.juros_acumulados_ativo = 0
                        self.juros_acumulados_passivo = 0

                    else:
                        self.juros_acumulados_ativo = (self.saldo_ativo_abertura - Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)
                        self.juros_acumulados_passivo = (self.saldo_passivo_abertura - Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_passivo(self.vertice_anterior)

                else:
                    if(self.formato_taxa == "%CDI"):
                        self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, self.spread, Tela.get_taxa_cliente(self.tela))
                        self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, 0, Tela.get_taxa_ref(self.tela))
    
                    elif(self.formato_taxa == "EXP/252M"):
                        self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, "EXP/252", self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_cliente(self.tela), Tela.get_taxa_cliente(self.tela))
                        self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_ativo, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_ref(self.tela), Tela.get_taxa_ref(self.tela))
                    
                    elif(self.formato_taxa == "IPCA"):
                        self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, "IPCA", "IPCA", Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_cliente(self.tela), Tela.get_taxa_cliente(self.tela))
                        self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, "IPCA", "IPCA", Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_ref(self.tela), Tela.get_taxa_ref(self.tela))
                    

                    elif(self.formato_taxa == "FLAT"):
                        self.saldo_ativo_abertura = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, "FLAT", "FLAT", Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_cliente(self.tela), Tela.get_taxa_cliente(self.tela))
                        self.saldo_passivo_abertura = Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior) * Toolkit_finance.get_accrual(self.toolkit_finance, "FLAT", "FLAT", Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, Tela.get_taxa_ref(self.tela), Tela.get_taxa_ref(self.tela))
                    
                    self.juros_acumulados_ativo = (self.saldo_ativo_abertura - Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)
                    self.juros_acumulados_passivo = (self.saldo_passivo_abertura - Vertice_operacao.get_saldo_passivo_fechamento(self.vertice_anterior)) + Vertice_operacao.get_juros_acumulados_passivo(self.vertice_anterior)

                    self.saldo_ativo_fechamento = self.saldo_ativo_abertura
                    self.saldo_passivo_fechamento = self.saldo_passivo_abertura

                    if(self.is_pagamento_juros):
                        if(Tela.get_modalidade(self.tela) == "12431" and (self.formato_taxa == "FLAT" or self.formato_taxa == "IPCA" or self.formato_taxa == "EXP/252M" or self.formato_taxa == "%CDI")):
                            self.mfb = (self.juros_acumulados_ativo * ajuste_12431(0.25, 0.15, Veiculo_legal.get_aliquota_ir_cs(self.veiculo_legal) - 0.25, get_piscofins())) - self.juros_acumulados_passivo
                        else:
                            self.mfb = self.juros_acumulados_ativo - self.juros_acumulados_passivo
                        
                        self.saldo_ativo_fechamento -= self.juros_acumulados_ativo
                        self.saldo_passivo_fechamento -= self.juros_acumulados_passivo

                        self.pagamento_ativo = self.juros_acumulados_ativo + self.valor_amortizacao
                        self.pagamento_passivo = self.juros_acumulados_passivo + self.valor_amortizacao

                        self.pagamento_juros_ativo = self.juros_acumulados_ativo
                        self.pagamento_juros_passivo = self.juros_acumulados_passivo

                        self.juros_acumulados_ativo = 0
                        self.juros_acumulados_passivo = 0

                self.saldo_ativo_fechamento += self.valor_desembolso - self.valor_amortizacao
                self.saldo_passivo_fechamento += self.valor_desembolso - self.valor_amortizacao

        if(Tela.is_garantia_prestada(self.tela)):
            self.saldo_passivo_abertura = 0
            self.juros_acumulados_passivo = 0
            self.saldo_passivo_fechamento = 0

            if(self.is_genesis_vertex()):
                self.saldo_ativo_abertura = self.valor_desembolso
                self.juros_acumulados_ativo = 0
                self.saldo_ativo_fechamento = self.valor_desembolso
            
            else:
                teste = Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior)
                self.saldo_ativo_abertura = (Vertice_operacao.get_saldo_ativo_fechamento(self.vertice_anterior)) * Toolkit_finance.get_accrual(self.toolkit_finance, self.formato_taxa, self.indexador_garantia, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido, 0,  Tela.get_taxa_cliente(self.tela))

                self.juros_acumulados_ativo = self.saldo_ativo_abertura * Toolkit_finance.diarizar_spread(self.toolkit_finance, self.formato_taxa, self.spread, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido) + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)

                if(self.is_pagamento_juros):
                    self.receita_comissao = self.juros_acumulados_ativo
                    self.juros_acumulados_ativo = 0
                
                self.saldo_ativo_fechamento = (self.get_valor_desembolso_acumulado() - self.get_valor_amortizacao_acumulado()) * Toolkit_finance.get_accrual(self.toolkit_finance, "EXP/360", self.indexador_garantia, 0, self.prazo_decorrido, 0,  Tela.get_taxa_cliente(self.tela))

        #risco a termo
        self.receita_termo_acumulada = 0

        if(Tela.is_credito_ativo(self.tela) or Tela.is_garantia_prestada(self.tela)):
            self.saldo_risco_termo = self.obter_risco_termo(self.prazo_total)

            if(not(self.is_genesis_vertex())):
                spread = Toolkit_finance.diarizar_spread(self.toolkit_finance, self.formato_taxa, self.commitment_spread, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido)
                self.receita_termo_acumulada = Vertice_operacao.get_receita_termo_acumulada(self.vertice_anterior) + Vertice_operacao.get_saldo_risco_termo(self.vertice_anterior) * Toolkit_finance.diarizar_spread(self.toolkit_finance, self.formato_taxa, self.commitment_spread, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido)

                if (self.is_pagamento_juros):
                    self.receita_termo = self.receita_termo_acumulada
                    self.receita_termo_acumulada = 0
        
        #rotativo
        self.receita_rotativos_acumulada = 0

        if(Tela.is_rotativo(self.tela)):
            self.saldo_nao_sacado_rotativo = max(self.valor_limite_linha - self.saldo_ativo_abertura, 0)

            if(not(self.is_genesis_vertex())):
                self.receita_rotativos_acumulada = Vertice_operacao.get_receita_rotativos_acumulada(self.vertice_anterior) + Vertice_operacao.get_saldo_nao_sacado_rotativo(self.vertice_anterior) * Toolkit_finance.diarizar_spread(self.toolkit_finance, self.formato_taxa, self.commitment_spread, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido)

                if(self.is_pagamento_juros):
                    self.receita_comprometida_rotativos = self.receita_rotativos_acumulada
                    self.receita_rotativos_acumulada = 0

    def calcula_posicao_risco(self):    #OK
        if(Tela.is_credito_ativo(self.tela)):
        
            if(self.is_genesis_vertex()):
                self.ead_cea = self.saldo_ativo_fechamento
                self.ead_kreg = self.saldo_ativo_fechamento
            else:
                self.ead_cea = self.saldo_ativo_fechamento + Vertice_operacao.get_receita_termo_acumulada(self.vertice_anterior)
                self.ead_kreg = self.saldo_ativo_fechamento + Vertice_operacao.get_receita_termo_acumulada(self.vertice_anterior)
            
            
            if(Tela.get_ccf_manual(self.tela)):
                if(Tela.get_onshore(self.tela)):
                    if(ishighyield_ccf(self.rating_operacao)):
                        self.ead_cea += self.obter_risco_termo(359) * self.tela.ccf_cea
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * self.tela.ccf_cea
                    else:
                        self.ead_cea += (self.obter_risco_termo(359)) * self.tela.ccf_cea
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * self.tela.ccf_cea
                
                else:
                    
                    if(ishighyield_ccf(self.rating_operacao)):

                        self.ead_cea += self.obter_risco_termo(359) *self.tela.ccf_cea

                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * self.tela.ccf_cea

                    else:

                        self.ead_cea += self.obter_risco_termo(359) * self.tela.ccf_cea
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * self.tela.ccf_cea
            
            else:
                if(Tela.get_onshore(self.tela)):
                    if(ishighyield_ccf(self.rating_operacao)):
                        self.ead_cea += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_hy_cea(self.produto_modalidade)
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_hy_cea(self.produto_modalidade)
                    else:
                        self.ead_cea += float(float(self.obter_risco_termo(359)) * float(Produto_modalidade.get_ccf_l1y_ig_cea(self.produto_modalidade)))
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_ig_cea(self.produto_modalidade)
                
                else:
                    
                    if(ishighyield_ccf(self.rating_operacao)):

                        self.ead_cea += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_hy_cea_off(self.produto_modalidade)

                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_hy_cea_off(self.produto_modalidade)

                    else:

                        self.ead_cea += float(float(self.obter_risco_termo(359)) * float(Produto_modalidade.get_ccf_l1y_ig_cea_off(self.produto_modalidade)))
                        self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_ig_cea_off(self.produto_modalidade)

            self.ead_cea = self.ead_cea * (1 - Produto_modalidade.get_mitigacaoead_cea(self.produto_modalidade))

            self.ead_kreg = self.saldo_ativo_fechamento
            
            if(Tela.get_ccf_manual(self.tela)):
                
                if(ishighyield_ccf(self.rating_operacao)):
                    self.ead_kreg += self.obter_risco_termo(359) * self.tela.ccf_kreg

                    if(Tela.get_ccf_manual(self.tela)):
                        self.ead_kreg += self.obter_risco_termo(359) * self.tela.ccf_kreg
                else:

                    self.ead_kreg += self.obter_risco_termo(359) * self.tela.ccf_kreg
                    
                    if(Tela.get_ccf_manual(self.tela)):
                        self.ead_kreg += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * self.tela.ccf_kreg

                    self.ead_kreg = self.ead_kreg * (1 - Produto_modalidade.get_mitigacaokreg(self.produto_modalidade))
            
            else:
                if(ishighyield_ccf(self.rating_operacao)):
                    self.ead_kreg += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_hy_kreg(self.produto_modalidade)

                    if(Tela.get_ccf_manual(self.tela)):
                        self.ead_kreg += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_m1y_hy_kreg(self.produto_modalidade)
            
                else:

                    self.ead_kreg += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_ig_kreg(self.produto_modalidade)
                    
                    if(Tela.get_ccf_manual(self.tela)):
                        self.ead_kreg += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_ig_kreg(self.produto_modalidade)

                    self.ead_kreg = self.ead_kreg * (1 - Produto_modalidade.get_mitigacaokreg(self.produto_modalidade))
        
        #PROBLEMA NO CALCULO DE DERIVATIVOS !
        if(Tela.is_derivativo(self.tela)):
            alpha_a = Alfas_betas.get_alpha_ativo(self.alfas_betas)
            alpha_p = Alfas_betas.get_alpha_passivo(self.alfas_betas)
            beta_a = Alfas_betas.get_beta_ativo(self.alfas_betas)
            beta_p = Alfas_betas.get_alpha_passivo(self.alfas_betas)
            rebate = Alfas_betas.get_rebate(self.alfas_betas)

            if(np.isnan(alpha_a)):
                alpha_p = Alfas_betas.get_alpha_ativo(self.alfas_betas)
                alpha_a = Alfas_betas.get_alpha_passivo(self.alfas_betas)
                beta_p = Alfas_betas.get_alpha_ativo(self.alfas_betas)
                beta_a = Alfas_betas.get_alpha_passivo(self.alfas_betas)
                rebate = Alfas_betas.get_rebate(self.alfas_betas)
            
            cva = 1 + (calcular_cva(self.prazo_decorrer, self.fpr, self.connDB)/100) 
            fepf = (get_fepf(self.prazo_decorrer,Tela.get_classe_derivativo(self.tela),self.connDB))/100

            if(Tela.get_is_rcp_cte(self.tela)):
                valor_rcp = Tela.get_rcp(self.tela)
            
            else:
                valor_rcp = bba_rcp(self.get_prazo_decorrer(), float(alpha_a), float(beta_a))
                valor_rcp_1 = bba_rcp(self.get_prazo_decorrer(), float(alpha_p), float(beta_p))

                if(valor_rcp > (valor_rcp_1 * rebate)):
                    valor_rcp = valor_rcp
                else:
                    valor_rcp = (valor_rcp_1 * rebate)
                
                valor_rcp_flat = bba_rcp(Tela.get_prazo_total(self.tela), float(alpha_a), float(beta_a))
                valor_rcp_1_flat = bba_rcp(Tela.get_prazo_total(self.tela), float(alpha_p), float(beta_p))
            
                if(valor_rcp_flat > (valor_rcp_1_flat * rebate)):
                    valor_rcp_flat = valor_rcp_flat
                else:
                    valor_rcp_flat = (valor_rcp_1_flat * rebate)
                
                if(abs(Tela.get_rcp(self.tela) - valor_rcp_flat) > 0.02):
                    fator_1 = Tela.get_rcp(self.tela)/valor_rcp_flat
                else:
                    fator_1 = 1
                
                valor_rcp = valor_rcp * fator_1
            
            if(self.is_genesis_vertex()):
                self.ead_cea = self.saldo_passivo_fechamento * valor_rcp * cva
                self.ead_kreg = self.saldo_passivo_fechamento * cva * fepf
            else:
                self.ead_cea = (self.saldo_passivo_fechamento * valor_rcp + (self.juros_acumulados_ativo - self.juros_acumulados_passivo)) * cva
                self.ead_kreg = self.saldo_passivo_fechamento * cva * fepf
            
            self.ead_cea = self.ead_cea * (1 - Produto_modalidade.get_mitigacaoead_cea(self.produto_modalidade))
        
        #MUDANCAS DO MIDDLE PARA PRODUTOS DE CLASSE ROTATIVO
        if(Tela.is_rotativo(self.tela)):

            if(self.is_genesis_vertex()):
                self.ead_cea = self.saldo_ativo_fechamento
                self.ead_kreg = self.saldo_ativo_fechamento
            else:
                self.ead_cea = self.saldo_ativo_fechamento + Vertice_operacao.get_receita_rotativos_acumulada(self.vertice_anterior)
                self.ead_kreg = self.saldo_ativo_fechamento + Vertice_operacao.get_receita_rotativos_acumulada(self.vertice_anterior)
            
            ccf_rot = 0

            if(Tela.get_onshore(self.tela)):
                if(ishighyield_ccf(self.rating_operacao)):
                    if(self.prazo_total < 360):
                        ccf_rot = Produto_modalidade.get_ccf_l1y_hy_cea(self.produto_modalidade)
                    else:
                        ccf_rot = Produto_modalidade.get_ccf_m1y_hy_cea(self.produto_modalidade)
                
                else:
                    if(self.prazo_total < 360):
                        ccf_rot = Produto_modalidade.get_ccf_l1y_ig_cea(self.produto_modalidade)
                    else:
                        ccf_rot = Produto_modalidade.get_ccf_m1y_ig_cea(self.produto_modalidade)
            else:
                if(ishighyield_ccf(self.rating_operacao)):
                    if(self.prazo_total < 360):
                        ccf_rot = Produto_modalidade.get_ccf_l1y_hy_cea_off(self.produto_modalidade)
                    else:
                        ccf_rot = Produto_modalidade.get_ccf_m1y_hy_cea_off(self.produto_modalidade)
                else:
                    if(self.prazo_total < 360):
                        ccf_rot = Produto_modalidade.get_ccf_l1y_ig_cea_off(self.produto_modalidade)
                    else:
                        ccf_rot = Produto_modalidade.get_ccf_m1y_ig_cea_off(self.produto_modalidade)
            
            self.ead_cea = min(self.ead_cea + ccf_rot * self.valor_limite_linha, max(1.05 * self.valor_limite_linha, self.ead_cea))
            self.ead_cea = self.ead_cea * (1 - Produto_modalidade.get_mitigacaoead_cea(self.produto_modalidade))
            self.ead_kreg = self.saldo_ativo_fechamento

            if(ishighyield_ccf(self.rating_operacao)):
                if(self.prazo_total < 360):
                    self.ead_kreg += self.saldo_nao_sacado_rotativo * Produto_modalidade.get_ccf_l1y_hy_kreg(self.produto_modalidade)
                else:
                    self.ead_kreg += self.saldo_nao_sacado_rotativo * Produto_modalidade.get_ccf_m1y_hy_kreg(self.produto_modalidade)
            else:
                if(self.prazo_total < 360):
                    self.ead_kreg += self.saldo_nao_sacado_rotativo * Produto_modalidade.get_ccf_l1y_ig_kreg(self.produto_modalidade)
                else:
                    self.ead_kreg += self.saldo_nao_sacado_rotativo * Produto_modalidade.get_ccf_m1y_ig_kreg(self.produto_modalidade)
            self.ead_kreg = self.ead_kreg * (1 - Produto_modalidade.get_mitigacaokreg(self.produto_modalidade))
                    
        if(Tela.is_garantia_prestada(self.tela)):
            if(self.is_genesis_vertex()):
                self.ead_cea = self.saldo_ativo_fechamento
                self.ead_kreg = self.saldo_ativo_fechamento * Produto_modalidade.get_ccf_l1y_ig_kreg(self.produto_modalidade)
            else:
                self.ead_cea = self.saldo_ativo_fechamento + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)
                self.ead_kreg = (self.saldo_ativo_fechamento + Vertice_operacao.get_juros_acumulados_ativo(self.vertice_anterior)) * Produto_modalidade.get_ccf_l1y_ig_kreg(self.produto_modalidade)
            
            if(Tela.get_onshore(self.tela)):
                if(ishighyield_ccf(self.rating_operacao)):
                    self.ead_cea += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_hy_cea(self.produto_modalidade)
                    self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_hy_cea(self.produto_modalidade)
                else:
                    self.ead_cea += float(float(self.obter_risco_termo(359)) * float(Produto_modalidade.get_ccf_l1y_ig_cea(self.produto_modalidade)))
                    self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_ig_cea(self.produto_modalidade)
            else:
                if(ishighyield_ccf(self.rating_operacao)):
                    self.ead_cea += self.obter_risco_termo(359) * Produto_modalidade.get_ccf_l1y_hy_cea_off(self.produto_modalidade)
                    self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_hy_cea_off(self.produto_modalidade)
                else:
                    self.ead_cea += float(float(self.obter_risco_termo(359)) * float(Produto_modalidade.get_ccf_l1y_ig_cea_off(self.produto_modalidade)))
                    self.ead_cea += (self.obter_risco_termo(self.prazo_total) - self.obter_risco_termo(359)) * Produto_modalidade.get_ccf_m1y_ig_cea_off(self.produto_modalidade)
            
            if(ishighyield_ccf(self.rating_operacao)):
                self.ead_kreg += self.obter_risco_termo(360) * Produto_modalidade.get_ccf_l1y_hy_kreg(self.produto_modalidade)
            
            else:
                self.ead_kreg += self.obter_risco_termo(360) * Produto_modalidade.get_ccf_l1y_ig_kreg(self.produto_modalidade)
            
            if(self.prazo_total > 366 and (Tela.get_produto(self.tela) == "LC" or Tela.get_produto(self.tela) == "LCIM")):
                self.ead_cea = self.ead_cea
            
            else:
                self.ead_cea = self.ead_cea * (1 - Produto_modalidade.get_mitigacaoead_cea(self.produto_modalidade))
            
            self.ead_kreg = self.ead_kreg * (1 - Produto_modalidade.get_mitigacaokreg(self.produto_modalidade))
       
        if(Tela.get_is_loan(self.tela)):
            cva = 1 + (calcular_cva(self.prazo_decorrer, self.fpr, self.connDB) / 100)    
            fepf = get_fepf(self.prazo_decorrer, self.tela.loan_classe_der, self.connDB) / 100

            self.ead_cea = self.ead_cea * (1 + self.tela.loan_epe)
            self.ead_kreg = self.ead_kreg + self.ead_kreg * cva * fepf
        
        if(Tela.get_is_rcp_cred(self.tela)):
            if(Tela.get_is_rcp_cred_cte(self.tela)):
                self.ead_cea = self.ead_cea * Tela.get_rcp_cred(self.tela)
            #else:
            #    self.ead_cea = self.ead_cea * encontra_eepe_cred(self.prazo_decorrer)

    def get_pe(self):
        return self.pe

    def get_receita_comprometida_rotativos(self):
        return self.receita_comprometida_rotativos

    def get_receita_termo(self):
        return self.receita_termo        
    
    def get_receita_comissao(self):
        return self.receita_comissao
    
    def get_mfb(self):
        return  self.mfb

    def get_kreg(self):
        return self.kreg

    def get_pagamento_passivo(self):
        return self.pagamento_passivo
    
    def get_pagamento_ativo(self):
        return self.pagamento_ativo

    def get_cea(self):
        return self.cea

    def get_prazo_decorrer(self):
        return self.prazo_decorrer

    def is_genesis_vertex(self):
        return self.genesis
    
    def is_termination_vertex(self):
        return self.termination_vertex

    def get_valor_amortizacao(self):
        return float(self.valor_amortizacao)

    def get_valor_desembolso_acumulado(self):
        return float(self.valor_desembolso_acumulado)

    def get_valor_amortizacao_acumulado(self):
        return float(self.valor_amortizacao_acumulado)
    
    def get_juros_acumulados_ativo(self):
        return self.juros_acumulados_ativo
    
    def get_juros_acumulados_passivo(self):
        return self.juros_acumulados_passivo
    
    def get_pagamento_juros_ativo(self):
        return self.pagamento_juros_ativo
    
    def get_pagamento_juros_passivos(self):
        return self.pagamento_juros_passivo

    def get_saldo_risco_termo(self):
        return self.saldo_risco_termo

    def get_saldo_nao_sacado_rotativo(self):
        return self.saldo_nao_sacado_rotativo

    def get_receita_termo_acumulada(self):
        return self.receita_termo_acumulada
    
    def get_receita_rotativos_acumulada(self):
        return self.receita_rotativos_acumulada
    
    def get_saldo_ativo_fechamento(self):
        return self.saldo_ativo_fechamento
    
    def get_saldo_passivo_fechamento(self):
        return self.saldo_passivo_fechamento
    
    def get_prazo_decorrido(self):
        return self.prazo_decorrido
    
    def get_valor_desembolso(self):
        return self.valor_desembolso

    def set_vertice_posterior(self, vertice_posterior):
        self.vertice_posterior = vertice_posterior
        self.dias_ate_proximo_vertice = max(Vertice_operacao.get_prazo_decorrido(self.vertice_posterior) - self.prazo_decorrido, 0)

    def set_vertice_anterior(self, vertice_anterior):
        self.vertice_anterior = vertice_anterior
    
    def get_dias_ate_proximo_vertice(self):
        return self.dias_ate_proximo_vertice

    def get_vertice_posterior(self):
        return self.vertice_posterior
    
    def get_ead_cea(self):
        return self.ead_cea

    def get_ead_kreg(self):
        return self.ead_kreg

    def get_ead_cluster(self):
        return self.cea_cluster

    def calcula_capital(self, np_estoque):             
        if(self.produto == "SEGURO"):
            self.kreg = (0.3/100) * Tela.get_notional(self.tela)
        else:
            ead_kreg_brl = Moeda_conversao.get_spot(self.moeda_conversao) * Toolkit_finance.forward_curency_dc(self.toolkit_finance, "BRL", self.indexador_ativo,  self.prazo_decorrido) * self.ead_kreg    
            self.kreg = calcula_kreg_normal(ead_kreg_brl, self.fpr, get_fatorn1(), get_fatorbis()) * (1 - self.mitigacao_tela_kreg)
        
        if(self.is_prospectivo and self.has_migracao):
            marginal_migracao_sem_pe = Estoque_df.cea_estoque_marginal_migracao_sem_pe(self.estoque, np_estoque, self.prazo_decorrido)
            marginal_migracao = Estoque_df.cea_estoque_marginal_migracao(self.estoque, np_estoque, self.prazo_decorrido)
            
            if(Tela.get_produto(self.tela) == "SEGURO"):
                #self.cea = ((4.5 / 12) * marginal_migracao_sem_pe) + (self.kreg)
                self.cea = (marginal_migracao_sem_pe) #desconsiderar regra de 1/3 de economico e 2/3 de regulatorio para 100% economico
            else:
                self.cea = max(((4.5 / 12) * marginal_migracao_sem_pe) + ((7.5 / 12) * self.kreg), min(marginal_migracao, self.ead_cea))

            if(Tela.get_id_grupo(self.tela) == "1172"):
                self.cea = max(((4.5 / 12) * self.ead_cea * 0.00503) + ((7.5 /12) * self.kreg), self.ead_cea * 0.0019)

        else:
            self.cea = Estoque_df.cea_estoque_marginal_migracao(self.estoque, np_estoque, self.prazo_decorrido)

            if(Tela.get_id_grupo(self.tela) == "1172"):
                self.cea = self.ead_cea * 0.0019
                
            self.cea_sem_pe = Estoque_df.cea_estoque_marginal_migracao_sem_pe(self.estoque, np_estoque, self.prazo_decorrido)
            self.cea_raroe = ((4.5 / 12) * self.cea_sem_pe) + ((7.5 / 12) * self.kreg)
            self.cea = max(self.cea, self.cea_raroe)

    def calcula_capital_cluster(self, np_estoque):
        if(self.is_prospectivo and self.has_migracao):
            marginal_migracao_cluster = Estoque_df.cea_estoque_marginal_migracao_cluster(self.estoque, np_estoque, self.prazo_decorrido)
            self.cea_cluster = marginal_migracao_cluster

            if(self.prazo_decorrido == 0):
                self.mfb_estoque = Estoque_df.get_mfb_estoque(self.estoque, self.prazo_decorrido, 0, self.prazo_decorrido)
            else:
                self.mfb_estoque = Estoque_df.get_mfb_estoque(self.estoque, self.prazo_decorrido, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido)
            
            self.kreg_estoque = Estoque_df.get_kreg_cluster(self.estoque, np_estoque, self.prazo_decorrido)
            self.pe_cluster = Estoque_df.get_pe_cluster(self.estoque, np_estoque, self.prazo_decorrido)
            self.ead_cluster = Estoque_df.get_ead_somado_cluster(self.estoque, self.prazo_decorrido)

            if(self.get_prazo_decorrido() > 0):
                self.mfb_x_sell = self.ead_cluster * (self.get_prazo_decorrido() - Vertice_operacao.get_prazo_decorrido(self.vertice_anterior) / 360 * get_spread_x_sell())
            else:
                self.mfb_x_sell = 0

            self.lgd_estoque = Estoque_df.get_lgd_ponderada_cluster(self.estoque, self.prazo_decorrido)
            self.prz_estoque = Estoque_df.get_prazo_poderada_cluster(self.estoque, self.prazo_decorrido)
            self.pdd_estoque = self.calcula_pe_prospectiva_variavel(self.ead_cluster, self.lgd_estoque)
            
        else:
            self.cea = Estoque_df.cea_marginal(self.estoque, np_estoque, self.prazo_decorrido)
            self.cea_sem_pe = Estoque_df.cea_marginal_sem_pe(self.estoque, np_estoque,self.prazo_decorrido)

            self.cea_raroe = ((4.5 / 12) * self.cea_sem_pe) + ((7.5 / 12) * self.kreg)

            self.cea = max(self.cea, self.cea_raroe)

    def get_mfb_x_sell(self):
        return self.mfb_x_sell

    def get_fpr_vertice(self):
        return [self.prazo_decorrido, self.fpr]

    def calcula_capital_cluster_corp(self):
        self.cea_cluster = Estoque_df.get_cea_cluster_corp(self.estoque, self.prazo_decorrido)
        self.mfb_estoque = Estoque_df.get_mfb_estoque(self.estoque,self.prazo_decorrido, Vertice_operacao.get_prazo_decorrido(self.vertice_anterior), self.prazo_decorrido)
        self.pe_cluster = Estoque_df.get_pe_estoque_migracao_cluster(self.prazo_decorrido, False, Tela.get_rating_grupo(self.tela))
        self.ead_cluster = Estoque_df.get_ead_somado_cluster(self.estoque, self.prazo_decorrido)
        self.lgd_estoque = Estoque_df.get_lgd_ponderada_cluster(self.estoque, self.prazo_decorrido)
        self.prz_estoque = Estoque_df.get_prazo_ponderada(self.estoque, self.prazo_decorrido)
    
    def calcula_pe(self, np_monstro):
        if(self.is_prospectivo and self.has_migracao):
            self.pe = self.calcula_pe_prospectiva(np_monstro)
             
        else:
            self.pe = self.calcula_pe_flat()

    def calcula_pe_cluster(self, np_monstro):
        if(self.is_prospectivo and self.has_migracao):
            self.pe = self.calcula_pe_prospectiva_cluster(np_monstro)
        else:
            self.pe = self.calcula_pe_flat()

    def calcula_pe_flat(self):
        if(self.is_genesis_vertex()):
            return 0
        else:
            pd = Regua_pd.get_pd_modelo(self.regua_pd, self.id_segmento_grupo, self.rating_operacao)
            return (pow(1-pd, (Vertice_operacao.get_prazo_decorrido(self.vertice_anterior) / 360)) - pow(1-pd, (self.prazo_decorrido / 360))) * self.lgd_mitigada * Vertice_operacao.get_ead_cea(self.vertice_anterior)
    
    def calcula_pe_prospectiva(self,np_monstro):
            array_pd_modelo = np_monstro[:,1]
            array_pd_modelo = array_pd_modelo.astype(float)

            array_lgd = np_monstro[:,7]
            array_lgd = array_lgd.astype(float)
                
            array_lgd_dt = np_monstro[:,3]
            array_lgd_dt = array_lgd_dt.astype(float) 
                        
            array_lgd_be = np_monstro[:,4]
            array_lgd_be = array_lgd_be.astype(float) 
   
            array_default = np_monstro[:,5]
                
            array_probabilidade_migracao = np_monstro[:,6]
            array_probabilidade_migracao = array_probabilidade_migracao.astype(float)
            
            array_prazo_simulado = np_monstro[:,12]
            array_prazo_simulado = array_prazo_simulado.astype(float)

            array_rating = np_monstro[:,0]
            array_rating_original = np_monstro[:,16]
            array_migrar = np_monstro[:,18]

            array_id_grupo = np_monstro[:,19]

            if(self.is_genesis_vertex()):
                return 0
            else:
                pd_atual = 0
                lgd_migracao = 0
                
                filtro = np.where((array_prazo_simulado == self.prazo_decorrido))
                filtro_id_grupo = np.where((array_prazo_simulado == self.prazo_decorrido) & (array_migrar == True) & (array_id_grupo == Tela.get_id_grupo(self.tela)))
                
                filtro_operacoes = np.where((array_prazo_simulado == self.prazo_decorrido) & (array_migrar == False))
                rating_dif_grupo = array_rating_original[filtro_operacoes]

                num_dif_id = len(array_rating[filtro_operacoes])/25  #dividir pelo numero de ratings
                num_mesmo_id = len(array_rating[filtro_id_grupo])/25 #dividir pelo numero de ratings

                array_pd_filtrado_mesmo_id_grupo = array_pd_modelo[filtro_id_grupo]
                array_prob_migracao_mesmo_id_grupo = array_probabilidade_migracao[filtro_id_grupo]

                array_pd_dif_id = array_pd_modelo[filtro_operacoes]
                array_dif_prob_migracao = array_probabilidade_migracao[filtro_operacoes]

                array_prob_filtrado = array_probabilidade_migracao[filtro]

                array_lgd_be_filtrado = array_lgd_be[filtro_id_grupo]
                array_default_filtrado = array_default[filtro_id_grupo]
                
                #calculo pd
                pd_mesmo_id = array_pd_filtrado_mesmo_id_grupo * array_prob_migracao_mesmo_id_grupo
                pd_atual = pd_mesmo_id.sum()/num_mesmo_id

                #calculo_lgd
                lgd_migracao_default = array_lgd_be_filtrado * array_prob_migracao_mesmo_id_grupo * (1- self.mitigacao_cea)
                lgd_migracao_n_default = self.lgd_mitigada * array_prob_migracao_mesmo_id_grupo
                lgd_migracao = np.where(array_default_filtrado == "TRUE", lgd_migracao_default, lgd_migracao_n_default)
                lgd_migracao_final = lgd_migracao.sum()/num_mesmo_id
               
            return (pow(1-pd_atual, (Vertice_operacao.get_prazo_decorrido(self.vertice_anterior)/360)) - pow(1-pd_atual, (self.prazo_decorrido / 360))) * lgd_migracao_final * Vertice_operacao.get_ead_cea(self.vertice_anterior)
    
    def calcula_pe_prospectiva_cluster(self,np_monstro):
            array_incluir_no_cluster = np_monstro[:,21]
            array_incluir_no_cluster = array_incluir_no_cluster.astype(bool)

            array_pd_modelo = np_monstro[:,1]
            array_pd_modelo = np.where(array_incluir_no_cluster == True, array_pd_modelo, 0)
            array_pd_modelo = array_pd_modelo.astype(float)

            array_lgd = np_monstro[:,7]
            array_lgd = np.where(array_incluir_no_cluster == True, array_lgd, 0)
            array_lgd = array_lgd.astype(float)
                
            array_lgd_dt = np_monstro[:,3]
            array_lgd_dt = np.where(array_incluir_no_cluster == True, array_lgd_dt, 0)
            array_lgd_dt = array_lgd_dt.astype(float) 
                        
            array_lgd_be = np_monstro[:,4]
            array_lgd_be = np.where(array_incluir_no_cluster == True, array_lgd_be, 0)
            array_lgd_be = array_lgd_be.astype(float) 
   
            array_default = np_monstro[:,5]
                
            array_probabilidade_migracao = np_monstro[:,6]
            array_probabilidade_migracao = array_probabilidade_migracao.astype(float)
            
            array_prazo_simulado = np_monstro[:,12]
            array_prazo_simulado = array_prazo_simulado.astype(float)

            array_rating = np_monstro[:,0]
            array_rating_original = np_monstro[:,16]
            array_migrar = np_monstro[:,18]

            array_id_grupo = np_monstro[:,19]

            if(self.is_genesis_vertex()):
                return 0
            else:
                pd_atual = 0
                lgd_migracao = 0
                
                filtro = np.where((array_prazo_simulado == self.prazo_decorrido))
                filtro_id_grupo = np.where((array_prazo_simulado == self.prazo_decorrido) & (array_migrar == True) & (array_id_grupo == Tela.get_id_grupo(self.tela)))
                
                filtro_operacoes = np.where((array_prazo_simulado == self.prazo_decorrido) & (array_migrar == False))
                rating_dif_grupo = array_rating_original[filtro_operacoes]

                num_dif_id = len(array_rating[filtro_operacoes])/25  #dividir pelo numero de ratings
                num_mesmo_id = len(array_rating[filtro_id_grupo])/25 #dividir pelo numero de ratings

                array_pd_filtrado_mesmo_id_grupo = array_pd_modelo[filtro_id_grupo]
                array_prob_migracao_mesmo_id_grupo = array_probabilidade_migracao[filtro_id_grupo]

                array_pd_dif_id = array_pd_modelo[filtro_operacoes]
                array_dif_prob_migracao = array_probabilidade_migracao[filtro_operacoes]

                array_prob_filtrado = array_probabilidade_migracao[filtro]

                array_lgd_be_filtrado = array_lgd_be[filtro_id_grupo]
                array_default_filtrado = array_default[filtro_id_grupo]
                
                #calculo pd
                pd_mesmo_id = array_pd_filtrado_mesmo_id_grupo * array_prob_migracao_mesmo_id_grupo
                pd_atual = pd_mesmo_id.sum()/num_mesmo_id

                #calculo_lgd
                lgd_migracao_default = array_lgd_be_filtrado * array_prob_migracao_mesmo_id_grupo * (1- self.mitigacao_cea)
                lgd_migracao_n_default = self.lgd_mitigada * array_prob_migracao_mesmo_id_grupo
                lgd_migracao = np.where(array_default_filtrado == "TRUE", lgd_migracao_default, lgd_migracao_n_default)
                lgd_migracao_final = lgd_migracao.sum()/num_mesmo_id
               
            return (pow(1-pd_atual, (Vertice_operacao.get_prazo_decorrido(self.vertice_anterior)/360)) - pow(1-pd_atual, (self.prazo_decorrido / 360))) * lgd_migracao_final * Vertice_operacao.get_ead_cea(self.vertice_anterior)
    
    def calcula_pe_prospectiva_variavel(self, ead, lgd):
        if(self.is_genesis_vertex()):
            return 0
        else:
            pd_atual = 0
            lgd_migracao = 0

            vetor_rating = get_vetor_rating()

            for i in range(len(vetor_rating)):
                if(Matriz_migracao.get_migracao(self.matriz_migracao, vetor_rating[i], self.prazo_decorrido) != 0):
                    pd_atual += Regua_pd.get_pd_modelo(self.regua_pd, self.id_segmento_grupo, vetor_rating[i]) * Matriz_migracao.get_migracao(self.matriz_migracao, vetor_rating[i], self.prazo_decorrido) 

                    if(Regua_pd.is_rating_default(self.regua_pd, self.id_segmento_grupo, vetor_rating[i]) == "TRUE"):
                        lgd_migracao += Regua_pd.get_lgd_be(self.regua_pd, self.id_segmento_grupo, vetor_rating[i]) * Matriz_migracao.get_migracao(self.matriz_migracao, vetor_rating[i], self.prazo_decorrido) * (1- self.mitigacao_cea)
                    else:
                        lgd_migracao += lgd * Matriz_migracao.get_migracao(self.matriz_migracao, vetor_rating[i], self.prazo_decorrido)
            
            if(ead > 0):
                return (pow(1-pd_atual, (Vertice_operacao.get_prazo_decorrido(self.vertice_anterior)/360)) - pow(1-pd_atual, (self.prazo_decorrido / 360))) * lgd_migracao * ead
            else:
                return 0
                  
    def obter_numerador_duration(self, dias):
        if(self.is_termination_vertex()):
            return 0
        prazo = self.get_vertice_posterior().get_prazo_decorrido() - dias
        teste = self.get_vertice_posterior().get_valor_amortizacao()
        return self.get_vertice_posterior().get_valor_amortizacao() * prazo + self.get_vertice_posterior().obter_numerador_duration(dias)
    
    def obter_denominador_duration(self):
        if(self.is_termination_vertex()):
            return 1    # nao pode retornar zero no denominador

        return self.get_vertice_posterior().get_valor_amortizacao() + self.get_vertice_posterior().obter_denominador_duration() 

    def obter_duration(self):
        numerador = self.obter_numerador_duration(self.prazo_decorrido)
        denominador = self.obter_denominador_duration()
        return (self.obter_numerador_duration(self.prazo_decorrido) / self.obter_denominador_duration())
    
    def obter_risco_termo(self, dias):
        if(self.is_termination_vertex() or self.get_vertice_posterior().is_termination_vertex()):
            return 0

        if(dias<= 0):
            return 0
        
        delta_tempo = self.prazo_decorrer - self.get_vertice_posterior().get_prazo_decorrer()
        
        if(delta_tempo>=dias):
            return 0
        
        res = self.get_vertice_posterior().get_valor_desembolso() + self.get_vertice_posterior().obter_risco_termo(dias - delta_tempo)
        if(res != 0):
            res = res

        return res

    def get_lgd_mitigada(self):
        return self.lgd_mitigada_2    