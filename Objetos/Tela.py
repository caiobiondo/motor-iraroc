#Importacoes de bibliotecas
import sqlite3
import numpy as np
from datetime import *
import datetime
#Importacoes de classes 
from ToolKitDB.Parametros import *
from ToolKitDB.Veiculo_legal import Veiculo_legal
from ToolKitDB.FPR import *
from Objetos.Fluxo import Fluxo

dicionario = {}
class Tela:             #Classe que captura os dados mostrados na tela
    
    def __init__(self, dicionario, conn, conn_fpr):

        if 'ccf_manual' in dicionario:
            if(dicionario['ccf_manual'] == 'true' or dicionario['ccf_manual'] == 'True'):    #Valor opcional
                self.ccf_manual = True
            else:
                self.ccf_manual = False

            self.ccf_cea = float(dicionario["ccf_cea"].replace(",","."))/100
            self.ccf_kreg = float(dicionario["ccf_kreg"].replace(",", "."))/100
        else:
            self.ccf_manual = False
            self.ccf_cea = 0
            self.ccf_kreg = 0

        self.fluxo = dicionario["fluxo"]
        self.is_mitigacao_fluxo = False
        if(len(dicionario['cnpj_cabeca']) == 9):
            self.cnpj_cabeca = dicionario['cnpj_cabeca'] # obrigatorio
        else:
            self.cnpj_cabeca = dicionario['cnpj_cabeca']
            while (len(self.cnpj_cabeca) < 9):
                self.cnpj_cabeca = "0" + self.cnpj_cabeca        

        if "fpr_pos" in dicionario:
            self.FPR_pos = dicionario["fpr_pos"]

        if "fpr_pre" in dicionario:
            self.FPR = dicionario["fpr_pre"]
        
        else:     
            self.FPR = dicionario['fpr']
            self.FPR_pos = self.FPR 
            
        if 'fpr_variavel' in dicionario:
            if(dicionario['fpr_variavel'] == 'True' or dicionario['fpr_variavel'] == 'true'):
                self.fpr_variavel = True      #Valor opcional
            else:
                self.fpr_variavel = False
        else:
            self.fpr_variavel = False
        
        if 'spread_variavel' in dicionario:
            if(dicionario['spread_variavel'] == 'True' or dicionario['spread_variavel'] == 'true'):
                self.spread_variavel = True  #Valor opcional
            else:
                self.spread_variavel = False
        else:
            self.spread_variavel = False
        ##############LOGICA PARA ACERTAR VALOR DE FPR##############
        data_limite = date(2023, 1, 1)
        for i in range(len(self.fluxo)):
            dia = int(self.fluxo[i]["data"][0:2])
            mes = int(self.fluxo[i]["data"][3:5])
            ano = int(self.fluxo[i]["data"][6:10])
            data_atual = date(ano, mes, dia)
            dif = data_atual - data_limite

            if(not(self.fpr_variavel)):
                if(dif.days <= 0):
                    self.fluxo[i]['fpr'] = self.FPR
                    
                elif(dif.days >= 0):
                    self.fluxo[i]['fpr'] = self.FPR_pos
            else:
                self.fluxo[i]['fpr'] = self.fluxo[i]['fpr']
        ##############LOGICA PARA ACERTAR VALOR DE FPR##############
        
        self.prospectivo = dicionario['prospectivo']    # Valor obrigatório
        if(self.prospectivo == "true" or bool(self.prospectivo)):
            self.prospectivo = True
        else:
            self.prospectivo = False

        self.volatilidade = dicionario['id_volatilidade']  # Valor obrigatório
        
        if "cd_quebra" in dicionario:
            self.quebra = dicionario['cd_quebra']   
        else:
            self.quebra = 0           

        #Logica para armazenar valor em id_cenario
        if "id_cenario" in dicionario:
            self.id_cenario = dicionario['id_cenario']
        else:
            self.id_cenario = 1

        #Valores retirados da Tela
        self.rating_operacao = dicionario['ratingOper'].upper()  #Valor obrigatório - String
        self.prazo_total = int(dicionario['prazo_total'])        #Valor obrigatório - String

        self.indexador = dicionario['indexador']              #Valor obrigatório

        if "indexadorGarantia" in dicionario:
            self.indexador_garantia = dicionario['indexadorGarantia'] #Valor obrigatorio apenas para fianca
        else:
            self.indexador_garantia = ""

        self.veiculo_legal = dicionario['veiculo_legal']
        
        #Valores vindo de Derivativos
        #self.indexador_derivativo_ativo = dicionario['indexador_derivativo_ativo'] # opcional
        #self.indexador_derivativo_passivo = dicionario['indexador_derivativo_passivo'] # opcional

        self.formato_taxa = dicionario['formato_taxa']
        spread_aux = str(dicionario['spread']).replace(',', '.')
        self.spread = float(spread_aux)/100                      #Valor obrigatório
        
        if 'TxCliente' in dicionario:
            self.taxa_cliente = float(dicionario['TxCliente'])/100    #Valor opcional
        else:
            self.taxa_cliente = 0
        
        if 'TxRef' in dicionario:
            self.taxa_ref = dicionario['TxRef']/100            #Valor opcional
        else:
            self.taxa_ref = 0

        if 'TxBOF' in dicionario:
            self.taxa_bof = float(dicionario['TxBOF']) / 100            #Valor opcional
        else:
            self.taxa_bof = 0
        
        if 'formatoBOF' in dicionario:
            self.formato_bof = dicionario['formatoBOF']            #Valor opcional\
        else:
            self.formato_bof = 0
        
        if 'TxVenda' in dicionario:
            self.taxa_venda = float(dicionario['TxVenda'])/ 100        #Valor opcional
        else:
            self.taxa_venda = 0
        
        if 'formatoVenda' in dicionario:
            self.formato_venda = dicionario['formatoVenda']        #Valor opcional
        else:
            self.formato_venda = 0 
        
        self.commitment_spread = str(dicionario['CommitmentSpread']).replace(",", ".")  #Valor obrigatório
        self.valor_limite_linha = dicionario['ValorLimiteLinha']  #Valor obrigatório para rotativos

        self.notional = dicionario['notional'] #Valor obrigatório
                
        self.mitigacao_cea = str(dicionario['mitigacaoCEA']).replace(",",".") #Valor obrigatório
        self.mitigacao_cea_2 = str(dicionario['mitigacaoCEA2']).replace(",",".") # obrigatório
        
        if dicionario['MitigacaoKREG'] == "":
            self.mitigacao_kreg = "0"
        else:
            self.mitigacao_kreg = str(dicionario['MitigacaoKREG']).replace(",", ".") # obrigatório

        self.classe = dicionario['classe_produto'] # obrigatorio

        if 'classe_derivativo' in dicionario:
            self.classe_derivativo = dicionario['classe_derivativo'] # opcional
        else:
            self.classe_derivativo = '' 
        
        self.produto = dicionario['produto'] # obrigatorio
        self.modalidade = dicionario['modalidade'] # obrigatorio

        self.id_grupo = dicionario['id_grupo'] # obrigatorio
        self.rating_grupo = dicionario['ratingGrupo'].upper() # obrigatório
        self.id_segmento_risco_cabeca = int(dicionario['id_segmento_risco_cabeca']) # obrigatorio
        self.id_segmento_risco_grupo = int(dicionario['id_segmento_risco_grupo']) # obrigatorio

        if 'perc_bof' in dicionario:
            bof = str(dicionario['perc_bof'])
            self.perc_bof = float(bof.replace(',', '.')) / 100 # opcional 
        else:
            self.perc_bof = 0
        
        
        if (dicionario['pzoMedio'] == "True" or dicionario['pzoMedio'] == "true"):
            self.tratamento_prazo_medio = True
        else:
            self.tratamento_prazo_medio = False

        self.fx_spot = dicionario['FXSpot']
        self.receita_fee = dicionario['FEE'].replace(',', '.')
        self.raroc_hurdle = float(str(dicionario['raroc_ref']).replace(',', '.')) /100 

        if (dicionario['prazo_indeterminado'] == 'True' or dicionario['prazo_indeterminado'] == 'true'):
            self.prazo_indeterminado = True
        else:
            self.prazo_indeterminado = False

        if 'prazo_quebrado' in dicionario:
            self.prazo_quebrado = dicionario['prazo_quebrado']
            if(self.prazo_quebrado == "True" or self.prazo_quebrado == "true"):
                self.prazo_quebrado = True
            elif(self.prazo_quebrado == "False" or self.prazo_quebrado == "false"):
                self.prazo_quebrado = False
        else:
            self.prazo_quebrado = False

        if "prazo_repac" in dicionario:
            self.prazo_repac_hub = dicionario['prazo_repac']
            self.prazo_repac_hub = int(self.prazo_repac_hub)
        else:
            self.prazo_repac_hub = 0 
        
        #ADICIONAR TRATAMENTO PARA SEGURO GARANTIA AQUI 
        if (flag_fianca_repac(self.rating_operacao, self.volatilidade) == True) and (self.produto == "FIANCA" or self.produto == "Fianca") and (self.prazo_total > 365) and (not(self.prazo_quebrado)) and (self.prazo_repac_hub != -1) :
            if(not(self.is_middle())):
                self.prazo_quebrado = True  # opcional
                self.prazo_inicial = 0      # opcional
            
            if(self.is_middle()):
                self.prazo_final = self.prazo_total

            elif 'prazo_final' in dicionario:
                if float(dicionario['prazo_final']) > 0:
                    self.prazo_final = dicionario['prazo_final']   # opcional
                else:
                    self.prazo_final = 365
                    
            elif self.prazo_repac_hub > 0:
                self.prazo_final = self.prazo_repac_hub
            else:
                self.prazo_final = 365

            if(self.is_middle()):
                self.prazo_repac = False
            else:
                self.prazo_repac = True

        else:
            if 'prazo_quebrado' in dicionario:
                self.prazo_quebrado = dicionario['prazo_quebrado'].lower() == "true" # opcional

            if 'prazo_inicial' in dicionario:
                self.prazo_inicial = dicionario['prazo_inicial'] # opcional
            else:
                self.prazo_inicial = 0
            
            if 'prazo_final' in dicionario:
                self.prazo_final = dicionario['prazo_final'] # opcional
            else:
                self.prazo_final = 0

            self.prazo_repac = False

        veiculo_legal = Veiculo_legal(conn)
        
        self.LGD = dicionario['lgd'].replace(",", ".")
        self.IRCS = veiculo_legal.get_aliquota_ir_cs(dicionario['veiculo_legal'])
        self.PIS_COFINS = get_piscofins()
        self.IE = dicionario['IE'].replace(",", ".")      # //FROM PRODUTO

        if "custo_manual" in dicionario:
            self.custo_manual = dicionario['custo_manual']
            if self.custo_manual is None or self.custo_manual == '':
                self.custo_manual = 0
            else:
                self.custo_manual = dicionario['custo_manual'].replace(",", ".")
        else:
            self.custo_manual = 0
    
        if (dicionario['onShore'] == "true" or dicionario['onShore'] == 'True'):
            self.onshore_offshore = True
        else:
            self.onshore_offshore = False

        if (dicionario['iss'] == 'True' or dicionario['iss'] == 'true'):
            self.ISS = get_iss() / (1 - get_iss())
        else:
            self.ISS = 0

        if dicionario['epe_constante']:
            self.is_rcp_cte = bool(dicionario['epe_constante'])
        else:
            self.is_rcp_cte = False

        if "epe_manual" in dicionario:
            self.rcp = float(dicionario['epe_manual']) /100
        else:
            self.rcp = 0

        # Combo loan
        if "epe" in dicionario['loan']['combinado']:                #LOGICA PARA DAR VALOR AO COMBINADO EPE
            self.loan_epe = float(dicionario['loan']['combinado']['epe']) /100
        else:
            self.loan_epe = -1

        if 'classe' in dicionario['loan']['der']:
            self.loan_classe_der = dicionario['loan']['der']['classe']
        else:
            self.loan_classe_der = 0

        if self.loan_epe >= 0:

            self.is_loan = True
        else:

            self.is_loan = False

        self.rcp_cred = 1

        if "cte" in dicionario['rcp_credito']:                      #LOGICA PARA DAR VALOR AO CRED CONSTANTE
            self.is_recp_cred_cte = dicionario['rcp_credito']['cte']
        else:
            self.is_recp_cred_cte = 0

        if "epe" in dicionario['rcp_credito'] and dicionario['rcp_credito']['epe'] > 0:
            self.rcp_cred = dicionario['rcp_credito']['epe']
        else:
            self.rcp_cred = {}

    # METODOS DA CLASSE   
    def get_fpr_pre(self):
        return (float(self.FPR)/100)
    
    def get_fpr_pos(self):
        return (float(self.FPR_pos)/100)

    def get_tratamento_prazo_medio(self):
        return self.tratamento_prazo_medio

    def get_ircs(self):
        return self.IRCS

    def get_is_loan(self):
        return self.is_loan

    def get_delta_prazo(self):
        return self.accrual_ativo

    def get_quebra(self):
        return self.quebra

    def get_delta_lgd(self):
        return self.delta_lgd
    
    def get_ccf_manual(self):
        return self.ccf_manual
    
    def get_prazo_inicial(self):
        return float(self.prazo_inicial)

    def get_prazo_final(self):
        return float(self.prazo_final)

    def get_perc_bof(self):
        return self.perc_bof

    def get_taxa_cliente(self):
        return self.taxa_cliente
    
    def get_taxa_cliente(self):
        return self.taxa_cliente

    def set_taxa_cliente(self,taxa):
        self.taxa_cliente = taxa/100

    def get_taxa_ref(self):
        return self.taxa_ref

    def set_taxa_ref(self, taxa):
        self.taxa_ref = taxa/100

    def get_taxa_bof(self):
        return self.taxa_bof

    def set_taxa_bof(self,taxa):
        self.taxa_bof = taxa/100

    def get_taxa_venda(self):
        return self.taxa_venda
    
    def set_taxa_venda(self,taxa):
        self.taxa_venda = taxa/100

    def get_formato_bof(self):
        return self.formato_bof

    def get_formato_venda(self):
        return self.formato_venda

    def get_id_cenario(self):
        return self.id_cenario

    def get_is_rcp_cred(self):
        if  self.rcp_cred != {} and self.rcp_cred > 0:
            return True
        else:
            return False

    def get_is_rcp_cred_cte(self):
        return self.is_recp_cred_cte

    def get_rcp_cred(self):
        return self.rcp_cred

    def get_is_rcp_cte(self):
        return self.is_rcp_cte

    def get_rcp(self):
        return self.rcp

    def get_prospectivo(self):
        return self.prospectivo

    def get_volatilidade(self):
        return self.volatilidade

    def get_produto(self):
        return self.produto
    
    def get_modalidade(self):
        return self.modalidade

    def get_raroc_hurdle(self):
        return self.raroc_hurdle

    def get_classe_derivativo(self):
        return self.classe_derivativo

    def get_id_veiculo_legal(self):
        return self.veiculo_legal
    
    def set_spread(self,spread):
        self.spread = spread/100

    def get_fluxo(self):
        Arrayfluxo = []
        array_data = []
        array_mitigacao = []
        for i in range(len(self.fluxo)):
            juros = self.fluxo[i]['juros']
            array_data.append(self.fluxo[i]["data"])
            
            if(float(self.fluxo[i]["mitiga"]) != 0):
                self.is_mitigacao_fluxo = True

            if(self.spread_variavel):
                
                fluxo = Fluxo(self.fluxo[i]['data'], self.fluxo[i]['desembolso'], self.fluxo[i]['amortiza'], juros, float(self.fluxo[i]['mitiga'])/100, self.fluxo[i]['dc'], float(self.fluxo[i]['fpr'])/100, float(self.fluxo[i]['spread'].replace(",", ".")))
                Arrayfluxo.append(fluxo)
            
            else:

                fluxo = Fluxo(self.fluxo[i]['data'], self.fluxo[i]['desembolso'], self.fluxo[i]['amortiza'], juros, float(self.fluxo[i]['mitiga'])/100, self.fluxo[i]['dc'], float(self.fluxo[i]['fpr'])/100, 0)
                Arrayfluxo.append(fluxo)                    
        
        data_limite = date(2023, 1, 1)
        ultimo_dia_fluxo = int(self.fluxo[len(self.fluxo) - 1]["data"][0:2])
        ultimo_mes_fluxo = int(self.fluxo[len(self.fluxo) - 1]["data"][3:5])
        ultimo_ano_fluxo = int(self.fluxo[len(self.fluxo) - 1]["data"][6:10])
        ultima_data_fluxo = date(ultimo_ano_fluxo, ultimo_mes_fluxo, ultimo_dia_fluxo)
        dif_dias = data_limite - ultima_data_fluxo

        if (("01/01/2023" not in array_data) and (dif_dias.days < 0) and (not(self.fpr_variavel) and not(self.spread_variavel))): 
            dia = int(self.fluxo[0]["data"][0:2])
            mes = int(self.fluxo[0]["data"][3:5])
            ano = int(self.fluxo[0]["data"][6:10])

            data_inicio = date(ano, mes, dia)
            dc = data_limite - data_inicio

            fluxo = Fluxo("01/01/2023", 0, 0, False, 0, dc.days, self.FPR_pos, 0)
            Arrayfluxo.append(fluxo)
            Arrayfluxo.sort(key=lambda x: x.dc, reverse=False)

        return Arrayfluxo
        
    def get_is_mitigacao_fluxo(self):
        return self.is_mitigacao_fluxo

    def get_fpr(self):
        return float(self.FPR)/100

    def get_ie(self):
        return float(self.IE)/100
    
    def get_prazo_repac(self):
        return self.prazo_repac

    def get_custo_manual(self):
        return float(self.custo_manual)

    def get_lgd(self):
        return float(self.LGD)/100
    
    def get_prazo_indeterminado(self):
        return self.prazo_indeterminado
    
    def get_prazo_quebrado(self):
        return self.prazo_quebrado

    def get_aliquota_pis_cofins(self):
        return float(self.PIS_COFINS)

    def get_aliquota_iss(self):
        return float(self.ISS)

    def is_rotativo(self):
        return (self.classe == "ROT")

    def is_credito_ativo(self) :
        return (self.classe == "ATI")

    def is_derivativo(self):
        return (self.classe == "DER")

    def is_garantia_prestada(self):
        return (self.classe == "GP")
    
    def get_onshore(self):
        return self.onshore_offshore

    def get_prospectivo(self):
        return self.prospectivo

    def get_receita_fee(self):
        return float(self.receita_fee)

    def set_receita_fee(self,fee):
        self.receita_fee = fee

    def get_cnpj_cabeca(self):
        return self.cnpj_cabeca

    def get_id_grupo(self):
        return float(self.id_grupo)
    
    def get_rating_operacao(self):
        return self.rating_operacao
    
    def get_rating_grupo(self):
        return self.rating_grupo

    def get_id_segmento_risco_cabeca(self):
        return int(self.id_segmento_risco_cabeca)

    def get_id_segmento_risco_grupo(self):
        return int(self.id_segmento_risco_grupo)

    def get_prazo_total(self):
        return int(self.prazo_total)

    def get_indexador(self):
        return self.indexador

    def get_indexador_garantia(self):
        return self.indexador_garantia

    def get_formato_taxa(self):
        return self.formato_taxa

    def get_spread(self):
        self.spread = float(self.spread)
        return self.spread

    def get_commitment_spread(self):
        return (float(self.commitment_spread)/100)

    def set_commitment_spread(self,comit_fee):
        self.commitment_spread = comit_fee

    def get_valor_limite_linha(self):
        return self.valor_limite_linha

    #Regra Seguro Garantia
    def get_notional(self):
        return float(self.notional)

    def get_mitigacao_cea(self):
        return (float(self.mitigacao_cea)/100)  #PEGAR VALOR DA MITIGACAO EM PORCENTAGEM

    def get_mitigacao_cea_2(self):
        return (float(self.mitigacao_cea_2)/100)

    def get_mitigacao_kreg(self):
        return (float(self.mitigacao_kreg)/100) #PEGAR VALOR DA MITIGACAO EM PORCENTAGEM

    def get_index_ativo(self):
        return self.indexador_derivativo_ativo

    def get_index_passivo(self):
        return self.indexador_derivativo_passivo

    def is_fpr_variavel(self):
        is_variavel = bool(self.fpr_variavel)
        return is_variavel

    def is_spread_variavel(self):
        is_variavel = bool(self.spread_variavel)
        return is_variavel

    def is_seguro_garantia(self):
        return self.produto == "SEGURO"
    
    def to_bool(self,string):
        if string == "True":
            return True
        elif string == "False":
            return False
    
    def is_middle(self):
        id_cabeca = self.get_id_segmento_risco_cabeca()
        if(id_cabeca == 42):
            return True
        else:
            return False
        
    def get_lgd_mitigada(self):
        return ((self.get_lgd()) * (1 - (self.get_mitigacao_cea_2())))