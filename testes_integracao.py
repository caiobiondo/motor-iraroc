# Imports obrigatorios
import unittest
import sqlite3
from ToolKitDB.Operacao_df import Operacao_df
from ToolKitDB.Matriz_migracao import Matriz_migracao           #Import da classe matriz_migracao
from ToolKitDB.Moeda_conversao import Moeda_conversao           #Import da clasee moeda_conversao
from ToolKitDB.Regua_pd import Regua_pd                         #Import da clase regua_pd
from ToolKitDB.Visao_carteira import Visao_carteira             #Import da classe visao_carteira
from ToolKitDB.Cea_globais import Cea_globais                   #Import da classe cea_globais
from Objetos.Fluxo import Fluxo                                 #Import da classe ObjFuxo
from Objetos.Tela import Tela                         
from ToolKitFinance.ToolKit_finance import Toolkit_finance      #Import da classe toolkit_finance
from ToolKitDB.Parametros import *                              #Import dos parametros
from ToolKitDB.Curvas_juros import Curvas_juros
from ToolKitDB.CVA import *
from ToolKitDB.FEPF import *
from ToolKitDB.Recalibrar_cea import *
from Objetos.Estoque_df import Estoque_df
from ToolKitDB.Vetor_operacao_df import *

#renomear: rating_operacao, rating_grupo, commitment_spread, valor_limite_linha, ie, mitigacao_cea, 
#mitigacao_kreg, fx_spot, prazo_medio, receita_fee, mitigacao_cea_2, taxa_cliente, taxa_ref, taxa_bof, formato_bof, taxa_venda
json = {"id_grupo":"1399","cnpj_cabeca":"033000167","rating_operacao":"BAA4",
                    "rating_grupo":"BAA4","id_segmento_risco_cabeca":"27",
                    "id_segmento_risco_grupo":27,"notional":100000000,"indexador":"BRL",
                    "prazo_total":"1095","qtd_parcela":"2","dt_inicio":"14/04/2020",
                    "dt_final":"14/04/2023","produto":"KG","modalidade":"Normal",
                    "classe_produto":"ATI","perc_uti":"100","fluxo":[{"data":"14/04/2020","desembolso":"100000000","amortiza":"0","juros":False,"mitiga":"0","dc":0,"fpr":"","spread":""},
                    {"data":"14/04/2023","desembolso":"0","amortiza":"100000000","juros":True,"mitiga":"0","dc":1095,"fpr":"","spread":""}],
                    "formato_taxa":"EXP/360","spread":"2",
                    "commitment_spread":"0","valor_limite_linha":"0",
                    "ie":"12,2","qtde_titulos":"","pb_adicional":"0","iss":"False","fpr":"85","frp":"0","mitigacao_cea":"0","mitigacao_kreg":"0",
                    "fx_spot":"1","prazo_medio":"True","onshore":"True","receita_fee":"0","raroc_ref":"12,5","prazo_indeterminado":"False","lgd":"48",
                    "veiculo_legal":"1222","custo_manual":"","id_volatilidade":"2","prospectivo":True,"perc_bof":"0,001","rcp_credito":{},"loan":{"der":{"localizacao":"","classe":"TCO"},
                    "cre":{"passivo":"","localizacao":""},"combinado":{},"sfloat":{},"juros":{},"moeda_limite":"","instrumento":"","contraparte":""},
                    "rcp_derivativo_bool":"False","indexador_garantia":"CDI","epe_constante":True,"mitigacao_cea_2":"0","id_cenario":1,"ccf_manual":"False",
                    "ccf_cea":"100","ccf_kreg":"50","taxa_cliente":0,"taxa_ref":0,"taxa_bof":"0","formato_bof":"CDI","taxa_venda":"0","formato_venda":"CDI","fpr_variavel":"False",
                    "prazo_quebrado":"False","prazo_inicial":"0","prazo_final":"0","spread_variavel":"False","cd_quebra":"CIB"
                }

connDB = sqlite3.connect('Banco_dados_att.db')

#Criacao da classe que vai executar todos os testes de integracao com o DB
class testsIntegracao(unittest.TestCase):

    def test_matriz_migracao(self):             #funcao responsavel pelo teste de integracao matriz_migracao
        connDB = sqlite3.connect("Banco_dados_att.db")
        tela = Tela(json,connDB)         
        matriz_migracao = Matriz_migracao(connDB,tela)
        
        self.assertEqual(matriz_migracao.get_migracao('BAA4',30),0.958511843716806)
    
    def test_moeda_conversao(self):             #funcao responsavel pelo teste de integracao moeda_conversao
        moedaConversao = Moeda_conversao(connDB, 'USD')    #Criacao do objeto moeda_conversao
        self.assertEqual(moedaConversao.get_spot(), 5.3073)

    def test_regua_pd(self):                    #funcao responsavel pelo teste de integracao da regua_pd
        regua = Regua_pd (connDB)
        self.assertEqual(regua.get_pd_modelo(27,"BAA4"), 0.001117646)
        self.assertEqual(regua.get_vol_pd_modelo(27,"BAA4"),0.001201202)
        self.assertEqual(regua.get_wi(27,"BAA4"),0.767686216003)
        self.assertEqual(regua.get_piso_cea(1,"AAA"), 0)
        self.assertEqual(regua.is_rating_default(1,"AAA"), 'FALSE')
        self.assertEqual(regua.get_lgd_be(1,"AAA"), 0.5)
        self.assertEqual(regua.get_lgd_dt(1,"AAA"), 1.0)
        self.assertEqual(regua.get_pd_roe(27, "BAA4"), 0.0028022172)
        self.assertEqual(regua.get_vol_pd_roe(27, "AAA"), 0.00205886632)

    def test_visao_carteira(self):              #funcao responsavel pelo teste de integracao da visao de carteira
        visaoCarteira = Visao_carteira (20001,connDB)
        self.assertEqual (visaoCarteira.get_num_raroc(),0)
        self.assertEqual (visaoCarteira.get_cea_vp(),0.08)
        self.assertEqual (visaoCarteira.get_num_roe(),0)
        self.assertEqual (visaoCarteira.get_cr_vp(),0.08)
        self.assertEqual (visaoCarteira.get_prazo_medio(),30)
        self.assertEqual (visaoCarteira.get_ead_vp(),1)

    def test_cea_globais(self):                 #funcao responsavel pelo teste de integracao das ceas_globais
        ceaGlobais = Cea_globais (connDB)
        self.assertEqual(ceaGlobais.get_pe_port(), 10422315171.38)  
        self.assertEqual(ceaGlobais.get_sigma_port(), 1.4)
        self.assertEqual(ceaGlobais.get_pewi_port(), 1255929237)
        self.assertEqual(ceaGlobais.get_k1_port(), 1)
        self.assertEqual(ceaGlobais.get_k2_port(), 1.000097112)
        self.assertEqual(ceaGlobais.get_k3_port(), 2.1)
        self.assertEqual(ceaGlobais.get_var_port(), 29891428593)
        self.assertEqual(ceaGlobais.get_desvio_port(), 1899385345)
 
    def test_toolkit_finance(self):             #funcao responsavel pelo teste de integracao do toolkit_finance uso do banco de dados atualizao        
        indexador_garantia = json["indexador_garantia"]
        if json["indexador_garantia"] == "":
            indexador_garantia = json["indexador_garantia"] = "BRL"
        else:
            indexador_grantia = json["indexador_garantia"]
    
        curvas_juros = Curvas_juros(connDB,int(json["prazo_total"]), json["indexador"],indexador_garantia)
        toolKitF = Toolkit_finance(curvas_juros)

        self.assertEqual(toolKitF.get_frp(),0.224)  
        self.assertEqual(curvas_juros.get_curva_dc("BRL",3),1.000224094)
        self.assertEqual(curvas_juros.get_curva_du("BRL",3), 1.000224094)
        self.assertEqual(toolKitF.get_taxa_periodo_dc("BRL",3,4),0.027252959869894955)
        self.assertEqual(toolKitF.get_accrual_perc_di_du("BRL",3,4,5),1.0005070586344194)
        self.assertEqual(toolKitF.fator_pv_dc("BRL",3),0.9997759562068698)
        self.assertEqual(toolKitF.fator_fv_dc("BRL",3), 1.000224094)
        self.assertEqual(toolKitF.fator_pv_du ("BRL",3),0.9997759562068698)
        self.assertEqual(toolKitF.fator_fv_du("BRL",3),1.000224094)

        self.assertEqual(toolKitF.forward_curency_dc("BRL","JCP",1),1.0000027268037657)
        self.assertEqual(toolKitF.forward_curency_du("BRL","JCP",1),1.0000027268037657)
        self.assertEqual(toolKitF.forward_rate_dc ("BRL",3,4),1.0000746922619121)

        self.assertEqual(toolKitF.forward_rate_du ("BRL",3,4),1.0000746922619121)

        self.assertEqual(toolKitF.forward_rate_lin_dc ("BRL",3,4),0.00007470899999995062)

        self.assertEqual(toolKitF.get_accrual("BRL","BRL",30,2,4,22),0.9983988722972296)
        self.assertEqual(toolKitF.accrual_exp_360 ("BRL",3,4,2),1.0031312825295517 )

        self.assertEqual(toolKitF.accrual_exp_252 ("BRL",3,4,2),1.0044441079343156 )
        self.assertEqual(toolKitF.accrual_ipca ("BRL",3,4,2),1.0044441079343156) 

        self.assertEqual(toolKitF.accrual_lin_360("BRL",3,4,2),1.0056302645555555 )    
        self.assertEqual(toolKitF.diarizar_spread ("BRL",3,4,2),-0.0077164831889500185 )   
        self.assertEqual(toolKitF.get_rem_cea (1000000,720),154.0849814932188)  
        self.assertEqual(toolKitF.get_rem_cea_2 (1000000,720,3),153.88570720281658 ) 
        self.assertEqual(toolKitF.get_custo_n2 (3,400),0.00017511573015294157 ) 
        self.assertEqual(toolKitF.get_custo_n2_2 (1000000,720,400),62.16841381201377) 
        self.assertEqual(toolKitF.get_jcp(10,40),0.0005215474764663774)
 
    def test_tela(self):                        #funcao responsavel pelo teste de integracao da tela
        #Criacao dos Objetos utilizados nos testes
        tela = Tela(json,connDB)

        #Aux custo_manual
        if(json["custo_manual"] != ''):
            json["custo_manual"] = json["custo_manual"]
        else:
            json["custo_manual"] = 0

        #Auxiliar para teste fluxo
        vetor = tela.get_fluxo()
        def teste_fluxo(vetor):
            for i in range(len(vetor)):
                if ((vetor[i].data == json['fluxo'][i]['data'])
                and (vetor[i].valor_desembolso == json['fluxo'][i]['desembolso'])
                and (vetor[i].valor_amortizacao == json['fluxo'][i]['amortiza'])
                and (vetor[i].pagamento_juros == json['fluxo'][i]['juros'])
                and (vetor[i].perc_mitigacao == float(json['fluxo'][i]['mitiga'])/100)
                and (vetor[i].dc == json['fluxo'][i]['dc'])
                and (vetor[i].fpr == json['fluxo'][i]['fpr'])
                and (vetor[i].spread == json['fluxo'][i]['spread'])):
                    return True
                else:
                    return False

        #inicio dos testes dos metodos da Tela
        self.assertEqual(teste_fluxo(vetor), True)
        self.assertEqual(tela.get_id_grupo(), json['id_grupo'])
        self.assertEqual(tela.get_cnpj_cabeca(), json['cnpj_cabeca'])
        self.assertEqual(tela.get_rating_operacao(), json['rating_operacao'])
        self.assertEqual(tela.get_rating_grupo(), json['rating_grupo'])
        self.assertEqual(tela.get_id_segmento_risco_cabeca(), int(json['id_segmento_risco_cabeca']))
        self.assertEqual(tela.get_notional(), json['notional'])
        self.assertEqual(tela.get_indexador(), json['indexador'])
        self.assertEqual(tela.get_prazo_total(), float(json['prazo_total']))
        self.assertEqual(tela.get_produto(), json['produto'])
        self.assertEqual(tela.get_modalidade(), json['modalidade'])
        self.assertEqual(tela.get_classe_derivativo(), json['classe_produto'])
        self.assertEqual(tela.get_formato_taxa(), json['formato_taxa'])
        self.assertEqual(tela.get_spread(), float(json['spread'])/100)
        self.assertEqual(tela.get_commitment_spread(), json['commitment_spread'])
        self.assertEqual(tela.get_valor_limite_linha(), json['valor_limite_linha'])
        self.assertEqual(tela.get_ie(), float(json['ie'].replace(',', '.')))
        self.assertEqual(tela.get_fpr(), float(json['fpr'])/100)
        self.assertEqual(tela.get_mitigacao_cea(), float(json['mitigacao_cea']))
        self.assertEqual(tela.get_mitigacao_kreg(),float(json['mitigacao_kreg']))
        self.assertEqual(tela.get_onshore(),bool(json['onshore']))
        self.assertEqual(tela.get_receita_fee(), float(json['receita_fee']))
        self.assertEqual(tela.get_prazo_indeterminado(), tela.to_bool(json['prazo_indeterminado']))
        self.assertEqual(tela.get_lgd(), float(json['lgd'])/100)
        self.assertEqual(tela.get_custo_manual(), json['custo_manual'])
        self.assertEqual(tela.get_volatilidade(), json['id_volatilidade'])
        self.assertEqual(tela.get_prospectivo(), json['prospectivo'])
        self.assertEqual(tela.get_perc_bof(),float(json['perc_bof'].replace(',','.'))/100)
        self.assertEqual(tela.get_rcp_cred(), {})
        self.assertEqual(tela.get_indexador_garantia(), "CDI")
        self.assertEqual(tela.get_mitigacao_cea_2(), float(json['mitigacao_cea_2']))
        self.assertEqual(tela.get_id_cenario(), json['id_cenario'])
        self.assertEqual(tela.get_ccf_manual(), tela.to_bool(json['ccf_manual']))
        self.assertEqual(tela.get_taxa_cliente(), json['taxa_cliente'])
        self.assertEqual(tela.get_taxa_ref(), json['taxa_ref'])
        self.assertEqual(tela.get_taxa_bof(), float(json['taxa_bof']))
        self.assertEqual(tela.get_taxa_venda(), float(json['taxa_venda']))
        self.assertEqual(tela.get_formato_venda(), json['formato_venda'])
        self.assertEqual(tela.is_fpr_variavel(), tela.to_bool(json['fpr_variavel']))
        self.assertEqual(tela.get_prazo_inicial(),float(json['prazo_inicial']))
        self.assertEqual(tela.get_prazo_final(), float(json['prazo_final']))
        self.assertEqual(tela.is_spread_variavel(), tela.to_bool(json['spread_variavel']))
        self.assertEqual(tela.get_quebra(), json['cd_quebra'])

    def test_fepf(self):                        #funcao responsalvel pelo teste de funcionamento da integracao com banco de dados atualizado
        self.assertEqual(get_fepf(6, "ACO",connDB), 9.93333333333333)
    
    def test_cva(self):                         #funcao responsalvel pelo teste de funcionamento da integracao com banco de dados atualizado
        self.assertEqual(get_cva(23, 100, connDB), 129)

    def test_recalibrar_cea(self):
        cea_globais = Cea_globais(connDB)
        self.assertEqual(recalibrar_cea(cea_globais, 1,2,3,4,5,"FALSE"), 20.930994099256317)
        self.assertEqual(recalibrar_cea(cea_globais, 1,2,3,4,5,True), 21.002039352)  

    def test_recalibrar_cea_cluster(self):
        self.assertEqual(recalibrar_cea_cluster(1,2,3,4,5,"FALSE"), 9.966172212349028)
        self.assertEqual(recalibrar_cea_cluster(1,2,3,4,5,True), 2)

    def test_estoque_df(self):          #teste do estoque data_frame
         #Objetos para teste
        indexador_garantia = json["indexador_garantia"]
        if json["indexador_garantia"] == "":
            indexador_garantia = json["indexador_garantia"] = "BRL"
        else:
            indexador_grantia = json["indexador_garantia"]
    
        curvas_juros = Curvas_juros(connDB,int(json["prazo_total"]), json["indexador"],indexador_garantia)
        tela = Tela(json, connDB)
        regua = Regua_pd(connDB)
        cea_globais = Cea_globais(connDB)
        matriz_migracao = Matriz_migracao(connDB, tela)
        operacoes = get_operacoes_df(tela.cnpj_cabeca, tela.volatilidade)
        estoque_df = Estoque_df(matriz_migracao, regua,cea_globais, curvas_juros,operacoes,tela.cnpj_cabeca, tela.rating_grupo, tela.rating_operacao, tela.id_segmento_risco_cabeca,tela.id_grupo, tela.id_cenario, tela.volatilidade)
        nova = gerar_nova_op(tela.get_id_grupo(), tela.get_lgd(), tela.get_modalidade(), tela.get_produto(), tela.get_cnpj_cabeca(), tela.get_rating_grupo(), tela.get_id_segmento_risco_grupo(), tela.get_id_segmento_risco_cabeca(), tela.get_lgd(), tela.get_notional(), tela.get_prazo_total(), tela.get_spread(), tela.get_volatilidade(), operacoes, regua)
        
        #self.assertEqual(estoque_df.atualiza_estoque(10),1.000613667)
        #self.assertAlmostEqual(estoque_df.get_ead_somado(30), 3047377662.3046813,3)
        #self.assertAlmostEqual(estoque_df.get_prazo_ponderada(10), 1265.1939272421396,8)  #precisao de 8 casas decimais
        #self.assertAlmostEqual(estoque_df.get_pd_modelo_ponderada(10), 0.000488283, 10)    #precisao de 10 casas decimais
        #self.assertAlmostEqual(estoque_df.get_lgd_ponderada(10), 0.3395911981901973, 10)   #precisao de 10 casas decimais
        #self.assertAlmostEqual(estoque_df.get_wi_ponderada(30), 0.8486647526710001, 10)       #precisao de 10 casas decimais
        #self.assertEqual(estoque_df.get_cea_puro_op(10), 6078044.993013427)
        #self.assertEqual(estoque_df.get_cea_puro_grupo(10), 8506190.73045364)
        #self.assertEqual(estoque_df.get_k_grupo(10), 1.3994945315856187)
        #self.assertEqual(estoque_df.get_kreg_estoque(10, 10, 10), -27395872367.240765)

        #Testes cea
        self.assertAlmostEqual(estoque_df.get_cea_estoque_np(28), 21515815.578350563, 5)   #precisao de 5 casas decimais
        self.assertAlmostEqual(estoque_df.get_cea_estoque_sem_pe_np(28), 21212686.435024787, 5) #precisao de 5 casas decimais
        
        #testes de migracao    
        self.assertAlmostEqual(estoque_df.get_cea_estoque_migracao_lote(28, False), 40426257.70309837, 5) #precisao de 5 casa decimais
        self.assertAlmostEqual(estoque_df.get_cea_estoque_migracao_sem_pe_lote(28, False), 39732416.06920679, 5) #precisao de 5 casa decimais
        
        #testes com novas operacoes
        self.assertAlmostEqual(estoque_df.get_cea_marginal_np(28, nova),2083310.0889858715, 5) #precisao de 5 casa decimais
        self.assertAlmostEqual(estoque_df.get_cea_marginal_sem_pe_np(28, nova),2051121.884185873, 5) #precisao de 5 casa decimais

        #teste marginal_migracao
        self.assertAlmostEqual(estoque_df.get_cea_marginal_migracao_lote(28, nova),4394700.571033865, 5) #precisao de 5 casa decimais
        self.assertAlmostEqual(estoque_df.get_cea_marginal_migracao_sem_pe_lote(28, nova),4362512.366233878, 5) #precisao de 5 casa decimais
        
    #realizar testes explodir_fluxo_vertices, vertices_operacao e consolidar PNL
if __name__ == '__main__':
   unittest.main(verbosity=2)