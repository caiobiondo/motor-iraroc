from ToolKitDB.Vetor_operacao_df import *
from Objetos.Tela import Tela
from Objetos.Estoque_df import Estoque_df
from Objetos.Vertices_operacao import Vertice_operacao
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Moeda_conversao import Moeda_conversao
from ToolKitDB.Veiculo_legal import Veiculo_legal
from ToolKitDB.Visao_carteira import Visao_carteira
from ToolKitDB.Parametros import *
from ToolKitDB.Explodir_fluxo_vertices import *
from Capital.Capital import *
from ToolKitDB.Vetor_operacao_df import *
from Cluster.ConsolidarCluster import *
from Raroc.Consolidar_vertices import *
import math

def float_2_spread(moeda):
    moeda = round(moeda * 100000) / 1000
    return float(moeda)

def float_2_perc(valor, casas):
    aux = 1
    for i in range(casas):
        aux = aux * 10
    valor = round(valor*100 * aux) / aux
    valor = str(valor).replace('.',',')
    return (valor)

def consolidar_pnl(connDB, nova, estoque_df, estoque_vazio,alfas_betas, produto_modalidade, meta, tela, volatilidade, matriz_migracao, regua, cea_globais, curvas_juros, toolkit_finance, moeda_conversao, veiculo_legal, visao_carteira, operacoes_grupo, operacoes_vazio):
    if(bool(meta)):                #dicionarios vazio em python retornam False e preenchidos retornam True
        
        ratings_otimos = ["BAA1", "A4", "A3", "A1", "AA3", "AAA"]   #Spreads variando majoritareamente entre 1% e 0.5%
        ratings_bons = ["BAA4", "BAA3"]  #Spreads variando entre 1.5% e 0.75%
        ratings_medios = ["BA1"] #Spreads variando entre 2.5% e 1.25%
        ratings_medios_ruins = ["BA4"]  #Spreads variando entre 3.2% e 1.6%
        ratings_ruins = ["BA5", "BA6"]     #Spreads variando entre 7% e 3.5%
        ratings_resto = ["B1", "B2", "B3", "B4", "C1", "C2", "C3", "D1", "D3", "E1", "F1", "G1", "H"]   #Range maior de spreads para ratings piores

        # zero como spread minimo para evitar problemas de atingir meta quando sai do escopo de minimos
        if(Tela.get_rating_operacao(tela) in ratings_otimos):
            taxa_min = 0 
            taxa_max = 1.3
        elif(Tela.get_rating_operacao(tela) in ratings_bons):
            taxa_min = 0 
            taxa_max = 1.5
        elif(Tela.get_rating_operacao(tela) in ratings_medios):
            taxa_min = 0 
            taxa_max = 2.5
        elif(Tela.get_rating_operacao(tela) in ratings_medios_ruins):
            taxa_min = 0 #
            taxa_max = 3.2
        elif(Tela.get_rating_operacao(tela) in ratings_ruins):
            taxa_min =  0 
            taxa_max = 7
        else:
            taxa_min = 0 
            taxa_max = 18

        taxa_zero = 0
        aux_raroc = 0
        limite_max = 100    
        erro = 0.001
        alvo = float(meta["alvo"])/100        #meta.alvo;
        
        if "id" in meta["objetivo"]:
            objetivo = meta["objetivo"]["id"]     #meta.objetivo.id;
        else:
            objetivo = meta["objetivo"]
        
        if "id" in meta["objetivo"]:          
            variavel = meta["variavel"]["id"]     #meta.variavel.id;
        else:
            variavel = meta["variavel"]
        fase = 0

        if(variavel == "Fee"):
            taxa_max = 300000000
            taxa_min = -300000000
        
        if(objetivo == "CV" or objetivo == "DELTA CV" or objetivo == "DELTA POOL"):    
            alvo = alvo*100
            erro = 1
            aux_raroc = 2
    
    else:
        aux_raroc = -1
        limite_max = 1      
        erro = 0.0001
        alvo = 1000

        objetivo = ""
        taxa_zero = 0
        taxa_max = 0

    array_constantes = []
    rodada_aux = 0
    i = 0
    for k in range(limite_max): 
        if(k != 0):
            i += 1
            
        prazo_total = Tela.get_prazo_total(tela)
        cronograma_fluxo = Tela.get_fluxo(tela)
        dif = aux_raroc - alvo

        if(dif > erro or (dif) < 0): #VERIFICA SE O RAROC_ATUAL ESTA FORA DO RAROC_OBJ
            
            if((i == 1) and (aux_raroc < alvo)):
                taxa_max += 1
                taxa_min = taxa_max/2
                i = 0
                taxa_zero = taxa_max
            elif((i == 1) and ((alvo + 0.2) < aux_raroc < (alvo + 10))):
                taxa_max = taxa_max/2
                taxa_min = 0
                taxa_zero = taxa_max
            elif((i == 1) and (aux_raroc > (alvo + 10))):
                taxa_max = taxa_max/10
                taxa_min = 0
                taxa_zero = taxa_max

            if(i != 0):
                if(not(objetivo == "DELTA CV" or objetivo == "DELTA POOL")):
                    if(aux_raroc > alvo):
                        taxa_max = taxa_zero
                    elif(aux_raroc < alvo):
                        taxa_min = taxa_zero    
                else:
                    if(aux_raroc < alvo):
                        taxa_max = taxa_zero
                    else:
                        taxa_min = taxa_zero

                taxa_zero = (taxa_max + taxa_min)/2 #ajuste de spread
            else:
                
                taxa_zero = taxa_max

            if(bool(meta)):     #dicionario vazio retorna false
                if(variavel == "Spread"):
                    Tela.set_spread(tela, taxa_zero)
                    troca_spread_nova_op(nova, taxa_zero)    
                elif(variavel == "Fee"):
                    Tela.set_receita_fee(tela, taxa_zero)
                elif(variavel == "Commitment Spread"):
                    Tela.set_commitment_spread(tela, taxa_zero)
                elif(variavel == "Taxa Cliente"):
                    Tela.set_taxa_cliente(tela, taxa_zero)
                elif(variavel == "Taxa Venda"):
                    Tela.set_taxa_venda(tela, taxa_zero)
                elif(variavel == "Taxa BOF"):
                    Tela.set_taxa_bof(tela, taxa_zero)

            if(rodada_aux == 0): #regras para primeira rodada
                rodada_aux = 1
                if(Tela.get_prazo_quebrado(tela)):
                    vertices = explodir_fluxo_vertice_mes_prazo_quebrado(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_grupo) 
                
                else:
                    if(prazo_total < 124):
                        vertices = explodir_fluxo_vertice_dias(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_grupo)   

                    else:
                        vertices = explodir_fluxo_vertice_mes(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_grupo)         

                df_ead_novo = Estoque_df.gerar_df_ead_novo(estoque_df, vertices, Tela.get_indexador(tela))

                df_setup = Estoque_df.get_setup_raroc(estoque_df, vertices, nova, df_ead_novo)
                estoque_completo_array = Estoque_df.gerar_estoque_completo(estoque_df, nova, df_ead_novo, vertices, df_setup[1], df_setup[0])
                estoque_completo = estoque_completo_array[0]
                estoque_na_carteira = estoque_completo[:,[0,1,2]]
                estoque_pe = estoque_completo_array[1]

            else:   # Regras para segunda rodada em diante (considerar o estoque vazio, visto que o capital da carteira é imutavel)
                if(Tela.get_prazo_quebrado(tela)):
                    vertices = explodir_fluxo_vertice_mes_prazo_quebrado(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_vazio) 
                
                else:
                    if(prazo_total < 124):
                        vertices = explodir_fluxo_vertice_dias(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_vazio)   

                    else:
                        vertices = explodir_fluxo_vertice_mes(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance,cronograma_fluxo, estoque_df, tela, operacoes_vazio)         

                df_ead_novo = Estoque_df.gerar_df_ead_novo(estoque_vazio, vertices, Tela.get_indexador(tela))

                estoque_completo_array = Estoque_df.gerar_estoque_completo(estoque_vazio, nova, df_ead_novo, vertices, df_setup[1], df_setup[0])
                estoque_completo = estoque_completo_array[0]
                estoque_completo[:,1] = estoque_na_carteira[:,1]
                estoque_completo[:,2] = estoque_na_carteira[:,2]
                estoque_pe = estoque_completo_array[1]
            
            [Vertice_operacao.calcula_capital(vertice, estoque_completo) for vertice in vertices]
            [Vertice_operacao.calcula_pe(vertice, estoque_pe) for vertice in vertices]
            
            consolidado = consolidar_vertices(vertices, toolkit_finance, moeda_conversao, tela, visao_carteira, veiculo_legal, array_constantes, k)
            array_constantes = consolidado[1]

            if(objetivo == "RAROX" or objetivo == "iRARoC"):
                aux_raroc = consolidado[0]["CALCULO_RAROC_OPERACAO"]

    if(bool(meta)):
        raroc_operacao_original = ((consolidado[0]["CALCULO_RAROC_OPERACAO"])*100)  #RAROC RETORNADO DO ATINGIR META
        range_aceitavel = 0.1 
        alvo_ajustado = round(alvo*100,2)

        if(raroc_operacao_original == alvo_ajustado):
            raroc_operacao = (float_2_perc(consolidado[0]["CALCULO_RAROC_OPERACAO"], 1))
        else:
            dif = raroc_operacao_original - (alvo*100)
            if(0 <= dif <= range_aceitavel):
                raroc_operacao = (float_2_perc(alvo, 1))
    
    else:
        raroc_operacao = (float_2_perc(consolidado[0]["CALCULO_RAROC_OPERACAO"], 1))
    
    if(Tela.get_produto(tela) == "SEGURO"):
                valor_1 = round(float(consolidado[0]["CALCULO_CRIACAO_VALOR"])/float(consolidado[0]["CALCULO_MFB_RAROC"]),2)   
                valor_2 = round(float(consolidado[0]["CALCULO_RGO_RAROC"])/float(consolidado[0]["CALCULO_MFB_RAROC"]),2)

                calculo_resultado = {
                    "CALCULO_SPREAD_RAROC": (float_2_spread(consolidado[0]["CALCULO_SPREAD_RAROC"])),    
                    "CALCULO_SPREAD_ROE": (float_2_spread(consolidado[0]["CALCULO_SPREAD_ROE"])), 
                    "CALCULO_MFB_RAROC": (consolidado[0]["CALCULO_MFB_RAROC"]), 
                    "calculo_mfb_roe": (consolidado[0]["calculo_mfb_roe"]), 
                    "CALCULO_RECEITAS_RAROC": (consolidado[0]["CALCULO_RECEITAS_RAROC"]), 
                    "calculo_receitas_roe": (consolidado[0]["calculo_receitas_roe"]), 
                    "CALCULO_GIROPROPRIO_RAROC": (consolidado[0]["CALCULO_GIROPROPRIO_RAROC"]),
                    "calculo_giroproprio_roe": (consolidado[0]["calculo_giroproprio_roe"]),
                    "CALCULO_TRIBUTARIAS_RAROC": (consolidado[0]["CALCULO_TRIBUTARIAS_RAROC"]),
                    "calculo_tributarias_roe": (consolidado[0]["calculo_tributarias_roe"]), 
                    "CALCULO_PERDA_RAROC": (consolidado[0]["CALCULO_PERDA_RAROC"]), 
                    "calculo_perda_roe": (consolidado[0]["calculo_perda_roe"]),
                    "CALCULO_CUSTOSADM_RAROC": (consolidado[0]["CALCULO_CUSTOSADM_RAROC"]),
                    "calculo_custo_adm_roe": (consolidado[0]["calculo_custo_adm_roe"]),
                    "CALCULO_RESULTADO_ANTES_IR_RAROC": (consolidado[0]["CALCULO_RESULTADO_ANTES_IR_RAROC"]),
                    "calculo_resultado_antes_ir_roe": (consolidado[0]["calculo_resultado_antes_ir_roe"]),
                    "CALCULO_IR_RAROC": (consolidado[0]["CALCULO_IR_RAROC"]),
                    "calculo_ir_roe": (consolidado[0]["calculo_ir_roe"]),
                    "CALCULO_JCP_RAROC": (consolidado[0]["CALCULO_JCP_RAROC"]),
                    "calculo_jcp_roe": (consolidado[0]["calculo_jcp_roe"]),
                    "CALCULO_RGO_RAROC": (consolidado[0]["CALCULO_RGO_RAROC"]),
                    "calculo_rgo_roe": (consolidado[0]["calculo_rgo_roe"]),
                    "CALCULO_CEA_RAROC": (consolidado[0]["CALCULO_CEA_RAROC"]),
                    "calculo_kreg_roe": (consolidado[0]["calculo_kreg_roe"]),
                    "CALCULO_PERC_CEA_RAROC": float_2_perc(consolidado[0]["CALCULO_PERC_CEA_RAROC"], 1),          
                    "calculo_perc_kreg_roe": float_2_perc(consolidado[0]["calculo_perc_kreg_roe"], 1),
                    "CALCULO_CUSTO_CAPITAL": (consolidado[0]["CALCULO_CUSTO_CAPITAL"]),
                    "CALCULO_CRIACAO_VALOR": (consolidado[0]["CALCULO_CRIACAO_VALOR"]),
                    "CALCULO_RAROC_OPERACAO": (float_2_perc(consolidado[0]["CALCULO_RAROC_OPERACAO"], 1)),
                    "CALCULO_RAROC_GRUPO": float_2_perc(consolidado[0]["CALCULO_RAROC_GRUPO"], 1),
                    "CALCULO_RAROC_COMBINADO": float_2_perc(consolidado[0]["CALCULO_RAROC_COMBINADO"], 1),
                    "CALCULO_ROE_OPERACAO":"",
                    "CALCULO_EAD_CEA_MEDIA": (consolidado[0]["CALCULO_EAD_CEA_MEDIA"]),
                    "CALCULO_EAD_KREG_MEDIA": consolidado[0]["CALCULO_EAD_KREG_MEDIA"],
                    "CALCULO_PZO_TOTAL": consolidado[0]["CALCULO_PZO_TOTAL"],
                    "CALCULO_CCF_CEA": consolidado[0]["CALCULO_CCF_CEA"],
                    "CALCULO_CCF_KREG": consolidado[0]["CALCULO_CCF_KREG"],
                    "CALCULO_PERC_UTI": "Em breve",
                    "HURDLE": consolidado[0]["HURDLE"],
                    "CALCULO_RES_META": taxa_zero,
                    "VL_BOF":consolidado[0]["VL_BOF"],
                    "PGTO_ATIVOS": consolidado[0]["PGTO_ATIVOS"],
                    "PGTO_PASSIVOS": consolidado[0]["PGTO_PASSIVOS"],
                    "PGTO_ATIVOS_VP": consolidado[0]["PGTO_ATIVOS_VP"], 
                    "PGTO_PASSIVOS_VP": consolidado[0]["PGTO_PASSIVOS_VP"], 
                    "PGTO_ATIVOS_PAR": consolidado[0]["PGTO_ATIVOS_PAR"],
                    "PGTO_PASSIVOS_PAR": consolidado[0]["PGTO_PASSIVOS_PAR"],
                    "RESULTADO_VENDA": consolidado[0]["RESULTADO_VENDA"],
                    "RESULTADO_VENDA_LIQ": consolidado[0]["RESULTADO_VENDA_LIQ"],
                    "DELTA_CV": (consolidado[0]["DELTA_CV"]),
                    "DELTA_POOL": (consolidado[0]["DELTA_POOL"]),
                    "CALCULO_RAROC_CRIACAO_VALOR_PREMIO":valor_1,
                    "CALCULO_RAROC_RGO_PREMIO":valor_2
                    }
            
    else:
                calculo_resultado = {
                    "CALCULO_SPREAD_RAROC": (float_2_spread(consolidado[0]["CALCULO_SPREAD_RAROC"])),    
                    "CALCULO_SPREAD_ROE": (float_2_spread(consolidado[0]["CALCULO_SPREAD_ROE"])), 
                    "CALCULO_MFB_RAROC": (consolidado[0]["CALCULO_MFB_RAROC"]), 
                    "calculo_mfb_roe": (consolidado[0]["calculo_mfb_roe"]), 
                    "CALCULO_RECEITAS_RAROC": (consolidado[0]["CALCULO_RECEITAS_RAROC"]), 
                    "calculo_receitas_roe": (consolidado[0]["calculo_receitas_roe"]), 
                    "CALCULO_GIROPROPRIO_RAROC": (consolidado[0]["CALCULO_GIROPROPRIO_RAROC"]),
                    "calculo_giroproprio_roe": (consolidado[0]["calculo_giroproprio_roe"]),
                    "CALCULO_TRIBUTARIAS_RAROC": (consolidado[0]["CALCULO_TRIBUTARIAS_RAROC"]),
                    "calculo_tributarias_roe": (consolidado[0]["calculo_tributarias_roe"]), 
                    "CALCULO_PERDA_RAROC": (consolidado[0]["CALCULO_PERDA_RAROC"]), 
                    "calculo_perda_roe": (consolidado[0]["calculo_perda_roe"]),
                    "CALCULO_CUSTOSADM_RAROC": (consolidado[0]["CALCULO_CUSTOSADM_RAROC"]),
                    "calculo_custo_adm_roe": (consolidado[0]["calculo_custo_adm_roe"]),
                    "CALCULO_RESULTADO_ANTES_IR_RAROC": (consolidado[0]["CALCULO_RESULTADO_ANTES_IR_RAROC"]),
                    "calculo_resultado_antes_ir_roe": (consolidado[0]["calculo_resultado_antes_ir_roe"]),
                    "CALCULO_IR_RAROC": (consolidado[0]["CALCULO_IR_RAROC"]),
                    "calculo_ir_roe": (consolidado[0]["calculo_ir_roe"]),
                    "CALCULO_JCP_RAROC": (consolidado[0]["CALCULO_JCP_RAROC"]),
                    "calculo_jcp_roe": (consolidado[0]["calculo_jcp_roe"]),
                    "CALCULO_RGO_RAROC": (consolidado[0]["CALCULO_RGO_RAROC"]),
                    "calculo_rgo_roe": (consolidado[0]["calculo_rgo_roe"]),
                    "CALCULO_CEA_RAROC": (consolidado[0]["CALCULO_CEA_RAROC"]),
                    "calculo_kreg_roe": (consolidado[0]["calculo_kreg_roe"]),
                    "CALCULO_PERC_CEA_RAROC": float_2_perc(consolidado[0]["CALCULO_PERC_CEA_RAROC"], 1),          
                    "calculo_perc_kreg_roe": float_2_perc(consolidado[0]["calculo_perc_kreg_roe"], 1),
                    "CALCULO_CUSTO_CAPITAL": (consolidado[0]["CALCULO_CUSTO_CAPITAL"]),
                    "CALCULO_CRIACAO_VALOR": (consolidado[0]["CALCULO_CRIACAO_VALOR"]),
                    "CALCULO_RAROC_OPERACAO": raroc_operacao,
                    "CALCULO_RAROC_GRUPO": float_2_perc(consolidado[0]["CALCULO_RAROC_GRUPO"], 1),
                    "CALCULO_RAROC_COMBINADO": float_2_perc(consolidado[0]["CALCULO_RAROC_COMBINADO"], 1),
                    "CALCULO_ROE_OPERACAO":"",
                    "CALCULO_EAD_CEA_MEDIA": (consolidado[0]["CALCULO_EAD_CEA_MEDIA"]),
                    "CALCULO_EAD_KREG_MEDIA": consolidado[0]["CALCULO_EAD_KREG_MEDIA"],
                    "CALCULO_PZO_TOTAL": consolidado[0]["CALCULO_PZO_TOTAL"],
                    "CALCULO_CCF_CEA": consolidado[0]["CALCULO_CCF_CEA"],
                    "CALCULO_CCF_KREG": consolidado[0]["CALCULO_CCF_KREG"],
                    "CALCULO_PERC_UTI": "Em breve",
                    "HURDLE": consolidado[0]["HURDLE"],
                    "CALCULO_RES_META": taxa_zero,
                    "VL_BOF":consolidado[0]["VL_BOF"],
                    "PGTO_ATIVOS": consolidado[0]["PGTO_ATIVOS"],
                    "PGTO_PASSIVOS": consolidado[0]["PGTO_PASSIVOS"],
                    "PGTO_ATIVOS_VP": consolidado[0]["PGTO_ATIVOS_VP"], 
                    "PGTO_PASSIVOS_VP": consolidado[0]["PGTO_PASSIVOS_VP"], 
                    "PGTO_ATIVOS_PAR": consolidado[0]["PGTO_ATIVOS_PAR"],
                    "PGTO_PASSIVOS_PAR": consolidado[0]["PGTO_PASSIVOS_PAR"],
                    "RESULTADO_VENDA": consolidado[0]["RESULTADO_VENDA"],
                    "RESULTADO_VENDA_LIQ": consolidado[0]["RESULTADO_VENDA_LIQ"],
                    "DELTA_CV": (consolidado[0]["DELTA_CV"]),
                    "DELTA_POOL": (consolidado[0]["DELTA_POOL"])
                    }
            
    return calculo_resultado   
