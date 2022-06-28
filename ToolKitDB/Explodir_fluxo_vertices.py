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
from ToolKitDB.Vetor_operacao_df import *
from Objetos.Estoque_df import Estoque_df
from Objetos.Vertices_operacao import Vertice_operacao
from ToolKitDB.Vetor_operacao_df import *

import numpy as np
import math
from numba import jit

def explodir_fluxo_vertice_mes_prazo_quebrado(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance, cronograma_fluxo, estoque_grupo, tela, operacoes):

    array_vertices = []

    for i in range(len(cronograma_fluxo)):
        if(i == (len(cronograma_fluxo) - 1)):
            array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
            veiculo_legal, toolkit_finance, array_vertices[(len(array_vertices) -1)], cronograma_fluxo[i], estoque_grupo, tela, False, True, len(array_vertices), operacoes))
        else:
            j = len(array_vertices) - 1

            dia = cronograma_fluxo[i].get_dc()
            
            while dia in range(cronograma_fluxo[i + 1].get_dc()):
                j += 1
                if(cronograma_fluxo[i].get_dc() == dia):
                    if(cronograma_fluxo[i].get_dc() == 0):
                        array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, 0, cronograma_fluxo[i], estoque_grupo, tela, True, False, j, operacoes))
                    else:
                        array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, array_vertices[j -1], cronograma_fluxo[i], estoque_grupo, tela, False, False, j, operacoes))
                else:
                    get_perc_mitigacao = Fluxo.get_perc_mitigacao(cronograma_fluxo[i])
                    fpr = Fluxo.get_fpr(cronograma_fluxo[i])
                    spread = Fluxo.get_spread(cronograma_fluxo[i])

                    fluxo_dummy = Fluxo(0,0,0,False, get_perc_mitigacao, dia, fpr, spread)

                    array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                    veiculo_legal, toolkit_finance, array_vertices[j - 1], fluxo_dummy, estoque_grupo, tela, False, False, j, operacoes))
                dia += 30
    
    existe_inicial = False
    inicial_split = -1
    existe_final = False
    final_split = -1

    for i in range(len(array_vertices)):
        if(array_vertices[i].prazo_decorrido == Tela.get_prazo_inicial(tela)):
            existe_inicial = True
        if(array_vertices[i].prazo_decorrido == Tela.get_prazo_final(tela)):
            existe_final = True
            array_vertices[i].pagamento_juros = True
        if(array_vertices[i].prazo_decorrido <= Tela.get_prazo_final(tela)):
            final_split = i
        
        if(array_vertices[i].prazo_decorrido <= Tela.get_prazo_inicial(tela)):
            inicial_split = i
    
    if(not(existe_inicial)):
        get_perc_mitigacao = array_vertices[inicial_split].mitigacao_cea
        fpr = array_vertices[inicial_split].fpr
        spread = array_vertices[inicial_split].spread

        fluxo_dummy = Fluxo(0,0,0, False, get_perc_mitigacao, Tela.get_prazo_inicial(tela), fpr, spread)

        split = Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                veiculo_legal, toolkit_finance, array_vertices[inicial_split - 1], fluxo_dummy, estoque_grupo, tela, False, False, j, operacoes)
        
        array_vertices.insert(inicial_split + 1, split)

    if(not(existe_final)):
        get_perc_mitigacao = array_vertices[final_split].mitigacao_cea
        fpr = array_vertices[final_split].fpr
        spread = array_vertices[final_split].spread

        fluxo_dummy = Fluxo(0,0,0, True, get_perc_mitigacao, Tela.get_prazo_final(tela), fpr, spread)

        split = Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                veiculo_legal, toolkit_finance, array_vertices[final_split - 1], fluxo_dummy, estoque_grupo, tela, False, False, j, operacoes)
        
        array_vertices.insert(final_split + 1, split)

    for i in range(len(array_vertices) - 1):
        array_vertices[i].set_vertice_posterior(array_vertices[i + 1])  
        if(i > 0):
            array_vertices[i].set_vertice_anterior(array_vertices[i - 1])   
        
    return array_vertices

def explodir_fluxo_vertice_dias(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance, cronograma_fluxo, estoque_grupo, tela, operacoes):
    array_vertices = []

    for i in range(len(cronograma_fluxo)):
        if(i == (len(cronograma_fluxo) - 1)):
            array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
            veiculo_legal, toolkit_finance, array_vertices[cronograma_fluxo[i].get_dc() - 1], cronograma_fluxo[i], estoque_grupo, tela, False, True, cronograma_fluxo[i].get_dc(), operacoes))
        else:
            dia = cronograma_fluxo[i].get_dc()
            while dia in range(cronograma_fluxo[i + 1].get_dc()):

                if(cronograma_fluxo[i].get_dc() == dia):

                    if(cronograma_fluxo[i].get_dc() == 0):
                        array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, 0, cronograma_fluxo[i], estoque_grupo, tela, True, False, dia, operacoes))
                    else:
                        array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, array_vertices[dia - 1], cronograma_fluxo[i], estoque_grupo, tela, False, False, dia, operacoes))
                else:
                    get_perc_mitigacao = cronograma_fluxo[i].get_perc_mitigacao()
                    fpr = cronograma_fluxo[i].get_fpr()
                    spread = cronograma_fluxo[i].get_spread()

                    fluxo_dummy = Fluxo(0,0,0, False, get_perc_mitigacao, dia, fpr, spread)

                    array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                    veiculo_legal, toolkit_finance, array_vertices[dia - 1], fluxo_dummy, estoque_grupo, tela, False, False, dia, operacoes))
                dia += 1

    for i in range((len(array_vertices) - 1)):
        array_vertices[i].set_vertice_posterior(array_vertices[i + 1])
        
    return array_vertices

#funcao OK
def explodir_fluxo_vertice_mes(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, veiculo_legal, toolkit_finance, cronograma_fluxo, estoque_grupo, tela, operacoes):
    array_vertices = []
    
    for i in range(len(cronograma_fluxo)):
        if(i == (len(cronograma_fluxo) - 1)):
            array_vertices.append(Vertice_operacao(connDB, moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
            veiculo_legal, toolkit_finance, array_vertices[len(array_vertices) - 1], cronograma_fluxo[i], estoque_grupo, tela, False, True, len(array_vertices), operacoes))
        else:
            j = len(array_vertices) - 1
            dia = cronograma_fluxo[i].get_dc()
            
            while dia in range((cronograma_fluxo[i + 1].get_dc())):
                j += 1
                if(cronograma_fluxo[i].get_dc() == dia):
                    if(cronograma_fluxo[i].get_dc() == 0):
                        array_vertices.append(Vertice_operacao(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, 0, cronograma_fluxo[i], estoque_grupo, tela, True, False, j, operacoes))
                    else:
                        array_vertices.append(Vertice_operacao(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                        veiculo_legal, toolkit_finance, array_vertices[j - 1], cronograma_fluxo[i], estoque_grupo, tela, False, False, j, operacoes))
                else:
                    get_perc_mitigacao = cronograma_fluxo[i].get_perc_mitigacao()
                    fpr = cronograma_fluxo[i].get_fpr()
                    spread = cronograma_fluxo[i].get_spread()
                    fluxo_dummy = Fluxo(0,0,0,False, get_perc_mitigacao, dia, fpr, spread)

                    array_vertices.append(Vertice_operacao(connDB,moeda_conversao, alfas_betas, produto_modalidade, regua, matriz_migracao, 
                    veiculo_legal, toolkit_finance, array_vertices[j - 1], fluxo_dummy, estoque_grupo, tela, False, False, j, operacoes))
                
                dia += 30
                #print(dia)
                
    for i in range(len(array_vertices) - 1):
        array_vertices[i].set_vertice_posterior(array_vertices[i + 1])
    
    return array_vertices

            