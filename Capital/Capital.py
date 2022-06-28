import math
from ToolKitDB.Cea_globais import Cea_globais
import numpy as np
from ToolKitDB.CVA import *
from ToolKitDB.FEPF import *
from numba import njit
from Objetos.Tela import Tela

def bba_rcp(npz_dc, n_alfa, n_beta):
    nK = 2  #hard input

    if (n_beta != 0):
        npz_dc_be = nK * math.exp(-n_alfa/n_beta)
    else:
        npz_dc_be = 0
        
    if (npz_dc > npz_dc_be):
        n_rcp = n_alfa + n_beta * math.log(npz_dc)
        
    else:
        n_rcp = (n_alfa + n_beta + math.log(npz_dc_be)) * (npz_dc / npz_dc_be)

    return n_rcp

@njit(fastmath=True)
def cr_com_concentracao(n_pd, n_lgd, n_ead, n_wi, n_pe_wi, n_sigma, n_var, n_pe, n_desvio):
    return max(n_pd * n_lgd * n_ead * (n_lgd * n_ead + n_wi * n_sigma * n_sigma * n_pe_wi) * ((n_var - n_pe) / (n_desvio * n_desvio)), 0)

@njit(fastmath=True)    
def default_pi(ead, lgd_dt, lgd_be):
    return max((lgd_dt - lgd_be) * ead, 0)

@njit(fastmath=True)
def recalibrar_cea_nao_default(k1, k2, k3, ead, cea_puro, pd_bis, prazo_rem, k_grupo):
    cea_puro_k1 = cea_puro * (k_grupo * (1 + (min(prazo_rem, 1800) / 360 - 2.5) * pow((0.11852 - 0.05478 * math.log(pd_bis)), 2)) / (1 - 1.5 * (pow((0.11852 - 0.05478 * math.log(pd_bis)), 2))) * k1)
    return max(cea_puro_k1 * k2, 0) * k3

@njit(fastmath=True)
def recalibrar_cea_default(k1, k2, k3, ead, cea_puro, pd_bis, prazo_rem, k_grupo):
    cea_puro_k1 = cea_puro * (k_grupo * k1)
    return cea_puro_k1 * k2 * k3

@njit(fastmath=True)
def recalibrar_cea_cluster_nao_default(cea_puro, pd_bis, prazo_rem, k_grupo):
    cea_puro_k1 = cea_puro * (k_grupo * (1 + (min(prazo_rem, 1800) / 360 - 2.5) * pow((0.11852 - 0.05478 * math.log(pd_bis)), 2)) / (1 - 1.5 * (pow((0.11852 - 0.05478 * math.log(pd_bis)), 2))))
    return cea_puro_k1

@njit(fastmath=True)
def recalibrar_cea_cluster_default(cea_puro):
    return  cea_puro

def ajuste_prazo_bis(prazo, pd):
    return (1 + (min(prazo, 1800) / 360 - 2.5) * pow((0.11852 - 0.05478 * math.log(pd)), 2)) / (1 - 1.5 * (pow((0.11852 - 0.05478 * math.log(pd)), 2)))
    
def calcula_kreg_normal(ead, fpr, perc_n1, perc_bis):
        return min(ead * fpr * perc_n1 * perc_bis, ead)

def calcular_fepf(prazo_remanescente_dias, tipo_derivativo):
    
    if (prazo_remanescente_dias % 360 == 0):
        prazo_anos_convertido = math.floor(prazo_remanescente_dias / 360)
    else:
        prazo_anos_convertido = math.floor(prazo_remanescente_dias / 360) + 1
    
    return get_fepf(prazo_anos_convertido, tipo_derivativo)

def calcular_cva(prazo_remanescente_dias, fpr, connDB):
    
    fpr = fpr*100
    fpr_validos = [0,2,20,50,85,100]

    if(fpr not in fpr_validos):
        fpr = min(fpr_validos, key=lambda x:abs(x-fpr))
    else:
        fpr = fpr

    prazo_anos_convertido_inferior = math.floor(prazo_remanescente_dias / 360)
    prazo_anos_convertido_superior = math.floor(prazo_remanescente_dias / 360) + 1

    if((prazo_anos_convertido_inferior > 15) and (prazo_anos_convertido_inferior < 20)):
        prazo_anos_convertido_inferior = 15

    if(prazo_anos_convertido_superior > 15):
        prazo_anos_convertido_superior = 20

    return (prazo_remanescente_dias - prazo_anos_convertido_inferior * 360)*(float((get_cva(prazo_anos_convertido_superior, fpr,connDB) - get_cva(prazo_anos_convertido_inferior, fpr,connDB))) / 360) + float(get_cva(prazo_anos_convertido_inferior,fpr,connDB))

def get_curva_hurdle(prazo, hurdle):
    
        if (prazo <= 731):

            return hurdle
 
        elif (prazo <= 1096):
            retorno = (((prazo - 731) / (1096 - 731)) * (0.0015)) + hurdle;

            return retorno;

        elif (prazo <= 1461):
            hurdle_novo = hurdle + (0.0015)
            retorno = (((prazo - 1096) / (1461 - 1096)) * (0.0030)) + hurdle_novo;

            return retorno;

        elif (prazo <= 1826):
            hurdle_novo = hurdle + (0.0045)
            retorno = (((prazo - 1461) / (1826 - 1461)) * (0.0050)) + hurdle_novo;

            return retorno
                     
        elif (prazo > 1826):
            hurdle_novo = hurdle + (0.0095)
            retorno = min((((prazo - 1826) / (2191 - 1826))), 1) * (0.0055) + hurdle_novo;
            
            return retorno
        else:
            return (hurdle + 0.015)
