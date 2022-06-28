from ToolKitDB.Cea_globais import Cea_globais
import numpy as np
from Capital.Capital import *

def recalibrar_cea(cea_globais, ead, cea_puro, pd_bis, prazo_rem, k_grupo, is_default):
    if (is_default == 'FALSE'):
        cea_puro_k1 = cea_puro * (k_grupo * ajuste_prazo_bis(prazo_rem, pd_bis) * cea_globais.get_k1_port())
        return max(cea_puro_k1 * cea_globais.get_k2_port(), 0) * cea_globais.get_k3_port()
    else:
        cea_puro_k1 = cea_puro * (k_grupo * cea_globais.get_k1_port())
        return cea_puro_k1 * cea_globais.get_k2_port() * cea_globais.get_k3_port()
 
def recalibrar_cea_cluster(cea_globais, ead, cea_puro, pd_bis, prazo_rem, k_grupo, is_default):
        if(is_default == 'FALSE'):
            cea_puro_k1 = cea_puro * (k_grupo * ajuste_prazo_bis(prazo_rem, pd_bis))
            return cea_puro_k1
        else:
            cea_puro_k1 = cea_puro
            return  cea_puro_k1

