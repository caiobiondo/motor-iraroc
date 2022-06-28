#CALCULA CLUSTER NO JS!
from ToolKitDB.MRC import MRC
from Objetos.Tela import Tela
from ToolKitDB.Parametros import *
def tabela_cluster(valor_mrc, mrc, tela, rating, is_corp):
        
        nome = ""
        quebra_1 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "BXS")
        quebra_2 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "BX")
        quebra_3 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "MRU")
        quebra_4 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "MRL")
        quebra_5 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "AR")
        quebra_6 = MRC.get_quebra_mrc(mrc, Tela.get_quebra(tela), "ARS")


        if (iscoupior(rating)):
            nome = "C ou pior"
        else:
            if (valor_mrc <= quebra_2):
                nome = "BXS";
            elif (valor_mrc <= quebra_3):
                nome = "BX";
            elif (valor_mrc <= quebra_4):
                nome = "MRU";
            elif (valor_mrc <= quebra_5):
                nome = "MRL";
            elif (valor_mrc <= quebra_6):
                nome = "AR";
            elif (valor_mrc > quebra_6):
               nome = "ARS";
            else:
                nome = "-"
            
        
        return nome
    