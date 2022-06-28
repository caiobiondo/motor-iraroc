#bibliotecas 
import sqlite3
import pandas as pd
import math
from ToolKitDB.Curvas_juros import Curvas_juros
from ToolKitDB.Parametros import *

class Toolkit_finance:
      
    def __init__(self, curvas_juros):
        self.curvas_juros = curvas_juros
        self.fpr = 0.224
    
    def get_frp(self):
        return self.fpr

    def get_taxa_periodo_dc(self,indexador,j,k):
        j = max(j,0)
        k = max(k,1)
        return pow(self.curvas_juros.get_curva_dc(indexador, k) / (self.curvas_juros.get_curva_dc(indexador, j)), (360 / (k -j))) - 1

    def get_taxa_periodo_dc(self,indexador,j,k):
        j = max(j,0)
        k = max(k,1)
        return pow(self.curvas_juros.get_curva_du(indexador, k) / (self.curvas_juros.get_curva_du(indexador, j)), (360 / (k -j))) - 1

    def get_accrual_perc_di_du(self,indexador,j,k,taxa):
        j = max(j,0)
        k = max(k,1)
        tx_an_per = self.get_taxa_periodo_dc(indexador,j,k)
        tx_an_per *= taxa
        return pow(tx_an_per + 1, (self.curvas_juros.get_du_from_dc(k) - self.curvas_juros.get_du_from_dc(j)) / 252)

    def fator_pv_dc (self,indexador, dc):
        return 1 / (self.curvas_juros.get_curva_dc(indexador,dc))

    def fator_fv_dc (self,indexador, dc):
        return self.curvas_juros.get_curva_dc(indexador,dc)

    def fator_pv_du (self,indexador, du):
        return 1 / (self.curvas_juros.get_curva_du(indexador,du))

    def fator_fv_du (self,indexador, du):
        return self.curvas_juros.get_curva_du(indexador,du)

    def forward_curency_dc (self,base_currency,quote_currency,dc):
        return self.curvas_juros.get_curva_dc(base_currency,dc)/self.curvas_juros.get_curva_dc(quote_currency,dc)

    def forward_curency_du (self,base_currency,quote_currency,du):
        return self.curvas_juros.get_curva_du(base_currency,du)/self.curvas_juros.get_curva_du(quote_currency,du)

    def forward_rate_dc (self,indexador, j,k):
        j = max(j,0)
        k = max(k,1)
        return self.curvas_juros.get_curva_dc(indexador,k)/self.curvas_juros.get_curva_dc(indexador,j)

    def forward_rate_du(self,indexador, j,k):
        j = max(j,0)
        k = max(k,1)
        return self.curvas_juros.get_curva_du(indexador,k)/self.curvas_juros.get_curva_du(indexador,j)

    def forward_rate_lin_dc(self,indexador,j,k):
        j = max(j,0)
        k = max(k,1)
        return self.curvas_juros.get_curva_dc(indexador,k) - self.curvas_juros.get_curva_dc(indexador,j)

    def accrual_exp_360(self,indexador,j,k,yield_spread):
        prazo = max((k-j),0)
        spread = pow((1+ float(yield_spread)),(prazo/360))
        return self.forward_rate_dc(indexador,j,k)*spread

    def accrual_exp_252(self,indexador,j,k,yield_spread):
        prazo = max((k-j),0)
        spread = pow((1+ float(yield_spread)),(prazo/252))
        return self.forward_rate_du(indexador,j,k)*spread

    def accrual_ipca(self,indexador,j,k,yield_spread):
        prazo = max((k-j),0)
        spread = pow((1+ float(yield_spread)),(prazo/252))
        return self.forward_rate_du(indexador,j,k)*spread

    def accrual_lin_360(self,indexador,j,k,yield_spread):
        prazo = max((k-j),0)
        spread = float(yield_spread) * prazo / 360
        return 1 + self.forward_rate_lin_dc(indexador,j,k) + spread
    
    def get_accrual (self, formato, indexador, j, k, yield_spread, taxa):
        if formato == "EXP/360":
            valor = self.accrual_exp_360(indexador,j,k,float(yield_spread))
        elif formato == "EXP/252":
            j = self.curvas_juros.get_du_from_dc(j)
            k = self.curvas_juros.get_du_from_dc(k)
            valor = self.accrual_exp_252(indexador,j,k,float(yield_spread))
        elif formato == "IPCA":
            j = self.curvas_juros.get_du_from_dc(j)
            k = self.curvas_juros.get_du_from_dc(k)
            valor = self.accrual_ipca(indexador,j,k,float(yield_spread))
        elif formato == "FLAT":
            j = self.curvas_juros.get_du_from_dc(j)
            k = self.curvas_juros.get_du_from_dc(k)
            valor = self.accrual_exp_252(indexador,j,k,float(yield_spread))  
        elif formato == "LIN/360":
            valor = self.accrual_lin_360(indexador,j,k,float(yield_spread))          
        elif formato == "%CDI":
            valor = self.get_accrual_perc_di_du(indexador,j,k,taxa)
        else:
            valor = self.accrual_exp_360(indexador,j,k,float(yield_spread))
        return valor

    def diarizar_spread(self, formato,yield_spread,j,k):
        valor = 0
        if formato == "EXP/360":
            valor = (pow(1+ float(yield_spread),1/360)-1)*(k-j) 
        elif formato == "EXP/252":
            valor = (pow(1+ float(yield_spread),1/252)-1)*(self.curvas_juros.get_du_from_dc(k)-self.curvas_juros.get_du_from_dc(j))
        elif formato == "LIN/360":
            valor = (float(yield_spread)/360)*(k-j)
        else:
            valor = (pow(1+ float(yield_spread),1/360)-1)*(k-j) 
        return valor

    def get_rem_cea(self,cea_estoque,prazo):
        return (cea_estoque/get_fatorn1()) * (pow(self.curvas_juros.get_curva_dc("SELIC_ECONOMICA",prazo), 1/prazo)-1)

    def get_rem_cea_2(self,cea_estoque,prazo,prazo2):
        if(prazo != prazo2):
            return (cea_estoque/get_fatorn1()) * (pow(self.curvas_juros.get_curva_dc("SELIC_ECONOMICA",prazo)/max(self.curvas_juros.get_curva_dc("SELIC_ECONOMICA",prazo2),1),1/(prazo - prazo2))-1) 
        else:
            return 0
    
    def get_custo_n2(self,cea_estoque, prazo_total):
        return (cea_estoque/get_fatorn1())*get_fatorn2()*(pow(self.curvas_juros.get_curva_dc("SELIC_ECONOMICA",prazo_total)*pow(1+ get_custon2(),prazo_total/360),1/prazo_total)-1)

    def get_custo_n2_2(self,cea_estoque,prazo_total,prazo_total2):
        if(prazo_total != prazo_total2):
            taxa = self.curvas_juros.get_curva_dc("SELIC_ECONOMICA", prazo_total) / max(self.curvas_juros.get_curva_dc("SELIC_ECONOMICA", prazo_total2), 1)
            return (cea_estoque / get_fatorn1()) * (get_fatorn2()) * (pow((taxa) * (pow(1 + get_custon2(), (prazo_total - prazo_total2)/360)), (1 / (prazo_total - prazo_total2))) - 1)
        else:
            return 0
    
    def get_jcp(self,cea_estoque,prazo_total):
        return cea_estoque * (pow(self.curvas_juros.get_perc_jcp(prazo_total), 1/prazo_total) - 1)

  



   
        
        
        
        
        
        
        
        