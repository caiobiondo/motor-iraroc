import pandas as pd
import numpy as np

#FUNCOES DE BUSCA DE PARAMETROS
def get_fatorpeliq ():
    return 0.60

def get_fatorbis (): 
    return 0.15

def get_fatorn1 ():
    return 0.8

def get_fatorn2 (): 
    return 0.2
        
def get_custon2 ():
    return 0.0619
      
def get_piscofins ():
    return 0.0465
      
def get_cea_ro (): 
    return 1.31
        
def get_fatorcva(): 
    return 1.3
        
def get_iss():
    return 0.05
      
def get_vetor_rating():
    
    vetor_rating = ["AAA", "AA3", "A1", "A3", "A4", "BAA1", "BAA3", "BAA4", "BA1", "BA4", "BA5", "BA6", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "D1", "D3", "E1", "F1", "G1", "H"]
          
    return vetor_rating

def get_vetor_rating_private_pf():
    vetor_rating = ["AA3", "BAA1", "B2", "C3"]
    
    return vetor_rating

def get_vetor_rating_private_pj():
    vetor_rating = ["BAA3", "BA5", "B2", "B4"]
    
    return vetor_rating

def get_vetor_rating_private_offshore():
    vetor_rating = ["BAA3"]

    return vetor_rating
def ishighyield_ccf(rating):
         
        if(rating == "B1" or
            rating == "B2" or
            rating == "B3" or
            rating == "B4" or
            rating == "C1" or
            rating == "C2" or
            rating == "C3" or
            rating == "D1" or
            rating == "D3" or
            rating == "E1" or
            rating == "F1" or
            rating == "G1" or
            rating == "H"):
          
            return True
        else:
            return False 
      
def flag_fianca_repac (rating,vol):
        
    vetor_rtg_baixa = [ "H", "G1", "F1", "E1", "D3", "D1", "C3", "C2", "C1", "B4", "B3", "B2", "B1", "BA6", "BA5"]
    vetor_rtg_media = [ "H", "G1" , "F1", "E1", "D3", "D1", "C3", "C2", "C1", "B4", "B3", "B2", "B1", "BA6", "BA5", "BA4"]
    vetor_rtg_alta = [ "H", "G1", "F1", "E1", "D3", "D1", "C3", "C2", "C1", "B4", "B3", "B2", "B1", "BA6", "BA5", "BA4", "BA1" ]
             
    if (vol == "1"):
        if (rating in vetor_rtg_baixa):
            return False 
        else: 
            return True 
            
    elif (vol == "2"):
        if (rating in vetor_rtg_media): 
            return False
        else: 
            return True
                    
    else: 
        if (rating in vetor_rtg_alta):
            return False 
        else: 
            return True 
                      
def iscoupior(rating):
            
            if ( rating == "C1" or 
                rating == "C2" or
                rating == "C3" or
                rating == "D1" or
                rating == "D3" or
                rating == "E1" or
                rating == "F1" or
                rating == "G1" or
                rating == "H"):
            
                
                return True
            else: 
                return False

def ajuste_12431(ir, ir_12431, cs, pis_cofins):
    ajuste = (1 - (ir_12431 + cs) - pis_cofins + (pis_cofins * cs)) / ((1 - (ir + cs)) * (1 - pis_cofins))
    return ajuste

#X_SELL
def get_spread_x_sell():    #Retornar zero por hora
    return 0

def get_ie_x_sell():    #Retornar zero por hora
    return 0

def get_ie_cred():  #Retornar zero por hora
    return 0