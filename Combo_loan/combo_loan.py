def CalculaComboLoan (instrumento1, contraparte1, localizacao_cred, localizacao_der, moeda_op, index_ativo_cred, index_ativo_der, index_passivo_der, prazo_tela, EEPE_CRED, EEPE_DER, EEPE_SINT, EEPE_JUROS, HV, HE, LMM1, moeda_limite):

        if(EEPE_SINT > 0):
            EEPE_SINT = 0
        else:
            EEPE_SINT = 1 
        
        if(EEPE_JUROS > 0):
            EEPE_JUROS = 0
        else:
            EEPE_JUROS = 1
        
        EEPE_CRED += -1 
        EEPE_JUROS += 0 
        EEPE_SINT += -1 
        
        valorComboRCPLimite = 0 
        valorComboRCPCEA = 0 
        RCPCredito =0 
        RCPDerivativo =0 
        RCPCreditoCEA =0 
        RCPDerivativoCEA =0 
        RCPSintetico =0 
        RCPSinteticoCEA = 0 
        LMM = 0 
        He = 0 
        Hv = 0 
        cobertura = 1 	
        perc_rcp_loan = 0 
        perc_rcp_deriv = 0 
        m0="" 
        m1="" 
        m2="" 
        m3="" 
        m4="" 
        m5="" 
        m6="" 
        m7="" 
        res = ["",""]
        
        frpDer = 0.224 
        prazo = prazo_tela
        moedaLimite = moeda_limite
        moedaCred = moeda_op 
        indAtivoCred = index_ativo_cred
        indAtivoDer = index_ativo_der
        indPassivoDer = index_passivo_der

        

        instrumento = instrumento1
        contraparte = contraparte1
        localizacaoCredito = localizacao_cred
        localizacaoDerivativo = localizacao_der 
        
        
        LMM = LMM 	
        He = HE 	
        Hv = HV 							
            
        
        res[0] = EEPE_CRED 
        RCPCreditoCEA = res[0] 
        if(res[1]!=""):
            m2=res[1].replace("."," ") + "da operação de crédito."+"\n" 
        res[0]= "'res[1]='" 
    
    
        res[0]	= EEPE_DER/100 
        RCPDerivativoCEA = res[0]*frpDer 
        if (res[1]!=""):
            m4=res[1].replace("."," ") + "do derivativo."+"\n" 
        res[0]= "' res[1]='" 
        
        if not(calcular_rcp_eepe("BRL", "BRL",index_ativo_der)):
            res[0] = EEPE_SINT 
            RCPSinteticoCEA = res[0] 
            if (res[1]!=""):
                m6=res[1].replace("."," ") + "da operação sintética."+"\n" 
    
            res[0]="'res[1]='" 
    
        if(m1 !="" and m2 !=""):
            m1 = "Utilizado par de indexadores default (XXX/YYY) no cálculo do %RCP Limite e do %RCP CEA da operação de crédito.\n" 
            m2 = "" 
    

        if(m3 !="" and m4 !=""):
            m3 = "Utilizado par de indexadores default (XXX/YYY) no cálculo do %RCP Limite e do %RCP CEA do derivativo.\n" 
            m4 = "" 
    

        if(m5 !="" and m6 !=""):
            m5 = "Utilizado par de indexadores default (XXX/YYY) no cálculo do %RCP Limite e do %RCP CEA da operação sintética.\n" 
            m6 = "" 
    

        m7 = "Obs: no NoRisk a mitigação é feita no Risco Efetivo e não no RCP das operações." 
        
        if (prazo == ""):
            
            return ("Favor informar o prazo do Combo Loan")
        
        else:
            if(instrumento == "Sem Vinculo"):
                if (verifica_igualdade_moedas(index_ativo_der,moeda_limite)):
                    RCP_juros =0 
                    RCP_juros_CEA = EEPE_JUROS 

                    valorComboRCPLimite = RCPCredito * 100 + RCP_juros * 100 
                    valorComboRCPCEA = RCPCreditoCEA * 100 + RCP_juros_CEA * 100
                    m0="Caso: operação de crédito com risco swapado para moeda do limite e ausência de vínculo jurídico.\n" 
                
                else:
                    valorComboRCPLimite = RCPCredito * 100 + RCPDerivativo  * 100 
                    valorComboRCPCEA = RCPCreditoCEA * 100 + RCPDerivativoCEA * 100 
                    m0="Para operações sem vínculo jurídico, apenas há tratamento para operações de hedge do ativo transferindo o risco para a moeda do limite.\n" + "No resultado foi considerado RCP Clean para as duas operações.\n" 
                
            else:
                if (verifica_igualdade_moedas(indAtivoDer,moedaLimite) and moedaCred != moedaLimite and moedaLimite == "BRL"):
                    if (Hv*He*cobertura > LMM ):
                        K_t = Hv*He/cobertura
                    else:
                        K_t = LMM
                    RCP_tot_cons_II =(1-K_t)*RCPCredito 
                    RC_clean =RCPCredito +RCPDerivativo  
                    RC_tot_Cons_II= min(RC_clean,max(0,RCP_tot_cons_II)) 

                    valorComboRCPLimite = RC_tot_Cons_II* 100 

                    RCP_tot_cons_II_CEA =(1-K_t)*RCPCreditoCEA 
                    RC_clean_CEA =RCPCreditoCEA +RCPDerivativoCEA  
                    RC_tot_Cons_II_CEA= min(RC_clean_CEA,max(0,RCP_tot_cons_II_CEA)) 
                    
                    valorComboRCPCEA = RC_tot_Cons_II_CEA * 100 
                    m0="Caso: operação de crédito com risco swapado para moeda do limite (BRL), com vínculo jurídico.\n" 		        		        
	
                elif (moedaCred == moedaLimite):
                    valorComboRCPLimite = RCPSintetico* 100 
                    valorComboRCPCEA = RCPSinteticoCEA* 100 
                    m0="Caso: operação de crédito na mesma moeda do limite, com risco no indexador ativo do derivativo e com vínculo jurídico.\n" 
        
                else:
                    if(Hv*He*cobertura > LMM ):
                        K_t = Hv*He/cobertura
                    else:
                        K_t = LMM
                
                    RCP_tot_caso_III = K_t*RCPSintetico+(1-K_t)*RCPCredito 
                    RC_clean =RCPCredito +RCPDerivativo  	
                    RC_tot_caso_III = min(RC_clean,max(0,RCP_tot_caso_III)) 

                    valorComboRCPLimite = RC_tot_caso_III* 100 				

                    RCP_tot_caso_III_CEA = K_t*RCPSinteticoCEA+(1-K_t)*RCPCreditoCEA 
                    RC_clean_CEA =RCPCreditoCEA +RCPDerivativoCEA  				
                    RC_tot_caso_III_CEA = min(RC_clean_CEA,max(0,RCP_tot_caso_III_CEA)) 

                    valorComboRCPCEA = RC_tot_caso_III_CEA* 100 
                    m0="Caso: operação de crédito com risco swapado para moeda qualquer, com vínculo jurídico.\n" 

        return valorComboRCPCEA/100 
        
def verifica_igualdade_moedas(moeda_1,moeda_2):

    lista_validos_brl = ["CDI", "IPCA", "TJLP", "TR","SELIC", "IGPM"]
    lista_validos_usd = [ "USD_LIBOR_01M", "USD_LIBOR_02M", "USD_LIBOR_03M", "USD_LIBOR_06M", "USD_LIBOR_12M"]
    lista_validos_eur = ["EUR_LIBOR_01M","EUR_LIBOR_02M", "EUR_LIBOR_03M","EUR_LIBOR_06M", "EUR_LIBOR_12M"]

    if(moeda_1== moeda_2):
        return True

    elif(moeda_1== "BRL"):
        if(moeda_2 in lista_validos_brl):
            return True
        else:
		    return False

    elif(moeda_2== "BRL"):
        if(moeda_1 in lista_validos_brl):
            return True
        else:
		    return False

    elif(moeda_1 == "USD"):
        if(moeda_2 in lista_validos_usd):
            return True
        else:
            return False

    elif (moeda_2== "USD"):
        if(moeda_1 in lista_validos_usd):
            return True
        else:
            return False

    elif(moeda_1== "EUR"):
        if(moeda_2 in lista_validos_eur):
            return True
        else:
            return False

    elif (moeda_2 == "EUR") :
        if(moeda_1 in lista_validos_eur):
            return True
        else:
            return False

    else:
        return False

def calcular_rcp_eepe(moeda_ativa, moeda_passivo,  indexador):
    lista_validos = ["CDI", "IPCA", "TJLP", "TR","SELIC", "IGPM"]

    if((moeda_ativa == "BRL") and (moeda_passivo == "BRL")):
	    if(indexador in lista_validos):
	        return True

    return False