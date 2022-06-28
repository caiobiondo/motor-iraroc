from datetime import *
import math
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
def monta_json(data):
    ####INICIAR VETOR DE FLUXO VAZIO####
    fluxo = []

    ####COLETA DE VARIAVEIS####
    amortizacao = str(data['Amortizacao'])
    desembolso = int(data['Desembolso'])
    carencia = int(data['Carencia'])
    periodo_amortizacao = str(data['Periodo_amortizacao'])
    unidade_amortizacao = str(data['Unidade_amortizacao'])
    periodo_carencia = str(data['Periodo_carencia'])
    unidade_carencia = str(data['Unidade_carencia'])
    notional = float(data['Notional'])
    desembolso_multiplicador = 1    #float(data['desembolso_multiplicador'])
    periodo_desembolso = 'Bullet'   #str(data['periodo_desembolso'])
    
    ####PEGAR DIA DA REALIZACAO DA OPERACAO####
    date_atual = date.today()
    dia = date_atual.day
    mes = date_atual.month
    ano = date_atual.year
    if(dia >= 10 and mes >= 10):
        data_em_texto = '{}/{}/{}'.format(mes, dia, ano)
    elif(dia < 10 and mes < 10):
        zero = "0"
        data_em_texto = '{}{}/{}{}/{}'.format(zero, mes, zero, dia, ano)
    elif(dia >= 10 and mes < 10):
        zero = "0"
        data_em_texto = '{}{}/{}/{}'.format(zero, mes, dia, ano)
    elif(dia < 10 and mes >= 10):
        zero = "0"
        data_em_texto = '{}/{}{}/{}'.format(mes, zero, dia, ano)             
    
    ####INICIO LOGICA CRIACAO DO JSON####
    if(desembolso != 0 ):
        fluxo_zero = {'data': data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'false', 'mitiga': 0, 'dc': 0}
        fluxo.append(fluxo_zero)
    
    proxima_data = date_atual + relativedelta(days=+desembolso)
    proximo_dia = proxima_data.day
    mes = proxima_data.month
    ano = proxima_data.year


    if(proximo_dia >= 10 and mes >= 10):
        nova_data_em_texto = '{}/{}/{}'.format(mes, proximo_dia, ano)
    elif(proximo_dia < 10 and mes < 10):
        zero = "0"
        nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes, zero, proximo_dia, ano)
    elif(proximo_dia >= 10 and mes < 10):
        zero = "0"
        nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes, proximo_dia, ano)
    elif(proximo_dia < 10 and mes >= 10):
        zero = "0"
        nova_data_em_texto = '{}/{}{}/{}'.format(mes, zero, proximo_dia, ano)    

    valor_parcela = notional / desembolso_multiplicador
    periodo_meses = 0

    if(periodo_desembolso == "Mensal"):
        periodo_meses = 0
    elif(periodo_desembolso == "Bimestral"):
        periodo_meses = 2
    elif(periodo_desembolso == "Trimestral"):
        periodo_meses = 3
    elif(periodo_desembolso == "Semestral"):
        periodo_meses = 6
    elif(periodo_desembolso == "Anual"):
        periodo_meses = 12
    elif(periodo_desembolso == "Bullet"):
        periodo_meses = 0
    
    if(periodo_meses == 0 or desembolso_multiplicador == 0):
        fluxo.append({'data': nova_data_em_texto, 'desembolso': str(notional), 'amortiza': '0', 'juros': 'false', 'mitiga': 0, 'dc': 0})
    else:
        for i in range(desembolso_multiplicador):
            fluxo.append({'data': nova_data_em_texto, 'desembolso': str(valor_parcela), 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})

    em_dias = 0
    em_meses = 0
    periodo_meses = 0

    dias = 0
    meses = 0
    anos = 0

    if(unidade_carencia == "Dias"):
        em_dias = carencia
    elif(unidade_carencia == "Meses"):
        em_meses = carencia
    elif(unidade_carencia == "Anos"):
        em_meses = carencia*12
    
    if(periodo_carencia == "Mensal"):
        periodo_meses = 1
    if(periodo_carencia == "Bimestral"):
        periodo_meses = 2
    if(periodo_carencia == "Trimestral"):
        periodo_meses = 3
    if(periodo_carencia == "Semestral"):
        periodo_meses = 6
    if(periodo_carencia == "Anual"):
        periodo_meses = 12
    if(periodo_carencia == "Bullet"):
        periodo_meses = 0
    
    if(periodo_meses == 0 and carencia != 0):
        data = fluxo[len(fluxo)-1]['data']
        ano = int(data[6:10])
        dia = int(data[3:5])
        mes = int(data[0:2])

        data_anterior = date(ano, mes, dia)
        nova_data = data_anterior + relativedelta(days=+em_dias ,months=+em_meses)

        dia_atual = nova_data.day
        mes_att = nova_data.month
        ano_atual = nova_data.year
        
        if(dia_atual >= 10 and mes_att >= 10):
                nova_data_em_texto = '{}/{}/{}'.format(mes_att, dia_atual, ano_atual)
        elif(dia_atual < 10 and mes_att < 10):
                zero = "0"
                nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes_att, zero, dia_atual, ano_atual)
        elif(dia_atual >= 10 and mes_att < 10):
                zero = "0"
                nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes_att, dia_atual, ano_atual)
        elif(dia_atual < 10 and mes_att >= 10):
                zero = "0"
                nova_data_em_texto = '{}/{}{}/{}'.format(mes_att, zero, dia_atual, ano_atual) 
                
        if(fluxo[len(fluxo) - 1]['data'] != nova_data_em_texto):
                fluxo.append({'data': nova_data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})
        
    else:
        if(em_dias > 0):

            while(em_dias > 0):
                data = fluxo[len(fluxo)-1]['data']
                ano_anterior = int(data[6:10])
                dia_anterior = int(data[3:5])
                mes_anterior = int(data[0:2])

                data_anterior = date(ano_anterior, mes_anterior, dia_anterior)
                data_nova = data_anterior + relativedelta(months=+periodo_meses)

                diferenca_dias = abs(data_nova - data_anterior).days

                if(em_dias >= diferenca_dias):
                    data_nova = data_nova
                else:
                    data_nova = data_anterior + relativedelta(days=+em_dias)

                dia_atual = data_nova.day
                mes_att = data_nova.month
                ano_atual = data_nova.year 
                 
                if(dia_atual >= 10 and mes_att >= 10):
                    nova_data_em_texto = '{}/{}/{}'.format(mes_att, dia_atual, ano_atual)
                elif(dia_atual < 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes_att, zero, dia_atual, ano_atual)
                elif(dia_atual >= 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes_att, dia_atual, ano_atual)
                elif(dia_atual < 10 and mes_att >= 10):
                    zero = "0"
                    nova_data_em_texto = '{}/{}{}/{}'.format(mes_att, zero, dia_atual, ano_atual) 
                
                if(fluxo[len(fluxo) - 1]['data'] != nova_data_em_texto):
                    fluxo.append({'data': nova_data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})

                em_dias -= diferenca_dias
        
        elif(em_meses > 0):
            while(em_meses > 0):
                data = fluxo[len(fluxo)-1]['data']
                ano_atual = int(data[6:10])
                dia_atual = int(data[3:5])
                mes_atual = int(data[0:2])
                data_atual = date(ano_atual, mes_atual, dia_atual)
                
                if(em_meses >= periodo_meses):    
                    data_nova = data_atual + relativedelta(months=+periodo_meses)
                    dia_atual = data_nova.day
                    mes_att = data_nova.month
                    ano = data_nova.year
                else:
                    data_nova = data_atual + relativedelta(months=+em_meses)
                    dia_atual = data_nova.day
                    mes_att = data_nova.month
                    ano = data_nova.year

                if(dia_atual >= 10 and mes_att >= 10):
                    nova_data_em_texto = '{}/{}/{}'.format(mes_att, dia_atual, ano)
                elif(proximo_dia < 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes_att, zero, dia_atual, ano)
                elif(proximo_dia >= 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes_att, dia_atual, ano)
                elif(proximo_dia < 10 and mes_att >= 10):
                    zero = "0"
                    nova_data_em_texto = '{}/{}{}/{}'.format(mes_att, zero, dia_atual, ano) 
                
                if(fluxo[len(fluxo) - 1]['data'] != nova_data_em_texto):
                    fluxo.append({'data': nova_data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})

                em_meses -= periodo_meses
            

    tamanho_fluxo_inicio_amortizacao = len(fluxo)

    em_dias = 0
    em_meses = 0
    periodo_meses = 0

    dias = 0
    meses = 0
    anos = 0

    valor_parcela = 0
    vertices_amortizacao = 0

    if(unidade_amortizacao == "Dias"):
        em_dias = int(amortizacao)
    elif(unidade_amortizacao == "Meses"):
        em_meses = int(amortizacao)
    elif(unidade_amortizacao == "Anos"):
        em_meses = int(amortizacao) * 12
    
    if(periodo_amortizacao == "Mensal"):
        periodo_meses = 1
    elif(periodo_amortizacao == "Bimestral"):
        periodo_meses = 2
    elif(periodo_amortizacao == "Trimestral"):
        periodo_meses = 3
    elif(periodo_amortizacao == "Semestral"):
        periodo_meses = 6
    elif(periodo_amortizacao == "Anual"):
        periodo_meses = 12
    elif(periodo_amortizacao == "Bullet"):
        periodo_meses = 0
    
    if(periodo_meses == 0 or amortizacao == 0):
        fluxo[len(fluxo)-1]['amortiza'] = str(notional)
    else:
        if(em_dias > 0):
            while(em_dias > 0):
                data = fluxo[len(fluxo)-1]['data']
                ano_anterior = int(data[6:10])
                dia_anterior = int(data[3:5])
                mes_anterior = int(data[0:2])

                date_atual = date.today()
                dia_novo = date_atual.day
                mes_novo = int(date_atual.month) + periodo_meses
                ano_novo = date_atual.year    

                data_anterior = date(ano_anterior, mes_anterior, dia_anterior)

                data_nova = data_anterior + relativedelta(months=+periodo_meses)

                diferenca_dias = abs(data_nova - data_anterior).days
                
                if(em_dias >= diferenca_dias):
                    data_nova = data_nova
                else:
                    data_nova = data_anterior + relativedelta(days=+em_dias)

                dia_atual = data_nova.day
                mes_att = data_nova.month
                ano_atual = data_nova.year 

                if(dia_atual >= 10 and mes_att >= 10):
                    nova_data_em_texto = '{}/{}/{}'.format(mes_att, dia_atual, ano_atual)
                elif(dia_atual < 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes_att, zero, dia_atual, ano_atual)
                elif(dia_atual >= 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes_att, dia_atual, ano_atual)
                elif(dia_atual < 10 and mes_att >= 10):
                    zero = "0"
                    nova_data_em_texto = '{}/{}{}/{}'.format(mes_att, zero, dia_atual, ano_atual) 
                
                if(fluxo[len(fluxo) - 1]['data'] != nova_data_em_texto):
                    fluxo.append({'data': nova_data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})

                em_dias -= diferenca_dias
        elif(em_meses > 0):
            while(em_meses > 0):
                data = fluxo[len(fluxo)-1]['data']
                ano = int(data[6:10])
                dia = int(data[3:5])
                mes = int(data[0:2])

                data = date(ano, mes, dia)
                data_nova = data + relativedelta(months=+periodo_meses)

                dia_atual = data_nova.day
                mes_att = data_nova.month
                ano_atual = data_nova.year

                if(dia_atual >= 10 and mes_att >= 10):
                    nova_data_em_texto = '{}/{}/{}'.format(mes_att, dia_atual, ano_atual)
                elif(proximo_dia < 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}{}/{}'.format(zero, mes_att, zero, dia_atual, ano_atual)
                elif(proximo_dia >= 10 and mes_att < 10):
                    zero = "0"
                    nova_data_em_texto = '{}{}/{}/{}'.format(zero, mes_att, dia_atual, ano_atual)
                elif(proximo_dia < 10 and mes_att >= 10):
                    zero = "0"
                    nova_data_em_texto = '{}/{}{}/{}'.format(mes_att, zero, dia_atual, ano_atual) 
                
                if(fluxo[len(fluxo) - 1]['data'] != nova_data_em_texto):
                    fluxo.append({'data': nova_data_em_texto, 'desembolso': '0', 'amortiza': '0', 'juros': 'true', 'mitiga': 0, 'dc': 0})

                em_meses -= periodo_meses
            

        
        vertices_amortizacao = len(fluxo) - tamanho_fluxo_inicio_amortizacao
        valor_parcela = notional / vertices_amortizacao

        i = tamanho_fluxo_inicio_amortizacao
        while i in range(len(fluxo)):
            fluxo[i]['amortiza'] = str(valor_parcela)
            i+=1
            
    #CALCULO DE DIAS CORRIDOS    
    fluxo[0]['dc'] = 0
    i = 1
    while i in range(len(fluxo)):
        data_ant = fluxo[0]['data']
        ano_ant = int(data_ant[6:10])
        dia_ant = int(data_ant[3:5])
        mes_ant = int(data_ant[0:2])

        data = fluxo[i]['data']
        ano_atual = int(data[6:10])
        dia_atual = int(data[3:5])
        mes_atual = int(data[0:2])

        data_ant = date(ano_ant, mes_ant, dia_ant)
        data = date(ano_atual, mes_atual, dia_atual)

        dc = data - data_ant 
    
        fluxo[i]['dc'] = dc.days
        i += 1

    return fluxo
    

    