#{"id_volatilidade":"BD","id_grupo":2,"ratingGrupo":"BA5","ratingOper":"BA5","cnpj_cabeca":60619202}'
from Calculo.Calcular_cluster import *
import pandas as pd
import json
import sqlite3

def ajuste_cluster(data):

    connection = sqlite3.connect("Banco_dados_att.db")
    cursor = connection.cursor()
    data_json = json.loads(data['operacao'])
    
    cluster = data['cluster']

    tela = {"id_grupo":"","mitigacaoCEA2":"0","id_cenario":1,"cnpj_cabeca":"","ratingOper":"","ratingGrupo":"","id_segmento_risco_cabeca":"27","id_segmento_risco_grupo":"27","notional":1,"indexador":"BRL","prazo_total":"1080","qtd_parcela":"2","dt_inicio":"05/06/2018","dt_final":"05/06/2021","produto":"KG","modalidade":"Normal","classe_produto":"ATI","perc_uti":"100","fluxo":[{"data":"05/06/2018","desembolso":"1","amortiza":"0","juros":"false","mitiga":"0","dc":0},{"data":"05/06/2021","desembolso":"0","amortiza":"1","juros":"true","mitiga":"0","dc":1080}],"formato_taxa":"EXP/360","spread":"0","CommitmentSpread":"0","ValorLimiteLinha":"0","IE":"12.2","qtde_titulos":"","pb_adicional":"0","iss":"true","fpr":"100","frp":"0","mitigacaoCEA":"0","MitigacaoKREG":"0","FXSpot":"1","pzoMedio":"true","onShore":"true","FEE":"0","raroc_ref":"14.5","prazo_indeterminado":"false","lgd":"50","veiculo_legal":"1222","custo_manual":"","id_volatilidade":"","prospectivo":"true","perc_bof":0,"rcp_credito":{},"loan":{"der":{"localizacao":""},"cre":{"passivo":"","localizacao":""},"combinado":{},"sint":{},"juros":{},"moeda_limite":"","instrumento":"","contraparte":""},"rcp_derivativo_bool":"false","indexadorGarantia":"BRL","epe_constante":"true"}
    tela['id_grupo'] = str(data_json['id_grupo'])
    tela['ratingGrupo'] = str(data_json['ratingGrupo'])
    tela['ratingOper'] = str(data_json['ratingOper'])

    if(data_json['id_volatilidade'] == 'BD'):
        id_grupo = tela['id_grupo']
        query = f"SELECT id_volatilidade FROM parametros_estrategia_portfolio WHERE id_grupo = {id_grupo}"
        tela['id_volatilidade'] = (pd.read_sql(query, connection)).values[0][0]
    else:
        tela['id_volatilidade'] = data_json['id_volatilidade']
    
    tela['cnpj_cabeca'] = str(data_json['cnpj_cabeca'])
    tela['id_segmento_risco_grupo'] = '27'
    tela['id_segmento_risco_cabeca'] = '27'
    tela['cd_quebra'] = cursor.execute(f"SELECT cd_quebra FROM parametros_estrategia_portfolio WHERE id_grupo = {tela['id_grupo']}")
    
    #tela['delta_lgd'] = data_json['delta_lgd']
    #tela['delta_prazo'] = data_json["delta_prazo"]

    #tela['estoque'] = data_json['estoque']

    meta='{}'

    saida = calcula_cluster(tela, meta, cluster)


    
    return saida

#ajuste_cluster(data='{"id_volatilidade":"BD","id_grupo":2,"ratingGrupo":"BA5","ratingOper":"BA5","cnpj_cabeca":60619202}')




