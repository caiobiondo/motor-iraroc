from flask import Flask, jsonify
from flask import request
from flask import Response
from pyHEC import PyHEC
###IMPORT MODULO CORS PARA TRATAMENTO DE ERRO NO HUB###
#from flask_cors import CORS, cross_origin
import json
import ast
import logging
from urllib.parse import urlparse
from urllib.parse import parse_qs
from Objetos.Tela import Tela
from Calculo.Calcular_raroc import *
from Calculo.Monta_fluxo import *
from Calculo.Calcular_cluster import *
from Cluster.Ajuste_cluster import*
from Cluster.Puxa_estoque import *
from ToolKitDB.FPR import *
from ToolKitDB.Rating import *
from ToolKitDB.RCP import *
from Abas.Garantias import *
from Abas.Aba_operacao import *
from Abas.Aba_risco import *
from ToolKitDB.Mitigacao_kreg import *
app = Flask(__name__)


logging.basicConfig(filename='record.log', level=logging.DEBUG,
                    format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')


hec = PyHEC("4a688801-9005-4296-ae0d-1ce56de4bd2c", "http://localhost")
event = {"/calc_raroc_py/calculaRaroc_planilha": "observabilitty-raroc", "POST":"motorIraroc-v1"}
hec.send(event)
hec.send(logging.DEBUG)
hec.send(get_lista_fpr())


@app.route("/calc_raroc_py/calculaRaroc_planilha", methods=["POST"])
def json_handler_planilha():
    tela = request.form.to_dict()["tela"]
    tela_json = json.loads(tela)
    meta = '{}'
    resultado = calcula_raroc(tela_json, meta)
    return str(resultado)


@app.route("/calc_raroc_py/calculaFluxo_planilha", methods=["POST"])
def monta_fluxo_planilha():
    data = request.data
    data_json = json.loads(data)
    resultado = str(monta_json(data_json))
    return resultado

@app.route("/calc_raroc_py/calculaMeta_planilha", methods=["POST"])
def meta_planilha():
    tela = request.form.to_dict()['tela']
    tela_json = json.loads(tela)
    meta = request.form.to_dict()['meta']
    resultado = calcula_raroc(tela_json, meta)
    return str(resultado)

@app.route("/calc_raroc_py/calculaRaroc", methods=["POST"])
def json_handler():
    tela_json = request.get_json()
    meta = '{}'
    resultado = calcula_raroc(tela_json, meta)
    return jsonify(resultado)

@app.route("/calc_raroc_py/calculaMeta", methods=["POST"])
def meta():
    request_json = request.get_json()
    tela_json = request_json["tela"]
    meta = (request_json["meta"])
    meta_json = json.dumps(meta)
    resultado = calcula_raroc(tela_json, meta_json)
    return jsonify(resultado)

@app.route("/calc_raroc_py/calculaFluxo", methods=["POST"])
def monta_fluxo():
    data_json = request.get_json()
    resultado = (monta_json(data_json))
    return Response(json.dumps(resultado),  mimetype='application/json') 

#ABA RISCO
@app.route("/calc_raroc_py/fpr", methods=["GET"])
def get_fpr():
    app.logger.info('Info level log')
    app.logger.warning('Warning level log')
    url = request.full_path
    parsed_url = urlparse(url)
    if(not(parsed_url[4] == '')): 
        valor_cnpj = int(parse_qs(parsed_url.query)['cnpj'][0])
        resposta = get_fpr_pre_pos(valor_cnpj)
        return jsonify(resposta)
    else:
        lista_fpr = get_lista_fpr()
        return jsonify(lista_fpr)

@app.route("/calc_raroc_py/rating", methods=["GET"])
def get_rating():
    url = request.full_path
    params = request.args.to_dict()
    segmento = params["segmento"]
    
    if(segmento == "MIDDLE"):
        id_puro = int(params["id"])
        id_grupo = str(abs(id_puro))
        resposta = get_rating_middle_cnpj(id_grupo)
    else:
        id_grupo = int(params["id"])
        resposta = get_rating_cnpj(id_grupo)
    
    return jsonify(resposta)

@app.route("/calc_raroc_py/risco", methods=["GET"])
def get_cnpj_grupo():
    url = request.full_path
    params = request.args.to_dict()
    id_grupo = params["id_grupo"]
    aba_risco = get_infos_risco(id_grupo)
    return aba_risco

@app.route("/calc_raroc_py/volatilidade", methods=["GET"])
def get_volatilidade():
    url = request.full_path
    params = request.args.to_dict()
    id_grupo = params["id_grupo"]
    volatilidade = get_parametros_volatilidade(id_grupo)
    return volatilidade

#ABA GARANTIAS
@app.route("/calc_raroc_py/garantias_cea/instrumento", methods=["GET"])
def get_instrumento_garantia():
    params = request.args.to_dict() 
    resposta = get_instrumento_garantias(params["id_segmento"])
    return jsonify(resposta)

@app.route("/calc_raroc_py/garantias_cea/bem", methods=["GET"])
def get_bem():
    params = request.args.to_dict()
    resposta = get_bem_garantias(params["id_segmento"], params["instrumento"])
    return jsonify(resposta)

@app.route("/calc_raroc_py/garantias_cea/bem2", methods=["GET"])
def get_bem_2():
    return {"id": 1, "value": "Texto"}

@app.route("/calc_raroc_py/garantias_cea/natureza", methods=["GET"])
def get_natureza():
    params = request.args.to_dict()
    resposta = get_natureza_garantias(params["id_segmento"], params["instrumento"], params["bem"])
    return jsonify(resposta)

@app.route("/calc_raroc_py/garantias_cea/parametros", methods=["GET"])
def get_parametros():
    params = request.args.to_dict()
    resposta = get_parametros_garantias(params["id_segmento"], params["instrumento"], params["bem"], params["natureza"])
    return (resposta)

@app.route("/calc_raroc_py/garantias_cea/mitigacao", methods=["POST"])
def mitigacao_lgd():
    pacote_garantia = request.get_json()
    mitigacao = mitigacao_garantias(pacote_garantia["valor_operacao"], pacote_garantia["valor_garantia"], pacote_garantia["moeda_garantia"], pacote_garantia["moeda_operacao"], pacote_garantia["hv"], pacote_garantia["he"], pacote_garantia["lmm"], pacote_garantia["prazo_operacao"])
    return mitigacao

@app.route("/calc_raroc_py/garantias_cea/mitigacao_kreg", methods=["POST"])
def mitigar_kreg():
    json_info = request.get_json()
    mitigacao_kreg = mitiga_kreg_auto(json_info)
    return mitigacao_kreg

#Endpoints Aba de Operacao
@app.route("/calc_raroc_py/operacao/lista_produtos", methods=["GET"])
def lista_produto_modalidade():
    segmento = request.args.to_dict()["segmento"]
    produtos = get_produtos_segmento(segmento)
    return produtos

@app.route("/calc_raroc_py/operacao/lista_modalidades", methods=["GET"])
def lista_modalidades():
    produto = request.args.to_dict()["produto"]
    segmento = request.args.to_dict()["segmento"]
    modalidades = get_modalidades(segmento, produto)
    return modalidades

@app.route("/calc_raroc_py/operacao/info", methods=["GET"])
def lista_infos():
    produto = request.args.to_dict()["produto"]
    modalidade = request.args.to_dict()["modalidade"]
    segmento = request.args.to_dict()["segmento"]
    info = get_infos_produto(produto, modalidade, segmento)
    return info

@app.route("/calc_raroc_py/puxaEstoque", methods=["POST"])  #POST POR CONTA DA PLANILHA EXCEL!! 
def get_estoque():
    estoque = puxa_estoque()
    return estoque #jsonify(estoque)

#Endpoints RCP
@app.route("/calc_raroc_py/rcp/alfa_beta/lista_ativo", methods=["GET"])
def get_lista_ativo_rcp():
    lista_ativos = get_lista_ativo()
    return jsonify(lista_ativos)

@app.route("/calc_raroc_py/rcp/alfa_beta/lista_passivo", methods=["GET"])
def get_lista_passivo_rcp():
    filtro = request.args.to_dict()["filtro"]
    lista_passivos = get_lista_passivo(filtro)
    return jsonify(lista_passivos)

@app.route("/calc_raroc_py/rcp/alfa_beta/perc_rcp", methods=["GET"])
def get_lista_percentual_rcp():
    lista = []
    ativo = request.args.to_dict()["ativo"]
    passivo = request.args.to_dict()["passivo"]
    prazo = float(request.args.to_dict()["prazo"])
    mitigacao = perc_rcp(ativo, passivo, prazo)
    lista.append({"vl_mit": mitigacao})
    return jsonify(lista)

#CLUSTER
@app.route("/calc_raroc_py/calculaCluster", methods=["POST"])
def cluster():
    data = request.data
    data_json = json.loads(data)
    resultado = ajuste_cluster(data_json)
    return str(resultado)

if __name__ == "__main__":
    app.run(port = 3000, debug=True, use_reloader=False)



