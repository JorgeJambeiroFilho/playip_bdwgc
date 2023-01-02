from __future__ import annotations

import math
import traceback
from datetime import datetime
from typing import Dict, Optional, List, Iterable, Set,  cast

import numpy
import pydantic
from bson import ObjectId
from pydantic import Field

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco, buildFullImportName, address_level_fields
from playipappcommons.infra.infraimportmethods import findAddress
from playipappcommons.infra.inframethods import getInfraElementFullAddressName, getInfraElementFullStructuralName, \
    getInfraElementAddressHier, getInfraElementStructuralHier, expandDescendants
from playipappcommons.infra.inframodels import InfraElement
from playipappcommons.playipchatmongo import getBotMongoDB
from playipappcommons.util.LRUCache import LRUCache
from playipappcommons.analytics.contractsanalyticsmodels import *



productLevels = 4
eventLevels = 2

dateLimits = None

async def getDateLimits():
    global dateLimits
    if dateLimits is None:
        mdb = getBotMongoDB()
        cursor = mdb.ISPContextMetrics.aggregate\
            (
                [
                    { '$match': { "eventType" : "Cancelamento/"} },
                    {'$group': { '_id': {}, 'minDT': { '$min': '$period_metric'}, 'maxDT': { '$max': '$period_metric'}}}
                ]
            )
        dll = await cursor.to_list(length=1)
        dl = dll[0]
        dlMax = list(dl["maxDT"].keys())[0]

        cursor = mdb.ISPContextMetrics.aggregate\
            (
                [
                    { '$match': { "eventType" : "Instalacao/"} },
                    {'$group': { '_id': {}, 'minDT': { '$min': '$period_metric'}, 'maxDT': { '$max': '$period_metric'}}}
                ]
            )
        dll = await cursor.to_list(length=1)
        dl = dll[0]
        dlMin = list(dl["minDT"].keys())[0]


        dateLimits = [dlMin, dlMax]
    return dateLimits


def dateToPeriods(moment:float) -> Dict[str, str]:

    dt: datetime = datetime.fromtimestamp(moment)
    res: Dict[str, str] = {} # period_group, period

    #res["SEMANA"] = str(dt.year) + "/" + str(dt.month).zfill(2) + "/S" +   str(min(4, (dt.day-1) // 7) + 1)
    #res["QUINZENA"] = str(dt.year) + "/" + str(dt.month).zfill(2) + "/Q" + str(min(4, (dt.day-1) // 15) + 1)
    res["MES"] = str(dt.year)+"/"+str(dt.month).zfill(2)
    #res["TRIMESTRE"] = str(dt.year) + "/T" + str((dt.month-1) // 4 + 1)
    #res["SEMESTRE"] = str(dt.year) + "/S" + str((dt.month-1) // 6 + 1)
    #res["ANO"] = str(dt.year)

    return res



class LRUCacheAnalytics(LRUCache):

    def __init__(self, mdb, table, res:ImportAnalyticDataResult, *args, **kargs):
        super().__init__(*args, **kargs)
        self.res = res
        self.mdb = mdb

        self.table = table

    def registerHit(self):
        self.res.num_cache_hits += 1

    async def load(self, key):
        #icm = None
        icm = await self.mdb[self.table].find_one \
           (
                {
                    "infraElementId": key[0],
                    "infraElementOptic": key[1],
                    "fullProductName": key[2],
                    "eventType": key[3],
                    "metricName": key[4],
                    "period_group": key[5]
                }
           )
        if icm is None:
            icm = ISPContextMetrics \
                 (
                    infraElementId=key[0],
                    infraElementOptic=key[1],
                    fullProductName=key[2],
                    eventType=key[3],
                    metricName=key[4],
                    period_group=key[5],
                    period_metric={}
                )
            self.res.num_creations += 1
            return icm
        else:
            return ISPContextMetrics(**icm)

    async def save(self, key, obj):
        icm = cast(ISPContextMetrics, obj)
        icmDict = icm.dict(by_alias=True)
        self.res.num_updates += 1
        await self.mdb[self.table].replace_one({"_id": icm.id}, icmDict, upsert=True)
        print(self.res)

    async def getByIV(self, iv: ISPEvent) -> ISPContextMetrics:
        key = (iv.infraElementId, iv.infraElementOptic, iv.fullProductName, iv.eventType, iv.metricName, iv.period_group)
        return await self.get(key)

async def count_event(iv: ISPEvent, cache: LRUCacheAnalytics, unlearn: bool):
    icm:ISPContextMetrics = await cache.getByIV(iv)
    icm.count(iv, unlearn)


async def count_events(idv: ISPDateEvent, cache: LRUCacheAnalytics, unlearn: bool):
    #print("Enter count event")
    periods: Dict[str, str] = dateToPeriods(idv.dt)
    for period_group, period in periods.items():
        iv: ISPEvent = ISPEvent\
            (
                infraElementId=idv.infraElementId,
                infraElementOptic=idv.infraElementOptic,
                fullProductName=idv.fullProductName,
                period_group=period_group,
                period=period,
                eventType=idv.eventType,
                metricName=idv.metricName,
                metricValue=idv.metricValue
            )
        await count_event(iv, cache, unlearn)
    #print("Exit count event")

class SemDataDEInicioException(Exception):
    pass

class SemDownloadException(Exception):
    pass

async def count_events_contract(cdata: ContractAnalyticData, cache: LRUCacheAnalytics, unlearn: bool):
    for endIndex in range(len(cdata.enderecos)):
        endereco = cdata.enderecos[endIndex]
        if endereco.uf is None or endereco.cidade is None or endereco.bairro is None or endereco.logradouro is None:
            continue
        await count_events_contract_endfixed(cdata, endIndex, cache, unlearn)

def lengthOfCommonPrefix(lis1,lis2):
    p = 0
    while (p < len(lis1) and p<len(lis2) and lis1[p]==lis2[p]):
            p += 1
    return p

async def count_events_contract_endfixed(cdata: ContractAnalyticData, endIndex:int, cache: LRUCacheAnalytics, unlearn:bool):

    try:
        # idc = int(cdata.id_contract)
        # #if idc != 10740 and idc !=16183 and idc != 16184: # debug por número de contrato
        # #    return
        # if cdata.enderecos[endIndex].bairro == "Vila Santa Rita":
        #     print("count_events_contract_endfixed ", cdata.enderecos[endIndex])

        element: Optional[InfraElement] = await findAddress(cache.mdb, cdata.enderecos[endIndex])
        if element is None:
            element: Optional[InfraElement] = await findAddress(cache.mdb, cdata.enderecos[endIndex])
            cache.res.num_enderecos_nao_reconhecidos += 1
            return

        contexts: List[(str, str)] = []

        # addressHier = await getInfraElementAddressHier(element.id)
        # addressHier = addressHier[0:1] + addressHier[2:-2] # elimina mídia (cabo / radio) e elementos menores
        # addressHier = [str(x) for x in addressHier]
        # for i in range(len(addressHier)):
        #     ids = str(addressHier[i]) #"/".join(addressHier[:i])
        #     contexts.append((ids,"address"))

        structuralHier = await getInfraElementStructuralHier(element.id)
        structuralHier = structuralHier[:-2] # elimina elementos menores
        structuralHier = [str(x) for x in structuralHier]
        for i in range(len(structuralHier)):
            ids = str(structuralHier[i]) #"/".join(structuralHier[:i])
            contexts.append((ids,"struct"))

        idvs: List[ISPDateEvent] = []

        for context in contexts:
            #print("context", context)
            # if context[0] != "624087c95ceb4d10a5135040":
            #     continue

            if cdata.DT_INICIO:
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="Inicio/", metricName="Contagem", metricValue=1, dt=cdata.DT_INICIO
                )
                idvs.append(idv)
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratosValidos/", metricName="Contagem", metricValue=1, dt=cdata.DT_INICIO
                )
                idvs.append(idv)
            else:
                raise SemDataDEInicioException()

            if cdata.DT_FIM:
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="Fim/", metricName="Contagem", metricValue=1, dt=cdata.DT_FIM
                )
                idvs.append(idv)
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratosValidos/", metricName="Contagem", metricValue=-1, dt=cdata.DT_FIM
                )
                idvs.append(idv)

            if cdata.DT_ATIVACAO:
                if cdata.STATUS_CONTRATO!="Cancelamento" or cdata.DT_CANCELAMENTO:
                    idv: ISPDateEvent = ISPDateEvent\
                    (
                        infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                        eventType="Instalacao/", metricName="Contagem", metricValue=1, dt=cdata.DT_ATIVACAO
                    )
                    idvs.append(idv)
                    idv: ISPDateEvent = ISPDateEvent\
                    (
                        infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                        eventType="ContratosAtivos/", metricName="Contagem", metricValue=1, dt=cdata.DT_ATIVACAO
                    )
                    idvs.append(idv)

                elif not cdata.DT_CANCELAMENTO:
                    idv: ISPDateEvent = ISPDateEvent\
                    (
                        infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                        eventType="ContratoCanceladoSemData/", metricName="Contagem", metricValue=1, dt=cdata.DT_INICIO
                    )
                    idvs.append(idv)

            else:
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratoSemInstalacao/", metricName="Contagem", metricValue=1, dt=cdata.DT_INICIO
                )
                idvs.append(idv)



            if cdata.DT_CANCELAMENTO:
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="Cancelamento/", metricName="Contagem", metricValue=1, dt=cdata.DT_CANCELAMENTO
                )
                idvs.append(idv)
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratosAtivos/", metricName="Contagem", metricValue=-1, dt=cdata.DT_CANCELAMENTO
                )
                idvs.append(idv)

        if cdata.DT_ATIVACAO and (cdata.STATUS_CONTRATO!="Cancelamento" or cdata.DT_CANCELAMENTO):

            if not cdata.services:
                print("Contrato sem serviços ",cdata.id_contract)
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratoSemServicos/", metricName="Contagem", metricValue=1, dt=cdata.DT_ATIVACAO
                )
                idvs.append(idv)
            elif cdata.services[-1].DT_DESATIVACAO and not cdata.DT_CANCELAMENTO:
                print("Contrato sem cancelamento sem serviços ativos", cdata.id_contract, cdata.STATUS_CONTRATO)
                idv: ISPDateEvent = ISPDateEvent\
                (
                    infraElementId=context[0], infraElementOptic=context[1], fullProductName="Contrato/",
                    eventType="ContratoSemServicos/", metricName="Contagem", metricValue=1, dt=cdata.services[-1].DT_DESATIVACAO
                )
                idvs.append(idv)




            counted_service = False
            for i_sp in range(len(cdata.services)):
                try:
                    sidvs: List[ISPDateEvent] = []
                    sp: ServicePackAnalyticData = cdata.services[i_sp]
                    fullPackNameList = sp.fullName.split("/")
                    priorSP: ServicePackAnalyticData = None
                    priorFullPackNameList = []
                    lenCommonProdNameWithPrior = 0
                    if i_sp > 0:
                        priorSP: ServicePackAnalyticData = cdata.services[i_sp-1]
                        priorFullPackNameList = priorSP.fullName.split("/")
                        lenCommonProdNameWithPrior = lengthOfCommonPrefix(priorFullPackNameList, fullPackNameList)

                    postSP: ServicePackAnalyticData = None
                    postFullPackNameList = []
                    lenCommonProdNameWithPost = 0
                    if i_sp < len(cdata.services)-1:
                        postSP: ServicePackAnalyticData = cdata.services[i_sp+1]
                        postFullPackNameList = postSP.fullName.split("/")
                        lenCommonProdNameWithPost = lengthOfCommonPrefix(postFullPackNameList, fullPackNameList)

                    for context in contexts:
                        for pnivel in range(len(fullPackNameList)):
                            sl = "/".join((fullPackNameList[:pnivel + 1]))+"/"
                            # if context[0] != "624087c95ceb4d10a5135040" or sl != "internet/":
                            #     continue
                            #scontext = context + "#" + sl
                            #print("scontext", scontext)
                            if sp.DT_ATIVACAO:
                                if i_sp == 0:
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="ContratatacaoPacote/", metricName="Contagem",metricValue=1, dt=sp.DT_ATIVACAO)
                                    sidvs.append(idv)
                                else:
                                    if lenCommonProdNameWithPrior <= pnivel:
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="MigracaoEntradaPacote/", metricName="Contagem", metricValue=1, dt=sp.DT_ATIVACAO)
                                        idvs.append(idv)

                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Contagem", metricValue=1, dt=sp.DT_ATIVACAO)
                                sidvs.append(idv)
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Valor", metricValue=sp.VL_SERVICO, dt=sp.DT_ATIVACAO)
                                sidvs.append(idv)

                                # if context[0] == "624087c95ceb4d10a5135040" and sl=="internet/":
                                #     await cache.mdb.debugCount.insert_one({"id_contract":cdata.id_contract, "valor": sp.VL_SERVICO, "contagem":1, "dt":sp.DT_ATIVACAO})

                                if sp.download_speed is None:
                                    sp.download_speed = 0
                                    sp.upload_speed = 0
                                    #raise SemDownloadException()
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Download", metricValue=sp.download_speed, dt=sp.DT_ATIVACAO)
                                sidvs.append(idv)
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Upload", metricValue=sp.upload_speed, dt=sp.DT_ATIVACAO)
                                sidvs.append(idv)
                            else:
                                if i_sp == 0:
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="ContratatacaoPacoteNuncaAtivado/", metricName="Contagem",metricValue=1, dt=sp.DT_CADASTRO)
                                    sidvs.append(idv)
                                else:
                                    if lenCommonProdNameWithPrior <= pnivel:
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="MigracaoEntradaPacoteNuncaAtivado/", metricName="Contagem", metricValue=1, dt=sp.DT_CADASTRO)
                                        idvs.append(idv)

                            if not sp.DT_DESATIVACAO and cdata.DT_CANCELAMENTO:# and i_sp == len(cdata.services)-1:
                                sp.DT_DESATIVACAO = cdata.DT_CANCELAMENTO;

                            if sp.DT_DESATIVACAO:
                                if i_sp == len(cdata.services)-1:
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="CancelamentoPacote/", metricName="Contagem",metricValue=1, dt=sp.DT_DESATIVACAO)
                                    sidvs.append(idv)
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="CancelamentoPacote/"+sp.TX_MOTIVO_CANCELAMENTO+"/", metricName="Contagem", metricValue=1, dt=sp.DT_DESATIVACAO)
                                    sidvs.append(idv)
                                else:
                                    if lenCommonProdNameWithPost <= pnivel:
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="MigracaoSaidaPacote/", metricName="Contagem", metricValue=1, dt=sp.DT_DESATIVACAO)
                                        sidvs.append(idv)

                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Contagem", metricValue=-1, dt=sp.DT_DESATIVACAO)
                                sidvs.append(idv)
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Valor", metricValue=-sp.VL_SERVICO, dt=sp.DT_DESATIVACAO)
                                sidvs.append(idv)
                                if sp.download_speed is None:
                                    sp.download_speed = 0
                                    sp.upload_speed = 0
                                    #raise SemDownloadException()
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Download", metricValue=-sp.download_speed, dt=sp.DT_DESATIVACAO)
                                sidvs.append(idv)
                                idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Pacote/", metricName="Upload", metricValue=-sp.upload_speed, dt=sp.DT_DESATIVACAO)
                                sidvs.append(idv)

                                # if context[0] == "624087c95ceb4d10a5135040" and sl=="internet/":
                                #     await cache.mdb.debugCount.insert_one({"id_contract":cdata.id_contract, "valor": -sp.VL_SERVICO, "contagem":-1, "dt":sp.DT_DESATIVACAO})


                            for ticket in sp.tickets:
                                if ticket.DT_ABERTURA:
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="TicketAbertura/", metricName="Contagem",metricValue=1, dt=ticket.DT_ABERTURA)
                                    sidvs.append(idv)
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Ticket/", metricName="Contagem",metricValue=1, dt=ticket.DT_ABERTURA)
                                    sidvs.append(idv)
                                    if ticket.NM_AREA_TICKET:
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="TicketAbertura/"+ticket.NM_AREA_TICKET+"/", metricName="Contagem",metricValue=1, dt=ticket.DT_ABERTURA)
                                        sidvs.append(idv)
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Ticket/"+ticket.NM_AREA_TICKET+"/", metricName="Contagem",metricValue=1, dt=ticket.DT_ABERTURA)
                                        sidvs.append(idv)

                                if ticket.DT_FECHAMENTO:
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="TicketFechamento/", metricName="Contagem",metricValue=1, dt=ticket.DT_FECHAMENTO)
                                    sidvs.append(idv)
                                    idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Ticket/", metricName="Contagem",metricValue=-1, dt=ticket.DT_FECHAMENTO)
                                    sidvs.append(idv)
                                    if ticket.NM_AREA_TICKET:
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="TicketFechamento/"+ticket.NM_AREA_TICKET+"/", metricName="Contagem",metricValue=1, dt=ticket.DT_FECHAMENTO)
                                        sidvs.append(idv)
                                        idv: ISPDateEvent = ISPDateEvent(infraElementId=context[0], infraElementOptic=context[1], fullProductName=sl, eventType="Ticket/"+ticket.NM_AREA_TICKET+"/", metricName="Contagem",metricValue=-1, dt=ticket.DT_FECHAMENTO)
                                        sidvs.append(idv)



                    idvs.extend(sidvs)

                except SemDownloadException:
                    cache.res.num_sem_download += 1
                    cache.res.num_service_fails += 1
                except:
                    traceback.print_exc()
                    print("Exceção diferente serviço")
                    cache.res.num_service_fails += 1

                cache.res.num_service_processed += 1
                counted_service = True

            if not counted_service:
                print("Serviços não contados")

        for idv in idvs:
            await count_events(idv, cache, unlearn)

    except SemDataDEInicioException as sda:
        cache.res.num_sem_data_inicio += 1
        cache.res.num_fails += 1

    except:
        traceback.print_exc()
        print("Exceção diferente contrato")
        cache.res.num_fails += 1

    cache.res.num_processed += 1

   # print(cdata)

# async def getFullAddressName(element: InfraElement):
#     fullAddressName = await getInfraElementFullAddressName(element.id)
#     fullAddressNameList = fullAddressName.split("/")  # buildFullImportName(cdata.endereco).split("/")
#     noMediaAddressNameList = fullAddressNameList[0:1] + fullAddressNameList[2:-2]
#     return "endereco/"+"/".join(noMediaAddressNameList)
#
# async def getFullStructuralName(element: InfraElement):
#     fullStructName = await getInfraElementFullStructuralName(element.id)
#     fullStructuralNameList = fullStructName.split("/")
#     return "estrutura/" + "/".join(fullStructuralNameList[:-2])



async def getAllPeriodsZeroed(period_group) -> Dict[str, float]:
    dateLimits = await getDateLimits()
    firstYear = 2011
    lastYear = datetime.now().year
    period_metric: Dict[str, float] = {}
    for y in range(firstYear, lastYear+1):
        year = str(y)
        if period_group == "SEMESTRE":
            period_metric[year + "/1"] = 0
            period_metric[year + "/2"] = 0
        elif period_group == "TRIMESTRE":
            period_metric[year + "/1"] = 0
            period_metric[year + "/2"] = 0
            period_metric[year + "/3"] = 0
            period_metric[year + "/4"] = 0
        elif period_group == "MES":
            for m in range(1, 13):
                pk = year + "/" + str(m).zfill(2)
                if pk > dateLimits[0] and pk < dateLimits[1]:
                    period_metric[pk] = 0
        elif period_group == "QUINZENA":
            for m in range(1, 13):
                period_metric[year + "/" + str(m).zfill(2) + "/1"] = 0
                period_metric[year + "/" + str(m).zfill(2) + "/2"] = 0
        elif period_group == "SEMANA":
            for m in range(1, 13):
                period_metric[year + "/" + str(m).zfill(2) + "/1"] = 0
                period_metric[year + "/" + str(m).zfill(2) + "/2"] = 0
                period_metric[year + "/" + str(m).zfill(2) + "/3"] = 0
                period_metric[year + "/" + str(m).zfill(2) + "/4"] = 0
    return period_metric





async def getContextMetricsPrimitive(query:MetricsQuery, expandableContexts: List[ExpandableFullMetricsContext]) -> ResultantMetrics:
    mdb = getBotMongoDB()
    dateLimits = await getDateLimits()
    res: ResultantMetrics = ResultantMetrics()
    res.context = query.context
    for econtext in expandableContexts:
        if econtext.context.infraElementId is not None and query.context.infraElementId is not None:
            raise Exception("Elementos de infraestrutura ou endereços definidos tanto nos contextos expansíveis quanto no contexto específico")
        if econtext.context.fullProductName is not None and query.context.fullProductName is not None:
            raise Exception("Produtos definidos tanto nos contextos expansíveis quanto no contexto específico")
        if econtext.context.eventType is not None and query.context.eventType is not None:
            raise Exception("Tipo de evento definidos tanto nos contextos expansíveis quanto no contexto específico")
        if econtext.context.metricName is not None and query.context.metricName is not None:
            raise Exception("Métrica definida tanto nos contextos expansíveis quanto no contexto específico")
        if econtext.context.period_group is not None and query.context.period_group is not None:
            raise Exception("Grupo de períodos definido tanto nos contextos expansíveis quanto no contexto específico")


        context: ExpandableFullMetricsContext = ExpandableFullMetricsContext\
        (
                context= FullMetricsContext
                (
                    infraElementId=query.context.infraElementId if econtext.context.infraElementId is None else econtext.context.infraElementId,
                    infraElementOptic=query.context.infraElementOptic if econtext.context.infraElementId is None else econtext.context.infraElementOptic,
                    fullProductName=query.context.fullProductName if econtext.context.fullProductName is None else econtext.context.fullProductName,
                    eventType=query.context.eventType if econtext.context.eventType is None else econtext.context.eventType,
                    metricName=query.context.metricName if econtext.context.metricName is None else econtext.context.metricName,
                    period_group=query.context.period_group if econtext.context.period_group is None else econtext.context.period_group,
                ),
                maxInfraElementDescendantsExpansion= 0 if econtext.context.infraElementId is None else econtext.maxInfraElementDescendantsExpansion,
                minInfraElementDescendantsExpansion= 0 if econtext.context.infraElementId is None else econtext.minInfraElementDescendantsExpansion,
                maxProductDescendantsExpansion=0 if econtext.context.fullProductName is None else econtext.maxProductDescendantsExpansion,
                minProductDescendantsExpansion=0 if econtext.context.fullProductName is None else econtext.minProductDescendantsExpansion,
                maxEventTypeDescendandsExpansion=0 if econtext.context.eventType is None else econtext.maxEventTypeDescendandsExpansion,
                minEventTypeDescendandsExpansion=0 if econtext.context.eventType is None else econtext.minEventTypeDescendandsExpansion,
        )

        if\
        (
                context.context.infraElementId is None or context.context.infraElementOptic is None or\
                context.context.fullProductName is None or context.context.eventType is None or\
                context.context.metricName is None or context.context.period_group is None
        ):
            continue

        context.context.infraElementFullName = await getInfraElementFullStructuralName(FAMongoId(context.context.infraElementId))

        elemChildren = await expandDescendants(mdb, context.context.infraElementId, maxLevel=context.maxInfraElementDescendantsExpansion, minLevel=context.minInfraElementDescendantsExpansion)
        for eid, ename in elemChildren:
            startLevelProduct = len(context.context.fullProductName.split("/"))
            startLevelEvent = len(context.context.eventType.split("/"))
            q = {
                    "infraElementId": eid,
                    "infraElementOptic": context.context.infraElementOptic,
                    "fullProductName": {"$regex": "^"+context.context.fullProductName+"/.*"},
                    "eventType": {"$regex": "^"+context.context.eventType+"/.*"},
                    "metricName": context.context.metricName,
                    "period_group": context.context.period_group
                }

            cursor = mdb.ISPContextMetrics.find(q)
            cm: Optional[ISPContextMetrics] = None
            async for dcm in cursor:
                cm: ISPContextMetrics = ISPContextMetrics(**dcm)
                cmFullProductNameLis = cm.fullProductName.split("/")[:-1]
                cmEventTypeLis = cm.eventType.split("/")[:-1]
                lcmFullProductNameLis = len(cmFullProductNameLis)
                lcmEventTypeLis = len(cmEventTypeLis)

                if lcmFullProductNameLis >= startLevelProduct + context.minProductDescendantsExpansion and\
                   lcmFullProductNameLis <= startLevelProduct + context.maxProductDescendantsExpansion and \
                   lcmEventTypeLis >= startLevelEvent + context.minEventTypeDescendandsExpansion and \
                   lcmEventTypeLis <= startLevelEvent + context.maxEventTypeDescendandsExpansion:

                    periods_values:Dict[str, float] = await getAllPeriodsZeroed(context.context.period_group)
                    for period, value in cm.period_metric.items():
                        if period in periods_values: #excluo os periodos incompletos que existem nos extremos
                            periods_values[period] = value

                    periods_values_ord: List[Tuple[str, float]] = list(periods_values.items())
                    periods_values_ord.sort()

                    if res.periods is None:
                        res.periods = [pv[0] for pv in periods_values_ord]



                    ccontext: FullMetricsContext = FullMetricsContext\
                    (
                        infraElementName=ename,
                        infraElementId=cm.infraElementId if econtext.context.infraElementId is not None else None,
                        infraElementOptic=cm.infraElementOptic if econtext.context.infraElementId is not None else None,
                        fullProductName=cm.fullProductName if econtext.context.fullProductName is not None else None,
                        eventType=cm.eventType if econtext.context.eventType is not None else None,
                        metricName=cm.metricName if econtext.context.metricName is not None else None,
                        period_group=cm.period_group if econtext.context.period_group is not None else None
                    )
                    if ccontext.infraElementId is not None:
                        ccontext.infraElementFullName = await getInfraElementFullStructuralName(FAMongoId(ccontext.infraElementId))

                    if ccontext in res.series:
                        raise Exception("Contexto duplicado")
                    res.series[ccontext] = [pv[1] for pv in periods_values_ord]

    return res

def fixNaNs(arr):
    last = -1
    for i in range(len(arr)):
        if not math.isnan(arr[i]) and not math.isinf(arr[i]):
            if last != -1:
                for j in range(last+1,i):
                    if math.isinf(arr[last]) and math.isinf(arr[i]) and arr[i]!=arr[last]:
                        if j-last == i-j:
                            arr[j] = 0
                        elif j-last < i-j:
                            arr[j] = arr[last]
                        else:
                            arr[j] = arr[i]
                    else:
                        arr[j] = (j-last) / (i-last) * arr[last] + (1 - (j-last) / (i-last)) * arr[i]
            else:
                for j in range(last + 1, i):
                    arr[j] = arr[i]
            last = i
    for j in range(last + 1, len(arr)):
        arr[j] = arr[last] if last != -1 else 0


def operateUnary(operator:str, left:List[float]):
    larr = numpy.array(left)

    if operator == "neg":
        res = - larr
    elif operator == "accumulate":
            res = numpy.cumsum(larr)
    elif operator == "movingaverage":
            w = 2
            res = numpy.convolve(larr, numpy.ones(2*w+1), 'valid') / 5
            beg = []
            end = []
            for t in range(w):
                bv = 0
                ev = 0
                for tt in range(t+w+1):
                    bv += larr[tt]
                    ev += larr[-tt-1]
                bv /= t+w+1
                ev /= t+w+1
                beg.append(bv)
                end.insert(0, ev)
            begarr = numpy.array(beg)
            endarr = numpy.array(end)

            res = numpy.concatenate([begarr, res, endarr])
    else:
        raise Exception("Operador desconhecido "+operator)

    lres = res.tolist()
    return lres



def operate(operator:str, left:List[float], right:list[float]):
    larr = numpy.array(left)
    rarr = numpy.array(right)
    ll= len(left)
    lr = len(right)
    if ll < lr:
        if lr % ll != 0:
            raise Exception("Listas de tamanhos incompatíveis")
        m = lr / ll
        larr = numpy.repeat(larr, m)
    if lr < ll:
        if ll % lr != 0:
            raise Exception("Listas de tamanhos incompatíveis")
        m = ll / lr
        rarr = numpy.repeat(rarr, m)

    if operator == "/":
        res = larr / rarr
        lres = res.tolist()
        fixNaNs(lres)
        return lres
    elif operator == "*":
        res = larr * rarr
        lres = res.tolist()
        return lres
    elif operator == "+":
        res = larr + rarr
        lres = res.tolist()
        return lres
    elif operator == "-":
        res = larr - rarr
        lres = res.tolist()
        return lres
    else:
        raise Exception("Operador desconhecido "+operator)


async def getContextMetricsBinary(query:MetricsQuery, expandableContexts: List[ExpandableFullMetricsContext]) -> ResultantMetrics:
    left: ResultantMetrics = await getContextMetrics(query.left, expandableContexts)
    right: ResultantMetrics = await getContextMetrics(query.right, expandableContexts)
    res: ResultantMetrics = ResultantMetrics()
    if len(left.periods) > len(right.periods):
        res.periods = left.periods
    else:
        res.periods = right.periods

    contexts = set()
    contexts.update(left.series.keys())
    contexts.update(right.series.keys())
    for context in contexts:
        if right.constant is not None:
            rseries = [right.constant]
        else:
            if context not in right.series:
                period_group: str = context.period_group if context.period_group is not None else right.context.period_group
                rseries = list(await getAllPeriodsZeroed(period_group).values())
            else:
                rseries = right.series[context]

        if left.constant is not None:
            lseries = [left.constant]
        else:
            if context not in left.series:
                    period_group: str = context.period_group if context.period_group is not None else left.context.period_group
                    lseries = list(await getAllPeriodsZeroed(period_group).values())
            else:
                lseries = left.series[context]

        series = operate(query.operator, lseries, rseries)
        res.series[context] = series

    return res

def isBinary(op:str):
    return op=="/" or op=="*" or op=="+" or op=="-"

async def getContextMetricsUnary(query:MetricsQuery, expandableContexts: List[ExpandableFullMetricsContext]) -> ResultantMetrics:
    left: ResultantMetrics = await getContextMetrics(query.left, expandableContexts)
    res: ResultantMetrics = ResultantMetrics()
    res.periods = left.periods
    contexts = set()
    contexts.update(left.series.keys())
    for context in contexts:
        lseries = left.series[context]
        series = operateUnary(query.operator, lseries)
        res.series[context] = series

    return res


async def getContextMetrics(query:MetricsQuery, expandableContexts: List[ExpandableFullMetricsContext]=[]) -> ResultantMetrics:
    if query.operator == "constant":
         return ResultantMetrics(constant=query.constant)
    elif query.operator == "primitive":
        return await getContextMetricsPrimitive(query, expandableContexts)
    elif isBinary(query.operator):
        return await getContextMetricsBinary(query, expandableContexts)
    else:
        return await getContextMetricsUnary(query, expandableContexts)


async def getContextMetricsExpandable(equery:ExpandableMetricsQuery) -> ResultantMetricsFlat:
    try:
        r = await getContextMetrics(equery.query, equery.expandableContexts)
        res: ResultantMetricsFlat = ResultantMetricsFlat(queryKey=equery.queryKey, rm=r)
        return res
    except Exception as ex:
        traceback.print_exc()
        return ResultantMetricsFlat(queryKey=equery.queryKey, fail=True, message=str(ex))


async def getAnalyticsReportList() -> List[AnalyticsReportSpecification]:
    mdb = getBotMongoDB()
    res: List[AnalyticsReportSpecification] = []
    cursor = mdb.AnalyticsReports.find({}, {"_id": True, "name": True})
    async for row in cursor:
        rs = AnalyticsReportSpecification(reportId=str(row["_id"]), reportName=row["name"])
        res.append(rs)

    return res

async def getAnalyticsReport(id:str) -> AnalyticsReport:
    mdb = getBotMongoDB()
    dres = await mdb.AnalyticsReports.find_one({"_id":ObjectId(id)})
    res = AnalyticsReport(**dres)
    return res

async def setAnalyticsReport(report: AnalyticsReport) -> AnalyticsReportSetResult:
    mdb = getBotMongoDB()
    rdic = report.dict(by_alias=True)
    await mdb.AnalyticsReports.replace_one({"_id": report.id}, rdic, upsert=True)
    return AnalyticsReportSetResult(fail=False, message="Ok")

async def createAnalyticsReport() -> AnalyticsReport:
    mdb = getBotMongoDB()
    report: AnalyticsReport = AnalyticsReport()
    rdic = report.dict(by_alias=True)
    await mdb.AnalyticsReports.insert_one(rdic)
    return report

async def getFullProductNames() -> List[str]:
    mdb = getBotMongoDB()
    res: List[str] = await mdb.ISPContextMetrics.distinct("fullProductName", {})
    res = [prod[:-1] for prod in res]
    return res

async def getEventTypes() -> List[str]:
    mdb = getBotMongoDB()
    res: List[str] = await mdb.ISPContextMetrics.distinct("eventType", {})
    res = [event[:-1] for event in res]
    return res

async def getPeriodGroups() -> List[str]:
    mdb = getBotMongoDB()
    res: List[str] = await mdb.ISPContextMetrics.distinct("period_group", {})
    return res

async def getMetricNames() -> List[str]:
    mdb = getBotMongoDB()
    res: List[str] = await mdb.ISPContextMetrics.distinct("metricName", {})
    return res

# async def getImportAnalyticDataResult(begin:bool) -> ImportAnalyticDataResult:
#     mdb = getBotMongoDB()
#     resDict = await mdb.control.find_one({"key":"ImportAnalyticDataResult"})
#     if resDict is None:
#         res = ImportAnalyticDataResult()
#         res.complete = True
#     else:
#         res: ImportAnalyticDataResult = ImportAnalyticDataResult(**resDict)
#     if res.complete and begin:
#         res = ImportAnalyticDataResult()
#         resDict = res.dict(by_alias=True)
#         resDict["key"] = "ImportAnalyticDataResult"
#         await mdb.control.replace_one({"key": "ImportAnalyticDataResult"}, resDict, upsert=True)
#
#     return res

# async def setImportAnalyticDataResult(adr: ImportAnalyticDataResult):
#     mdb = getBotMongoDB()
#     resDict = adr.dict(by_alias=True)
#     resDict["key"] = "ImportAnalyticDataResult"
#     await mdb.control.replace_one({"key": "ImportAnalyticDataResult"}, resDict, upsert=True)

