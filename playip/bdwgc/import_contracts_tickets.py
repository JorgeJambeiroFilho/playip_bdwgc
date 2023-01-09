import asyncio
import datetime
from typing import Iterable, AsyncGenerator, cast

from fastapi import APIRouter

from playip.bdwgc.bdwgc import getWDB
from playip.bdwgc.sql_import_analytics_tickets import sql_analytics_tickets
from playipappcommons.analytics.contractsanalytics import ContractAnalyticData, ServicePackAnalyticData, \
    ServicePackAndContractAnalyticData,  ImportContractsResult
from playipappcommons.analytics.contractsanalyticsmodels import TicketData, iadr_key
from playipappcommons.analytics.contractsimport import import_contracts_raw
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.infra.endereco import Endereco
from dateutil import tz
from dateutil.tz import tzutc


onGoingImportAnalyticDataResult: ImportContractsResult = None

tzsp = tz.tzoffset('IST', -10800)
def cf(s):
    if s is None:
        return None
    r = s.strip().replace("/","-")
    return r



class ObjRow:
    pass

async def getContratoPacoteServicoTicketIterator() -> AsyncGenerator[ServicePackAndContractAnalyticData, None]:
    wdb = await getWDB()

    global onGoingImportAnalyticDataResult
    res: ImportContractsResult = onGoingImportAnalyticDataResult

    with wdb.cursor() as cursor:
        cursor.execute(sql_analytics_tickets)

        columns = [column[0] for column in cursor.description]
        headers = columns

        rrow = cursor.fetchone()
        while rrow:

            row = ObjRow()
            for h, v in zip(headers, rrow):
                if v is None:
                    pass
                elif isinstance(v, datetime.datetime):
                    #dtv: datetime.datetime = cast(datetime.datetime)
                    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
                        # dtv.replace(tzinfo=tzsp)
                        v = datetime.datetime(year=v.year, month=v.month, day=v.day, hour=v.hour, minute=v.minute, second=v.second, microsecond=v.microsecond, tzinfo=tzsp)
                    v = v.timestamp()
                elif isinstance(v, datetime.date):
                    v = datetime.datetime(v.year, v.month, v.day, tzinfo=tzsp)
                    v = v.timestamp()
                elif isinstance(v, str):
                    v = cf(v)
                row.__setattr__(h, v)

            is_radio = row.NM_MEIO == "radio"  # a outra opção no wgc é "fibra"
            medianetwork = "Rádio" if is_radio else "Cabo"
            enderecoInfra: Endereco = Endereco\
            (
                    logradouro=row.logradouro, numero=row.num, complemento=row.complemento, bairro=row.bairro, cep=row.cep,
                    condominio=row.condominio, cidade=row.cidade, uf=row.id_uf, prefix="Infraestrutura-"+medianetwork
            )
            enderecoComercial: Endereco = Endereco\
            (
                    logradouro=row.logradouro, numero=row.num, complemento=row.complemento, bairro=row.bairro, cep=row.cep,
                    condominio=row.condominio, cidade=row.cidade, uf=row.id_uf, prefix="Comercial"
            )
            #for endereco in [enderecoInfra, enderecoComercial]:
            contract: ContractAnalyticData = ContractAnalyticData\
            (
                id_contract=row.ID_CONTRATO,
                DT_ATIVACAO=row.CONTRATO_DT_ATIVACAO,
                DT_CANCELAMENTO=row.CONTRATO_DT_CANCELAMENTO,
                DT_INICIO=row.CONTRATO_DT_INICIO,
                DT_FIM=row.CONTRATO_DT_FIM,
                STATUS_CONTRATO=row.STATUS_CONTRATO,
                enderecos=[enderecoComercial, enderecoInfra]
            )
            service: ServicePackAnalyticData = ServicePackAnalyticData\
            (
                fullName = row.NM_PROD + "/" + row.NM_MEIO + "/" + row.NM_TEC + "/" + row.NM_PACOTE_SERVICO, #+ "/",
                DT_ATIVACAO=row.SERVICO_DT_ATIVACAO,
                DT_DESATIVACAO=row.SERVICO_DT_DESATIVACAO,
                DT_DESISTENCIA=row.SERVICO_DT_DESISTENCIA,
                DT_CADASTRO=row.SERVICO_DT_CADASTRO,
                TX_MOTIVO_CANCELAMENTO=row.SERVICO_TX_MOTIVO_CANCELAMENTO if row.SERVICO_TX_MOTIVO_CANCELAMENTO else "Desconhecido",
                VL_SERVICO=row.VL_PACOTE, # só há um serviço, relevante,então posso jogar o preço do pacote todod nele para fins estatísticos
                download_speed=row.VL_DOWNLOAD,
                upload_speed=row.VL_UPLOAD,
                VL_PACOTE=row.VL_PACOTE
            )
            ticket: TicketData = TicketData\
            (
                DT_ABERTURA=row.DT_ticketAbertura,
                DT_FECHAMENTO=row.DT_ticketFechamento,
                NM_AREA_TICKET=row.ticketArea
            )
            spc: ServicePackAndContractAnalyticData = ServicePackAndContractAnalyticData(contract=contract, service=service, ticket=ticket)

            # try:
            #     spc: ServicePackAndContractAnalyticData = ServicePackAndContractAnalyticData(contract=contract, service=service)
            # except:
            #     res.num_fails += 1
            # res.num_processed += 1
            yield spc



            rrow = cursor.fetchone()



async def getImportContractsResultIntern(mdb, begin:bool) -> ImportContractsResult:
    return cast(ImportContractsResult, await getControlStructure(mdb, iadr_key, begin))

async def importAllContratoPacoteServicoTicket(mdb, iadr:ImportContractsResult):
    it: Iterable[ServicePackAndContractAnalyticData] = getContratoPacoteServicoTicketIterator()
    await import_contracts_raw(it, iadr)


    # onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    # if onGoingImportAnalyticDataResult.started:
    #     return
    # onGoingImportAnalyticDataResult.started = True
    # await setImportAnalyticDataResult(onGoingImportAnalyticDataResult)
    #
    # it: Iterable[ServicePackAndContractAnalyticData] = getContratoPacoteServicoTicketIterator()
    # await import_contracts_raw(it, onGoingImportAnalyticDataResult)
    #
    # # it: AsyncGenerator[ServicePackAndContractAnalyticData, None] = getContratoPacoteServicoIterator()
    # # await count_events_contracts_raw(it)
