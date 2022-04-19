import asyncio
import datetime
from typing import Iterable, AsyncGenerator

from fastapi import APIRouter

from playip.bdwgc.bdwgc import getWDB
from playipappcommons.analytics.analytics import ContractAnalyticData, ServicePackAnalyticData, \
    ServicePackAndContractAnalyticData, count_events_contracts_raw, ImportAnalyticDataResult, \
    getImportAnalyticDataResult, setImportAnalyticDataResult
from playipappcommons.analytics.analyticsmodels import TicketData
from playipappcommons.infra.endereco import Endereco


onGoingImportAnalyticDataResult: ImportAnalyticDataResult = None

def cf(s):
    if s is None:
        return None
    r = s.strip().replace("/","-")
    return r



class ObjRow:
    pass

async def getContratoPacoteServicoTicketIterator() -> AsyncGenerator[ServicePackAndContractAnalyticData, None]:
    wdb = getWDB()

    global onGoingImportAnalyticDataResult
    res: ImportAnalyticDataResult = onGoingImportAnalyticDataResult

    with wdb.cursor() as cursor:
        cursor.execute("""
            SELECT  

                cps.ID_CONTRATO_PACOTESERVICO_SERVICO as ID_CONTRATO_PACOTESERVICO_SERVICO,
                cps.ID_PACOTE_SERVICO as ID_PACOTE_SERVICO,
                cps.DT_ATIVACAO as SERVICO_DT_ATIVACAO, 
                cps.DT_DESATIVACAO as SERVICO_DT_DESATIVACAO, 
                cps.DT_DESISTENCIA as SERVICO_DT_DESISTENCIA, 
                cps.DT_CADASTRO as SERVICO_DT_CADASTRO,
                cps.TX_MOTIVO_CANCELAMENTO as SERVICO_TX_MOTIVO_CANCELAMENTO, 
                cps.VL_SERVICO as VL_PACOTE_SERVICO,

                contrato.ID_CONTRATO as ID_CONTRATO,
                contrato.DT_INICIO as CONTRATO_DT_INICIO,
                contrato.DT_FIM as CONTRATO_DT_FIM,
                contrato.DT_ATIVACAO as CONTRATO_DT_ATIVACAO,
                contrato.DT_CANCELAMENTO as CONTRATO_DT_CANCELAMENTO,

                s.VL_DOWNLOAD as VL_DOWNLOAD, 
                s.VL_UPLOAD as VL_UPLOAD, 
                s.VL_SERVICO as VL_SERVICO,
                ser.NM_SERVICO as NM_SERVICO,
                ser.VL_REFERENCIA as VL_REFERENCIA_SERVICO,

                dici.ID_TIPO_MEIO_ACESSO as ID_TIPO_MEIO_ACESSO, 
                dici.ID_TIPO_TECNOLOGIA as ID_TIPO_TECNOLOGIA, 
                dici.ID_TIPO_PRODUTO as ID_TIPO_PRODUTO, 
                tmeio.TX_DESCRICAO_TIPO as NM_MEIO, 
                ttec.TX_DESCRICAO_TIPO as NM_TEC, 
                tprod.TX_DESCRICAO_TIPO as NM_PROD, 

                ps.NM_PACOTE_SERVICO as NM_PACOTE_SERVICO, 
                ps.VL_PACOTE as VL_PACOTE,
                
                Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf,                

                Ticket.DT_ABERTURA as DT_ticketAbertura,
                Ticket.DT_FECHAMENTO as DT_ticketFechamento,
                AreaTicket.NM_AREA_TICKET as ticketArea


            FROM
                         
                Contrato_PacoteServico_Servico as cps 
                INNER JOIN Contrato as contrato on (cps.ID_CONTRATO=contrato.ID_CONTRATO)
                INNER JOIN PacoteServico as ps on (cps.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO)
                INNER JOIN Servico as ser on (ser.ID_SERVICO=s.ID_SERVICO)  
                INNER JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                INNER JOIN TiposDiversos as ttec on (ttec.ID_TIPO_DIVERSOS=dici.ID_TIPO_TECNOLOGIA) 
                INNER JOIN TiposDiversos as tprod on (tprod.ID_TIPO_DIVERSOS=dici.ID_TIPO_PRODUTO) 

                INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)

                INNER JOIN Ticket as Ticket on 
                          (
                              cps.ID_CONTRATO=Ticket.ID_CONTRATO and 
                              cps.DT_ATIVACAO<=Ticket.DT_ABERTURA and
                              (
                                  cps.DT_DESATIVACAO is null or
                                  cps.DT_DESATIVACAO > Ticket.DT_ABERTURA
                              )
                          )    
                INNER JOIN AreaTicket as AreaTicket on (Ticket.ID_AREA_TICKET=AreaTicket.ID_AREA_TICKET)


            WHERE
                tprod.TX_DESCRICAO_TIPO = 'internet'  and ser.NM_SERVICO like '%SCM'
            ORDER BY
                UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO,
                SERVICO_DT_ATIVACAO, ID_CONTRATO_PACOTESERVICO_SERVICO, Ticket.DT_ABERTURA
                         """)

        columns = [column[0] for column in cursor.description]
        headers = columns

        rrow = cursor.fetchone()
        while rrow:

            row = ObjRow()
            for h, v in zip(headers, rrow):
                if v is None:
                    pass
                elif isinstance(v, datetime.datetime):
                    v = v.timestamp()
                elif isinstance(v, datetime.date):
                    v = datetime.datetime(v.year, v.month, v.day)
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


async def importAllContratoPacoteServicoTicket():
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if onGoingImportAnalyticDataResult.started:
        return
    onGoingImportAnalyticDataResult.started = True
    await setImportAnalyticDataResult(onGoingImportAnalyticDataResult)

    it: Iterable[ServicePackAndContractAnalyticData] = getContratoPacoteServicoTicketIterator()
    await count_events_contracts_raw(it, onGoingImportAnalyticDataResult)

    # it: AsyncGenerator[ServicePackAndContractAnalyticData, None] = getContratoPacoteServicoIterator()
    # await count_events_contracts_raw(it)
