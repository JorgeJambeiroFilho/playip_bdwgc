import asyncio
import time
import uuid
from typing import Optional

from fastapi import APIRouter

from playip.bdwgc.bdwgc import getWDB
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ImportAddressResult, importOrFindAddress
from playipappcommons.playipchatmongo import getBotMongoDB

importrouter = APIRouter(prefix="/playipispbd/import")

def cf(s):
    if s is None:
        return None
    r = s.strip()#.strip("'").strip()
    return r


#curl -X 'GET' 'http://app.playip.com.br/playipispbd/import/importaddresses/wgc/Cotia' -H 'accept: application/json' -H 'access-token: djd62o$w*N<H$k8'
#curl -X 'GET' 'http://app.playip.com.br/playipchathelper/infra/clear' -H 'accept: application/json'

onGoingImportAddressResult: ImportAddressResult = None

@importrouter.get("/importaddresses", response_model=ImportAddressResult)
async def importAddresses() -> ImportAddressResult:
    global onGoingImportAddressResult
    if onGoingImportAddressResult is None or onGoingImportAddressResult.complete:
        onGoingImportAddressResult = ImportAddressResult()
        asyncio.create_task(importAddressesIntern())
    return onGoingImportAddressResult

@importrouter.get("/getimportaddressesresult", response_model=ImportAddressResult)
async def getImportAddressesResult() -> ImportAddressResult:
    global onGoingImportAddressResult
    if onGoingImportAddressResult is None:
        onGoingImportAddressResult = ImportAddressResult()
        onGoingImportAddressResult.complete = True
    return onGoingImportAddressResult


async def importAddressesIntern() -> ImportAddressResult:
    mdb = getBotMongoDB()
    wdb = getWDB()
    global onGoingImportAddressResult
    res: ImportAddressResult = onGoingImportAddressResult
    importExecUID: str = str(uuid.uuid1())
    time_ini = time.time()

    with wdb.cursor() as cursor:
        cursor.execute("""
            SELECT  
                    Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                    Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                    Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf, tmeio.TX_DESCRICAO_TIPO        
            FROM 
                    ContratoItem as ci 
                    INNER JOIN Contrato as Contrato on (ci.ID_CONTRATO = Contrato.ID_CONTRATO)
                    INNER JOIN PacoteServico as ps on (ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                    INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO) 
                    INNER JOIN Contrato_PacoteServico_Servico as cps on (cps.ID_SERVICO=s.ID_SERVICO and cps.ID_CONTRATO=ci.ID_CONTRATO)
                    INNER JOIN S    ervico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                    INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                            
                    INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                    LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                    LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                    LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
            ORDER BY 
                    UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO, Endereco.TX_ENDERECO
                         """)

        row = cursor.fetchone()
        while row:
            logradouro: Optional[str] = cf(row[0])
            numero: Optional[str] = cf(row[1])
            complemento: Optional[str] = cf(row[2])
            bairro: Optional[str] = cf(row[5])
            cep: Optional[str] = cf(row[3])
            condominio: Optional[str] = cf(row[4])
            cidade: Optional[str] = cf(row[7])
            uf: Optional[str] = cf(row[8])
            is_radio = row[10] == "radio"  # a outra opção no wgc é "fibra"
            medianetwork = "Rádio" if is_radio else "Cabo"
            endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, medianetwork=medianetwork)
            await importOrFindAddress(mdb, res, importExecUID, endereco)
            row = cursor.fetchone()
            res.num_processed += 1
    time_end = time.time()
    res.complete = True
    print("Tempo de importação ", time_end - time_ini)
    print(res)
    return res