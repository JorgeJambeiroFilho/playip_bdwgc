import time
import uuid
from typing import Optional, cast


from playip.bdwgc.bdwgc import getWDB
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ProcessAddressResult,  ImportAddressResult, \
    importAddressWithoutProcessing, iar_key


def cf(s):
    if s is None:
        return None
    r = s.strip().replace("/","-")
    return r


#curl -X 'GET' 'http://app.playip.com.br/playipispbd/import/importaddresses/wgc/Cotia' -H 'accept: application/json' -H 'access-token: djd62o$w*N<H$k8'
#curl -X 'GET' 'http://app.playip.com.br/playipchathelper/infra/clear' -H 'accept: application/json'
#curl -X 'GET' 'http://app.playip.com.br/playipchathelper/infra/failsclear' -H 'accept: application/json'

async def getImportAddressResultIntern(mdb, begin:bool) -> ImportAddressResult:
    return cast(ImportAddressResult, await getControlStructure(mdb, iar_key, begin))




async def importAddressesIntern(mdb, iar:ImportAddressResult):
    wdb = await getWDB()
    importExecUID: str = str(uuid.uuid1())
    time_ini = time.time()
    try:
        iar.clearCounts()
        with wdb.cursor() as cursor:
            cursor.execute("""
                SELECT  
                        Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                        Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                        Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf, tmeio.TX_DESCRICAO_TIPO        
                FROM             
                        Contrato_PacoteServico_Servico as cps 
                        INNER JOIN Contrato as Contrato on (cps.ID_CONTRATO = Contrato.ID_CONTRATO)
                        INNER JOIN PacoteServico as ps on (cps.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                        INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO) 
                        
                        INNER JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                        INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                                
                        INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                        LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                        LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                        LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
                GROUP BY
                        UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO, tmeio.TX_DESCRICAO_TIPO, Endereco.TX_CEP, Condominio.NM_CONDOMINIO,
                        UF.NM_UF, Cidade.TX_NOME_LOCALIDADE
                ORDER BY 
                        UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO, tmeio.TX_DESCRICAO_TIPO, Endereco.TX_CEP, Condominio.NM_CONDOMINIO,
                        UF.NM_UF, Cidade.TX_NOME_LOCALIDADE
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
                is_radio = row[10] == cf("radio")  # a outra opção no wgc é "fibra"
                medianetwork = "Rádio" if is_radio else "Cabo"

                endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento,
                                              bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf)

                await importAddressWithoutProcessing(mdb, iar, importExecUID, endereco, medianetwork)

                # endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, prefix="Infraestrutura-"+medianetwork)
                # await importOrFindAddress(mdb, res, importExecUID, endereco)
                # res.num_processed += 1
                #
                # endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, prefix="Comercial")
                # await importOrFindAddress(mdb, res, importExecUID, endereco)
                # res.num_processed += 1
                if await iar.saveSoftly(mdb):
                    return

                row = cursor.fetchone()
    finally:
        time_end = time.time()
        iar.done()
        await iar.saveSoftly(mdb)
        print("Tempo de importação ", time_end - time_ini)
        print(iar)


