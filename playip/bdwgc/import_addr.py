from typing import Optional

from fastapi import APIRouter

from playip.bdwgc.bdwgc import getWDB
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ImportAddressResult, importAddress
from playipappcommons.playipchatmongo import getBotMongoDB

importrouter = APIRouter(prefix="/playipispbd/import")

def cf(s):
    if s is None:
        return None
    r = s.strip()#.strip("'").strip()
    return r


#curl -X 'GET' 'http://app.playip.com.br/playipispbd/import/importaddresses/wgc/Cotia' -H 'accept: application/json' -H 'access-token: djd62o$w*N<H$k8'


@importrouter.get("/importaddresses/{import_key}/{cidade_alvo}", response_model=ImportAddressResult)
async def importAddresses(import_key: str, cidade_alvo: str) -> ImportAddressResult:
    mdb = getBotMongoDB()
    wdb = getWDB()
    res: ImportAddressResult = ImportAddressResult()

    with wdb.cursor() as cursor:
        cursor.execute("""
            SELECT  
                    Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                    Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                    Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf        
            FROM 
                Endereco as Endereco
                    LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                    LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                    LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
            ORDER BY 
                    UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO
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
            endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf)
            if cidade is not None and cidade.lower() == cidade_alvo.lower():
                await importAddress(mdb, res, import_key, endereco)
            row = cursor.fetchone()

    print(res)
    return res