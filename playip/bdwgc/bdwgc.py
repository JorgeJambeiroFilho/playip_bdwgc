from dynaconf import settings
from fastapi import APIRouter
import pydantic
import fastapi
import pyodbc

print("FASTAPI version ",fastapi.__version__)

wgcrouter = APIRouter(prefix="/playipispbd/basic")

gwbd = None

def getWDB():
    global gwbd
    if not gwbd:
        gwbd = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+settings.SERVER+';DATABASE='+settings.DATABASE+';UID='+settings.USERNAME+';PWD='+ settings.PASSWORD)
    return gwbd

class ContractData(pydantic.BaseModel):
    id_contract: str
    download_speed: int
    upload_speed:int
    is_radio:bool
    found:bool
    pack_name:str

@wgcrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str) -> ContractData:
    wdb = getWDB()

    #existe uma tabela chamada Servico que tinha tudo para ser relevante aqui, mas não foi
    with wdb.cursor() as cursor:
        cursor.execute("""
        
          SELECT  NM_PACOTE_SERVICO, VL_DOWNLOAD, VL_UPLOAD, s.VL_SERVICO, DT_ATIVACAO, DT_DESATIVACAO, 
                  dici.ID_TIPO_MEIO_ACESSO, dici.ID_TIPO_TECNOLOGIA, dici.ID_TIPO_PRODUTO, 
                  tmeio.TX_DESCRICAO_TIPO, ttec.TX_DESCRICAO_TIPO, tprod.TX_DESCRICAO_TIPO
          FROM 
                  ContratoItem as ci, PacoteServico as ps, PacoteServico_Servico as s, Contrato_PacoteServico_Servico as cps, Servico_DICI as dici,
                  TiposDiversos as tmeio, TiposDiversos as ttec, TiposDiversos as tprod
          WHERE 
                ci.ID_CONTRATO={param_id_contrato} and 
                ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO and 
                ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO and 
                cps.ID_SERVICO=s.ID_SERVICO and
                cps.ID_CONTRATO=ci.ID_CONTRATO and
                dici.ID_SERVICO=cps.ID_SERVICO and
                tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO and
                ttec.ID_TIPO_DIVERSOS=dici.ID_TIPO_TECNOLOGIA and
                tprod.ID_TIPO_DIVERSOS=dici.ID_TIPO_PRODUTO and
                s.VL_DOWNLOAD > 0
          ORDER BY
                    DT_ATIVACAO DESC
                
                         """.format(param_id_contrato=id_contract))
        row = cursor.fetchone()
        if not row or row[5]: #se tem data de desativação, não vale
            res = ContractData(id_contract=id_contract, found=False)
        else:
            name = row[0]
            dl = row[1]
            ul =row[2]
            is_radio = row[9]=="radio" # a outra opção no wgc é "fibra"

            res = ContractData(id_contract=id_contract, download_speed=dl, upload_speed=ul, pack_name=name, is_radio=is_radio, found=True)

    return res


          # SELECT
          #       NM_PACOTE_SERVICO,VL_DOWNLOAD, VL_UPLOAD, s.VL_SERVICO, DT_ATIVACAO, DT_DESATIVACAO
          # FROM
          #       ContratoItem as ci, PacoteServico as ps, PacoteServico_Servico as s, Contrato_PacoteServico_Servico as cps
          # WHERE
          #       ci.ID_CONTRATO={param_id_contrato} and
          #       ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO and
          #       ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO and
          #       cps.ID_SERVICO=s.ID_SERVICO and
          #       cps.ID_CONTRATO=ci.ID_CONTRATO and
          #       s.VL_DOWNLOAD > 0
          # ORDER BY
          #           DT_ATIVACAO DESC