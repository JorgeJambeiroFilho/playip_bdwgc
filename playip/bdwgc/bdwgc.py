
from typing import Optional
from typing import List
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

class Endereco(pydantic.BaseModel):
    logradouro: str
    numero: str
    complemento: str
    cep: str
    condominio: Optional[str]
    cidade: str

class ContractData(pydantic.BaseModel):
    id_contract: str
    found: bool = False
    download_speed: Optional[int] = None
    upload_speed: Optional[int] = None
    is_radio: Optional[bool] = None
    is_ftth: Optional[bool] = None
    pack_name:Optional[str] = None
    user_name:Optional[str] = None
    home_access_key:Optional[str] = None
    home_access_type:Optional[str] = None
    endereco: Optional[Endereco] = None

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)

@wgcrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str) -> ContractData:
    wdb = getWDB()

    #existe uma tabela chamada Servico que tinha tudo para ser relevante aqui, mas não foi
    with wdb.cursor() as cursor:
        cursor.execute("""

          SELECT  NM_PACOTE_SERVICO, VL_DOWNLOAD, VL_UPLOAD, s.VL_SERVICO, cps.DT_ATIVACAO, cps.DT_DESATIVACAO, 
                  dici.ID_TIPO_MEIO_ACESSO, dici.ID_TIPO_TECNOLOGIA, dici.ID_TIPO_PRODUTO, 
                  tmeio.TX_DESCRICAO_TIPO, ttec.TX_DESCRICAO_TIPO, tprod.TX_DESCRICAO_TIPO, uname.TX_USERNAME
          FROM 
                  ContratoItem as ci, PacoteServico as ps, PacoteServico_Servico as s, Contrato_PacoteServico_Servico as cps, Servico_DICI as dici,
                  TiposDiversos as tmeio, TiposDiversos as ttec, TiposDiversos as tprod, UserName as uname
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
                ci.ID_CONTRATO=uname.ID_CONTRATO and
                uname.dt_desativacao is null and
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
            is_ftth = "ftth" in str(row[10]).lower()
            userName = row[12]

            home_access_type = "smartolt" if is_ftth else "aircontrol" if is_radio else "none"


            res = ContractData(id_contract=id_contract, download_speed=dl, upload_speed=ul, pack_name=name, is_radio=is_radio, is_ftth=is_ftth, found=True, user_name=userName, home_access_key=userName, home_access_type=home_access_type)

    with wdb.cursor() as cursor:
        cursor.execute("""
        SELECT  
                Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, Endereco.TX_CEP as cep, Condominio.TX_NOME_LOCALIDADE as condominio, Cidade.TX_NOME_LOCALIDADE as cidade        
        FROM 
                Contrato as Contrato         
                INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                LEFT JOIN LOG_LOCALIDADE as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_LOCALIDADE)
    
        WHERE 
                ID_CONTRATO = 13000
    
                """)
        row = cursor.fetchone()
        if row:
            logradouro: str = row[0]
            numero: str = row[1]
            complemento: str = row[2]
            cep: str = row[3]
            condominio: Optional[str] = row[4]
            cidade: Optional[str] = row[5]
            endereco: Endereco(logradouro=logradouro, numero=numero, complemento=complemento, cep=cep, condominio=condominio, cidade=cidade)
            res.endereco = endereco

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