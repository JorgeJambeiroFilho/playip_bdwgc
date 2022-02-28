import math
from typing import List, Dict

import pydantic

from playip.bdwgc.bdwgc import wgcrouter, getWDB
from playip.playipchatmongo import getBotMongoDB
from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.inframodels import InfraElement
from playipappcommons.util.levenshtein import levenshteinDistanceDP


class ImportAddressResult(pydantic.BaseModel):
    fail: bool
    message: str
    num_bairros: int
    num_cidades: int
    num_condominios: int
    num_logradouros: int
    num_ufs: int
    last_id_endereco: int

niveis = {
             "TOPO": "uf",
             "uf": "cidade",
             "cidade": "bairro",
             "bairro": "logradouro",
             "logadouro": None
         }

def calc_pmatch(nome, n):
    d = levenshteinDistanceDP(nome, n)
    return 1 - d / math.max(len(nome), len(n))

def findApprox(nome, subs):

    ibest = -1
    best_pmatch = 0
    for i in range(len(subs)):
        sub = subs[i]
        for n in sub.nomes.keys():
            pmatch = calc_pmatch(nome, n)
            if pmatch > best_pmatch:
                best_pmatch = pmatch
                ibest = i
    if best_pmatch > 0.95:
        return ibest
    else:
        return -1


class LocalidadeBusca:
    nivel:str # "TOPO", "UF", "CIDADE", "BAIRRO", "LOGRADOURO"
    nomes:Dict[str, int]
    sub_locs: List['LocalidadeBusca'] = []
    super_loc: 'LocalidadeBusca'

    def createInfraElement(self, endereco:Endereco):
        mdb = getBotMongoDB()
        if self.super_loc.nivel== "TOPO":
            parent_name = "TOPO"
        else:
            parent_name = getattr(endereco, self.super_loc.nivel)

        parentElement: InfraElement = mdb.infra.find({"importKey": parent_name})
        parentId = parentElement["_id"]
        faParentiId = FAMongoId(parentId)
        for n in self.nomes.keys():
            nElement: InfraElement(name=n, parentId=faParentiId)
            break # só tem um elemento nesse momento e ele será sempre a referência



    def findAndAdd(self, add_if_not_found: bool, endereco:Endereco):
        nnivel = niveis[self.nivel]
        if nnivel is None:
            return
        nome = getattr(endereco, nnivel)
        if nome is None:
            return
        i = findApprox(nome, self.sub_locs)
        if i < 0:
            sloc: 'LocalidadeBusca' = LocalidadeBusca()
            sloc.nivel = niveis[self.nivel]
            sloc.super_loc = self
            sloc.nomes[nome] = sloc.nomes.get(nome, 0) + 1
            self.sub_locs.append(sloc)
            if add_if_not_found:
                sloc.createInfraElement(endereco)
        else:
            sloc: 'LocalidadeBusca' = self.sub_locs[i]
            if add_if_not_found:
                sloc.adjustInfraElement(endereco)
        sloc.findAndAdd(add_if_not_found, endereco)


@wgcrouter.get("/importaddresses/{}", response_model=ImportAddressResult)
async def importAddresses(id_client: str, last_id_endereco_imported: int) -> ImportAddressResult:
    wdb = getWDB()
    res: ImportAddressResult = ImportAddressResult()

    with wdb.cursor() as cursor:
        cursor.execute("""
            SELECT  
                    Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO, Cidade.ID_LOCALIDADE as id_cidade, Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf        
            FROM 
                Endereco as Endereco
                    LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                    LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                    LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
            WHERE 
                    Endereco.ID_ENDERECO > {param_last_id_endereco_imported}
            ORDER BY 
                    UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO
                         """.format(param_last_id_endereco_imported=last_id_endereco_imported))

        row = cursor.fetchone()
        while row:

            row = cursor.fetchone()


    return res