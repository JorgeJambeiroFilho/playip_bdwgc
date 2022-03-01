from typing import List

import pydantic

from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.inframethods import getChildren, createInfraElement, getInfraRoot
from playipappcommons.infra.inframodels import InfraElement
from playipappcommons.util.levenshtein import levenshteinDistanceDP


class ImportAddressResult(pydantic.BaseModel):
    fail: bool = False
    message: str = "ok"
    num_bairros: int = 0
    num_cidades: int = 0
    num_condominios: int = 0
    num_logradouros: int = 0
    num_ufs: int = 0

    num_alt_bairros: int = 0
    num_alt_cidades: int = 0
    num_alt_condominios: int = 0
    num_alt_logradouros: int = 0
    num_alt_ufs: int = 0

    last_id_endereco: int = 0

# niveis_down = {
#              "root": "uf",
#              "uf": "cidade",
#              "cidade": "bairro",
#              "bairro": "logradouro",
#              "logradouro": None
#             }

niveis_up = {
    "root": None,
    "uf": "root",
    "cidade": "uf",
    "bairro": "cidade",
    "logradouro": "bairro",
    None: "logradouro"
}

def calc_pmatch(nome, n):
    d = levenshteinDistanceDP(nome, n)
    return 1 - d / max(len(nome), len(n))

# def findApprox(nome, subs):
#
#     ibest = -1
#     best_pmatch = 0
#     for i in range(len(subs)):
#         sub = subs[i]
#         for n in sub.nomes.keys():
#             pmatch = calc_pmatch(nome, n)
#             if pmatch > best_pmatch:
#                 best_pmatch = pmatch
#                 ibest = i
#     if best_pmatch > 0.95:
#         return ibest
#     else:
#         return -1

def findApprox(import_key:str, nome:str, subs: List[InfraElement]):

    best:InfraElement = None
    best_pmatch = 0
    for sub in subs:
        if sub.importKey==import_key:
            for fn in sub.importFullNames:
                n = fn.split("/")[-1]
                pmatch = calc_pmatch(nome.lower(), n.lower())
                if pmatch > best_pmatch:
                    best_pmatch = pmatch
                    best = sub
    if best_pmatch > 0.90:
        return best
    else:
        return None


# class LocalidadeBusca:
#     nivel:str # "root", "UF", "CIDADE", "BAIRRO", "LOGRADOURO"
#     nomes:Dict[str, int]
#     sub_locs: List['LocalidadeBusca'] = []
#     super_loc: 'LocalidadeBusca'
#
#     def createInfraElement(self, endereco:Endereco):
#         mdb = getBotMongoDB()
#         if self.super_loc.nivel== "root":
#             parent_name = "root"
#         else:
#             parent_name = getattr(endereco, self.super_loc.nivel)
#
#         parentElement: InfraElement = mdb.infra.find({"importKey": parent_name})
#         parentId = parentElement["_id"]
#         faParentiId = FAMongoId(parentId)
#         for n in self.nomes.keys():
#             nElement: InfraElement(name=n, parentId=faParentiId)
#             break # só tem um elemento nesse momento e ele será sempre a referência
#
#
#
#     def findAndAdd(self, add_if_not_found: bool, endereco:Endereco):
#         nnivel = niveis[self.nivel]
#         if nnivel is None:
#             return
#         nome = getattr(endereco, nnivel)
#         if nome is None:
#             return
#         i = findApprox(nome, self.sub_locs)
#         if i < 0:
#             sloc: 'LocalidadeBusca' = LocalidadeBusca()
#             sloc.nivel = niveis[self.nivel]
#             sloc.super_loc = self
#             sloc.nomes[nome] = sloc.nomes.get(nome, 0) + 1
#             self.sub_locs.append(sloc)
#             if add_if_not_found:
#                 sloc.createInfraElement(endereco)
#         else:
#             sloc: 'LocalidadeBusca' = self.sub_locs[i]
#             if add_if_not_found:
#                 sloc.adjustInfraElement(endereco)
#         sloc.findAndAdd(add_if_not_found, endereco)


async def getInfraElementByFullImportName(mdb, importKey:str, fullName:str) -> InfraElement:
    delem = await mdb.infra.find_one({"importKey": importKey, "importFullNames":fullName})
    if not delem:
        return None
    infraElement: InfraElement = InfraElement(**delem)
    return infraElement

def buildFullImportName(endereco: Endereco, nivel:str):
    if not niveis_up[nivel]:
        return ""
    upname = buildFullImportName(endereco, niveis_up[nivel])
    cname = getattr(endereco, nivel).replace("/","-")
    return upname+"/"+cname


async def importAddress(mdb, importResult: ImportAddressResult, import_key: str, endereco: Endereco, nivel: str = None) -> InfraElement:
    if endereco.uf is None or endereco.cidade is None or endereco.bairro is None or endereco.logradouro is None:
        return None
    if nivel is None:
        nivel = niveis_up[None]
    if nivel == "root":
        return await getInfraRoot()
    fname = buildFullImportName(endereco, nivel)
    infraElement:InfraElement = await getInfraElementByFullImportName(mdb, import_key, fname)
    if not infraElement:
        parent:InfraElement = await importAddress(mdb, importResult, import_key, endereco, niveis_up[nivel])
        children: List[InfraElement] = await getChildren(mdb, parent.id)
        cname = getattr(endereco, nivel).replace("/", "-")
        infraElement:InfraElement = findApprox(import_key, cname, children)
        if not infraElement:
            infraElement:InfraElement = await createInfraElement(str(parent.id), cname)
            infraElement.importKey = import_key
            infraElement.importFullNames.append(fname)
            elemDict = infraElement.dict(by_alias=True)
            await mdb.infra.replace_one({"_id": infraElement.id}, elemDict)
            rcname = "num_" + nivel + "s"
            count = getattr(importResult, rcname)
            setattr(importResult,rcname, count + 1)
        else:
            found = False
            for fn in infraElement.importFullNames:
                found = found or fn == fname
            if not found:
                infraElement.importFullNames.append(fname)
                elemDict = infraElement.dict(by_alias=True)
                await mdb.infra.replace_one({"_id": infraElement.id}, elemDict)
                rcname = "num_alt_" + nivel + "s"
                count = getattr(importResult, rcname)
                setattr(importResult,rcname, count + 1)

    return infraElement
