import time
from typing import Optional

import pydantic
from bson import ObjectId
from pydantic import Field

from playipappcommons.famongo import FAMongoId

address_level_fields = [ "root", "prefix", "uf", "cidade", "bairro", "logradouro", "numero", "complemento"]

def increase_address_level(level:int):
    if level == -1:
        level = len(address_level_fields) - 1
    if level == 0:
        return None
    return level-1

def getFieldNameByLevel(level:int):
    if level ==-1:
        level = len(address_level_fields) - 1
    return address_level_fields[level]

def getFieldLevelByName(name:str):
    return address_level_fields.index(name)

# address_levels_up = {
#     "root": None,
#     "uf": "root",
#     "cidade": "uf",
#     "bairro": "cidade",
#     "logradouro": "bairro",
#     None: "logradouro"
# }




class Endereco(pydantic.BaseModel):
    logradouro: Optional[str]
    numero: Optional[str]
    complemento: Optional[str]
    bairro: Optional[str]
    cep: Optional[str]
    condominio: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    prefix: Optional[str]
    def setFieldValueByLevel(self, level:int, value:str):
        fn: str = getFieldNameByLevel(level)
        setattr(self, fn, value)
    def getFieldValueByLevel(self, level:int):
        fn:str = getFieldNameByLevel(level)
        v = getattr(self, fn)
        if v is None:
            v = ""
        return v

    def __repr__(self):
        return self.logradouro + ", " + self.numero + \
               (", " + self.complemento if self.complemento else "") + \
               (", " + self.condominio if self.condominio else "") + \
               (", " + self.bairro if self.bairro else "") + ". " \
               + self.cidade + "-" + self.uf + ". " + \
               ("CEP " + self.cep + "." if self.cep else "") + \
               ("Prefix" + self.prefix + "." if self.prefix else "")

class SavedAddress(Endereco):
    id: Optional[FAMongoId] = Field(alias='_id')
    timestamp: Optional[float] = None
    mediaNetwork: str

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = ObjectId()
        if not self.timestamp:
            self.timestamp = time.time()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }


def buildFullImportName(endereco: Endereco, nivel: int = -1):
    if nivel == 0:
        return ""
    upname = buildFullImportName(endereco, increase_address_level(nivel))
    cname = endereco.getFieldValueByLevel(nivel)
    cname = cname.replace("/","-")
    return upname+"/"+cname
