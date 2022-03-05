from typing import Optional

import pydantic

address_level_fields = [ "root", "uf", "cidade", "bairro", "logradouro"]

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


    def setFieldValueByLevel(self, level:int, value:str):
        fn: str = getFieldNameByLevel(level)
        setattr(self, fn, value)
    def getFieldValueByLevel(self, level:int):
        fn:str = getFieldNameByLevel(level)
        return getattr(self, fn)

    def __repr__(self):
        return self.logradouro + ", " + self.numero + \
               (", " + self.complemento if self.complemento else "") + \
               (", " + self.condominio if self.condominio else "") + \
               (", " + self.bairro if self.bairro else "") + ". " \
               + self.cidade + ". " + \
               ("CEP " + self.cep if self.cep else "") + ". "


