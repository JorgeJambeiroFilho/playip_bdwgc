from typing import Optional

import pydantic


class Endereco(pydantic.BaseModel):
    logradouro: Optional[str]
    numero: Optional[str]
    complemento: Optional[str]
    bairro: Optional[str]
    cep: Optional[str]
    condominio: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]

    def __repr__(self):
        return self.logradouro + ", " + self.numero + \
               (", " + self.complemento if self.complemento else "") + \
               (", " + self.condominio if self.condominio else "") + \
               (", " + self.bairro if self.bairro else "") + ". " \
               + self.cidade + ". " + \
               ("CEP " + self.cep if self.cep else "") + ". "


