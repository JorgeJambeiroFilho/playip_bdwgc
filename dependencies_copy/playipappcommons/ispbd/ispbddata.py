from typing import Optional

import pydantic

from playipappcommons.infra.endereco import Endereco


class ContractData(pydantic.BaseModel):
    id_contract: str
    found: bool = False
    download_speed: Optional[int] = None
    upload_speed: Optional[int] = None
    is_radio: Optional[bool] = None
    is_ftth: Optional[bool] = None
    pack_name:Optional[str] = None
    user_name:Optional[str] = None
#    onu_key:Optional[str] = None
    home_access_key:Optional[str] = None
    home_access_type:Optional[str] = None
    endereco: Optional[Endereco] = None
    bloqueado: Optional[bool] = None
    dt_ativacao: Optional[float] = None
    dt_cancelamento: Optional[float] = None

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)

    def getMedia(self):
        return "Rádio" if self.is_radio else "Cabo"


class Client(pydantic.BaseModel):
    found: bool = False
    id_client: Optional[str] = None
    name: Optional[str] = None
    alt_name: Optional[str] = None
    cpfcnpj: Optional[str] = None

    def __str__(self):
        return "Client(id_client="+str(self.id_client)+", name="+str(self.name)+", cpfcnpj="+str(self.cpfcnpj)+")"



