import pydantic
from bson import ObjectId
from pydantic import Field
from typing import Optional, List

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco


class AddressQuery(pydantic.BaseModel):
    address: Optional[str] = None
    endereco: Optional[Endereco] = None
    medianetwork: Optional[str] = None
    test:bool = False

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)

class AddressCondition(pydantic.BaseModel):
    id:Optional[FAMongoId] = Field(alias='_id')
    type:str
    rule:str
    address_field: Optional[str] = None

    def check(self, addressQuery: AddressQuery):
        if not addressQuery.endereco:
            address = addressQuery.address
            return address.find(self.rule) >= 0
        elif not self.address_field:
            address = str(addressQuery.endereco)
            return address.find(self.rule) >= 0
        elif getattr(addressQuery.endereco, self.address_field) is None:
            return False
        else:
            fv = getattr(addressQuery.endereco, self.address_field)
            return fv.find(self.rule) >= 0

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = ObjectId()

    def getLongestWordPair(self):
        maxl = 0
        imaxl = 0
        ww = self.rule.split()
        if len(ww) == 0:
            return ""
        if len(ww) == 1:
            return ww[0]
        for i in range(len(ww)-1):
            li = len(ww[i]) + len(ww[i+1]) + 1
            if li > maxl:
                maxl = li
                imaxl = i

        return ww[imaxl] + ' ' + ww[imaxl+1]


class AddressFilter(pydantic.BaseModel):
    id:Optional[FAMongoId] = Field(alias='_id')
    conditions: List[AddressCondition] = []
    indexedWordPair: str = ""

    def check(self, addressQuery: AddressQuery):
        for cond in self.conditions:
            if not cond.check(addressQuery):
                return False
        return True

    def getLongestWordPair(self):
        maxPair = ""
        for cond in self.conditions:
            pair = cond.getLongestWordPair()
            if len(pair) > len(maxPair):
                maxPair = pair
        return maxPair

    def adjustIndexedWordPair(self):
        self.indexedWordPair = self.getLongestWordPair()

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = ObjectId()


class InfraElementLevelName(pydantic.BaseModel):
    name :str
    timestamp: float = 0

class InfraElement(pydantic.BaseModel):
    id: Optional[FAMongoId] = Field(alias='_id')
    parentId: Optional[FAMongoId] # pai em termos de estrutura
    name: str
    inFail: bool = False
    numDescendantsInFail: int = 0
    filters: List[AddressFilter] = []
    message:str = ""
    dtInterrupcao: float = 0
    dtPrevisao: float = 0
#

    parentAddressId: Optional[FAMongoId]  # pai em termos de endereco.
    addressLevel: Optional[int] = None # determina o campo de endereço responsavel pela passagem do pai para o presente nó. Veja Endereco.getFieldNameByLevel()
    addressLevelValues: List[str] = [] # anteriomente os address level names eram string e se chamavam address level values. Entao, ao ler objetos antigos no banco, esse é o campo carregado. No contrutor, o campo addressLevelNames é preenchido
    addressLevelNames: List[InfraElementLevelName] = []
    maxAddressLevelNameTimestamp: float = 0

    # importKey: Optional[str] = None
    addressFullNames: List[str] = [] #um exemplo de um nome completo seria "SP/Itapevi/Jardim Santa Rita"
    importExecUID: Optional[str] = None
    manuallyMoved: bool = False

    def checkFilters(self, addressQuery: AddressQuery):
        for filt in self.filters:
            if filt.check(addressQuery):
                return True
        return False

    def adjustIndexedWordPairs(self):
        for filter in self.filters:
            filter.adjustIndexedWordPair()

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = ObjectId()
        for alv in self.addressLevelValues:
            self.addressLevelNames.append(InfraElementLevelName(name=alv, timestamp=0.0))
        self.addressLevelValues = []

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

def InfraElementToDict(elem: InfraElement):
    d = elem.dict(by_alias=True)
    return d

def InfraElementsToDict(elems: List[InfraElement]):
    dl = [elem.dict(by_alias=True) for elem in elems]
    return dl


class AddressInFail(pydantic.BaseModel):
    located: bool = False
    inFail: bool = False
    descricao: Optional[str] = None
    dtInterrupcao: float = 0
    dtPrevisao: float = 0


    # noFail: bool #o resultado pode ser ambíguo, como True em ambas estas variáveis se mais de uma regra casas com o endereço
    # elems = []  #para testes isto interessa, fora isto a lista vem sempre vazia
    #
    # #class Config:
    # #    json_encoders = { list: InfraElementsToDict }
