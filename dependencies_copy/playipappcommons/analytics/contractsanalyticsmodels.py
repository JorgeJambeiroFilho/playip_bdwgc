from __future__ import annotations
from typing import Optional, List, Dict, Tuple

import pydantic
from bson import ObjectId
from pydantic import Field

from playipappcommons.basictaskcontrolstructure import BasicTaskControlStructure, registerTaskControlStructureFactory
from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco


iadr_key: str = "ImportAnalyticDataResult"


class ImportContractsResult(BasicTaskControlStructure):
    key: str = iadr_key
    num_new: int = 0


def createImportAnalyticDataResult(json):
    return ImportContractsResult(**json)


registerTaskControlStructureFactory(iadr_key, createImportAnalyticDataResult)


pcr_key: str = "ProcessContractsResult"


class ProcessContractsResult(BasicTaskControlStructure):
    key: str = pcr_key
    max_inactive_time: float = 180.0 # há momentos demorados, então é preciso esperar

    num_sem_data_inicio: int = 0
    num_enderecos_nao_reconhecidos: int = 0

    num_service_fails: int = 0
    num_sem_download: int = 0
    num_service_processed: int = 0
    num_updates: int = 0
    num_creations: int = 0
    num_cache_hits: int = 0



def createProcessContractsResult(json):
    return ProcessContractsResult(**json)


registerTaskControlStructureFactory(pcr_key, createProcessContractsResult)

class TicketData(pydantic.BaseModel):
    DT_ABERTURA: Optional[float]
    DT_FECHAMENTO: Optional[float]
    NM_AREA_TICKET: Optional[str]

    def __eq__(self, other:TicketData):
        if self.DT_ABERTURA != other.DT_ABERTURA:
            return False
        if self.DT_FECHAMENTO != other.DT_FECHAMENTO:
            return False
        if self.NM_AREA_TICKET != other.NM_AREA_TICKET:
            return False

        return True

class ServicePackAnalyticData(pydantic.BaseModel):
    fullName: str #ex internet/cabo/ftth/planoxxxx  a contabilização usa os níveis

    # fazem parte do fullname
    # meio:str = None
    # tecnologia: str = None
    # produto: str = None

    DT_ATIVACAO: Optional[float]
    DT_DESATIVACAO: Optional[float]
    DT_DESISTENCIA: Optional[float]
    DT_CADASTRO: float
    TX_MOTIVO_CANCELAMENTO: Optional[str]
    VL_SERVICO: float
    download_speed: Optional[float] = None
    upload_speed: Optional[float] = None

    #NM_PACOTE: str
    VL_PACOTE: float

    tickets: List[TicketData] = []

    def __eq__(self, other:ServicePackAnalyticData):
        if self.fullName != other.fullName:
            return False
        if self.DT_ATIVACAO != other.DT_ATIVACAO:
            return False
        if self.DT_DESATIVACAO != other.DT_DESATIVACAO:
            return False
        if self.DT_DESISTENCIA != other.DT_DESISTENCIA:
            return False
        if self.DT_CADASTRO != other.DT_CADASTRO:
            return False
        if self.TX_MOTIVO_CANCELAMENTO != other.TX_MOTIVO_CANCELAMENTO:
            return False
        if self.VL_SERVICO != other.VL_SERVICO:
            return False
        if self.download_speed != other.download_speed:
            return False
        if self.upload_speed != other.upload_speed:
            return False
        if self.VL_PACOTE != other.VL_PACOTE:
            return False
        if len(self.tickets) != len(other.tickets):
            return False
        for i in range(len(self.tickets)):
            if not self.tickets[i].__eq__(other.tickets[i]):
                a = self.tickets[i].__eq__(other.tickets[i])
                return False
        return True







class ContractAnalyticData(pydantic.BaseModel):
    id_contract: str
    DT_INICIO: float # Estou me baseado no na ativação, entaõ comentei esse campo
    DT_FIM: float # no WGC é simplesmente 1 ano depois de DT_INICIO
    DT_ATIVACAO: Optional[float]
    DT_CANCELAMENTO: Optional[float]
    STATUS_CONTRATO: str
    enderecos: List[Endereco] # os endereços são vários para que seja possível manter estruturas paralelas com agruopamentos convenientes
                              # na verdade, é sempre memso endereço com modificações que levam a contabilização para outro ramo,
                              # tipicamente há uma versão comercial e uma estrutural
    services: List[ServicePackAnalyticData] = []

    # preenchidos a partir de endereco em count_events_contracts_raw
    # fullAddressName: Optional[str]
    # fullStructName: Optional[str]

    #bloqueado: Optional[bool] = None

    def __eq__(self, other: ContractAnalyticData):
        if self.id_contract != other.id_contract:
            return False
        if self.DT_INICIO != other.DT_INICIO:
            return False
        if self.DT_FIM != other.DT_FIM:
            return False
        if self.DT_ATIVACAO != other.DT_ATIVACAO:
            return False
        if self.DT_CANCELAMENTO != other.DT_CANCELAMENTO:
            return False
        if self.STATUS_CONTRATO != other.STATUS_CONTRATO:
            return False
        if len(self.enderecos) != len(other.enderecos):
            return False
        for i in range(len(self.enderecos)):
            if not self.enderecos[i].__eq__(other.enderecos[i]):
                a = self.enderecos[i].__eq__(other.enderecos[i])
                return False
        if len(self.services) != len(other.services):
            return False
        for i in range(len(self.services)):
            if not self.services[i].__eq__(other.services[i]):
                a = self.services[i].__eq__(other.services[i])
                return False
        return True








# os pacotes dos contratos estão vazios. As informações de contrato estão replicadas de modo mais parecido com o retorno de uma consulta SQL
class ServicePackAndContractAnalyticData(pydantic.BaseModel):
    contract: ContractAnalyticData
    service: ServicePackAnalyticData
    ticket: Optional[TicketData]

class ISPEvent(pydantic.BaseModel):

    # context: str # uma chave que identifica, por exemplo uma área geográfica, uma caracterítica de um cliente ou qualquer outra coisa que normalmente se queira colocar em um filtro
    #              # a chave é composta, podendo indentificar simutaneamente vários critérios de filtragem. Os critéris sao separado por ";"


    infraElementId: str # identificadores de região na hierarquia de infraestrutura da raiz até o elemento separado por "/"
    infraElementOptic: str = "struct" # podia ser "struct" or "address", mas desiti dese conceito e coloco sempre struct. Criei remaos separdos na hierarquia para aler pela duas óticas que tinha imaginado

    fullProductName: str # identificador do produto de um jeit hierarquico, por exemplo "internet/cabo/fttb/pacote xxx"

    period_group: str #  o grupo de períodos é algo razoavelmente arbitrário. Pode ser, por exemplo, "ANO" ou "MES", mas pode também ser "SEMESTRE:MARCO" indicado que as contabilizações
                 # são feitas a cada seis meses, mas que os semestres não começam em janeiro e sim em março.


    period: str # identifica um periodo de tempo denr do grupo.  Pode ser "2015", "2016/01", etc. O significado deve ser interpretado de acordo com o grupo, mas deve ser suficiente
                # para uma identificação global. Não adianta identificar o semestre sem identificar o ano, por exemplo.
                # As identificações devem ser tais que a ordem alfabética resulte na ordem correta

                 # contabilizações de perídos diferentes dentro de um meso contexto ficam no mesmo registro no BD

    eventType: str # pode ser algo como "INSTALACAO", "CANCELAMENTO", "ULTIMO PAGAMENTO", etc

    metricName: str # algo como "CONTAGEM" ou "VALOR"

    metricValue: float # para "CONTAGEM" deve ser sempre 1




class ISPDateEvent(pydantic.BaseModel):

    # context: str # uma chave que identifica, por exemplo uma área geográfica, uma caracterítica de um cliente ou qualquer outra coisa que normalmente se queira colocar em um filtro
    #              # a chave é composta, podendo indentificar simutaneamente vários critérios de filtragem. Os critéris sao separado por ";"

    infraElementId: str # identificadores de região na hierarquia de infraestrutura da raiz até o elemento separado por "/"
    infraElementOptic: str # pode ser "struct" or "address"

    fullProductName: str # identificador do produto de um jeit hierarquico, por exemplo "internet/cabo/fttb/pacote xxx"


    eventType: str # pode ser algo como "INSTALACAO", "CANCELAMENTO", "ULTIMO PAGAMENTO", etc

    metricName: str # algo como "CONTAGEM" ou "VALOR"

    metricValue: float # para "CONTAGEM" deve ser sempre 1

    dt: float



class FullMetricsContext(pydantic.BaseModel):
    #infraElementName: Optional[str] = None # esse campo é só para facilitar a depuração. Ele é preenchido quando possível, mas não é usado pelo programa. Note que é ignorado no método __key
    infraElementFullName: Optional[str] # ajuda o cliente javascript a exibir nome amigáveis, pois o infraElementId é só um código
    infraElementId: Optional[str] # identificadores de região na hierarquia de infraestrutura da raiz até o elemento separado por "/"
    infraElementOptic: Optional[str]
    fullProductName: Optional[str]
    eventType: Optional[str]
    metricName: Optional[str]
    period_group: Optional[str]

    def __key(self):
        return (self.infraElementId, self.infraElementOptic, self.fullProductName, self.eventType, self.metricName, self.period_group)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, FullMetricsContext):
            return self.__key() == other.__key()
        return NotImplemented


class ExpandableFullMetricsContext(pydantic.BaseModel):
    context: FullMetricsContext
    maxInfraElementDescendantsExpansion: int # zero fica só com o elemento, 1 pega filhos, 2 netos ...
    minInfraElementDescendantsExpansion: int
    maxProductDescendantsExpansion: int # zero fica só com o elemento, 1 pega filhos, 2 netos ...
    minProductDescendantsExpansion: int
    maxEventTypeDescendandsExpansion:int
    minEventTypeDescendandsExpansion: int


# class FullMetricsContextExpansions(pydantic.BaseModel):
#     contextExpansions:List[FullMetricsContext]


class MetricsQuery(pydantic.BaseModel):
    operator:str
    constant: Optional[float] = None
    left: Optional[MetricsQuery] = None
    right: Optional[MetricsQuery] = None
    context: Optional[FullMetricsContext] = None
    infraUpLevels: int = -1 # -1 significa que o infraelement dentro do contexto é absoluto.
                           # De zero para cima via subindo na hierarquia a partir de uma referência
                           # as consultas sempre chegam ao servidor com infraelements absolutos
                           # Porém, o servidor gurda consultas para os clientes e esse podem usar
                           # referências relativas
    # esse contexto tem que ser compatível com todos os expandableContexts da
    # ExpandableMetricsQuery na qual esta MetricsQuery está inserida
    # Só é none se o operador for constante



class ExpandableMetricsQuery(pydantic.BaseModel):
    queryKey: str # essa chave é retornada ao cliente para que ele possa saber de que query é essa resposta
    query: MetricsQuery
    expandableContexts: List[ExpandableFullMetricsContext]


MetricsQuery.update_forward_refs()

class UserQuery(MetricsQuery):
    pass

class ExpandableMetricsSession(pydantic.BaseModel):
    expandableContexts: List[ExpandableFullMetricsContext]
    queries: List[UserQuery]


class ChartQueries(pydantic.BaseModel):
    expandableContexts: List[ExpandableFullMetricsContext]
    expandableMetricsSessions: List[ExpandableMetricsSession]


class AnalyticsReportSetResult(pydantic.BaseModel):
    fail:bool
    message: str

class AnalyticsReportSpecification(pydantic.BaseModel):
    reportId: str
    reportName: Optional[str]

class AnalyticsReport(pydantic.BaseModel):
    id: Optional[FAMongoId] = Field(alias='_id')
    name: Optional[str]
    expandableContexts: List[ExpandableFullMetricsContext] = []
    charts: List[ChartQueries] = []

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = FAMongoId()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }


class ResultantMetrics(pydantic.BaseModel):
    periods:List[str] = None
    series:Dict[FullMetricsContext, List[float]] = {} # as chaves indicam nomes completos considerando expansão que tenha sido feitas se a consulta só envolveu um contexto, existe uma chave só igual a ""
    context: Optional[FullMetricsContext] = None
    constant: Optional[float] = None

class ResultantMetricsFlat(pydantic.BaseModel):
    queryKey: str # a chave passada na query
    fail: bool = False
    message:str = None
    periods:List[str] = None
    series:List[Tuple[FullMetricsContext, List[float]]] = {} # as chaves indicam nomes completos considerando expansão que tenha sido feitas se a consulta só envolveu um contexto, existe uma chave só igual a ""
    context: Optional[FullMetricsContext] = None
    constant: Optional[float] = None

    def __init__(self, queryKey, rm:ResultantMetrics=None, *args, **kargs):
        super().__init__(queryKey=queryKey, *args, **kargs)
        if rm:
            self.periods = rm.periods
            self.series = list(rm.series.items())
            self.context = rm.context
            self.constant = rm.constant
            self.queryKey = queryKey

class ISPContextMetrics(pydantic.BaseModel):
    id: Optional[FAMongoId] = Field(alias='_id')
    # campos que combinados identificam o registro
    #context: str
    infraElementId: str # identificadores de região na hierarquia de infraestrutura da raiz até o elemento separado por "/"
    infraElementOptic: str
    fullProductName: str
    eventType: str
    metricName: str
    period_group: str
    period_metric: Dict[str, float]
    years: Dict[str, bool] = {}

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = FAMongoId()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }


    def count(self, iv: ISPEvent, unlearn: bool):
        if self.infraElementId != iv.infraElementId or\
           self.infraElementOptic != iv.infraElementOptic or\
           self.fullProductName != iv.fullProductName or\
           self.eventType != iv.eventType or \
           self.metricName != iv.metricName or\
           self.period_group != iv.period_group:
                raise Exception("Contabilização de evento em contexto errado")

        #self.extendPeriod(iv.period)
        v: float = self.period_metric.get(iv.period, 0.0)
        sign = -1 if unlearn else 1
        self.period_metric[iv.period] = v + sign * iv.metricValue


class ContractStorageAnalyticData(pydantic.BaseModel):
    id:Optional[FAMongoId] = Field(alias='_id')
    id_contract: str
    not_accounted: Optional[ContractAnalyticData]
    accounted: Optional[ContractAnalyticData]

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = FAMongoId()
