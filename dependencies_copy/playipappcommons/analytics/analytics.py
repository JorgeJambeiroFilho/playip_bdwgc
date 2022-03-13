from datetime import datetime
from typing import Dict, Optional, List

import pydantic
from bson import ObjectId
from pydantic import Field

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco
from playipappcommons.playipchatmongo import getBotMongoDB

class ContractAnalyticData(pydantic.BaseModel):
    id_contract: str
    DT_INICIO: datetime
    DT_FIM: datetime
    DT_ATIVACAO: datetime
    DT_CANCELAMENTO: datetime
    endereco: Endereco

    download_speed: Optional[int] = None
    upload_speed: Optional[int] = None
    is_radio: Optional[bool] = None
    is_ftth: Optional[bool] = None
    pack_name:Optional[str] = None
    endereco: Optional[Endereco] = None
    bloqueado: Optional[bool] = None



class ISPEvent(pydantic.BaseModel):

    context: str # uma chave que identifica, por exemplo uma área geográfica, uma caracterítica de um cliente ou qualquer outra coisa que normalmente se queira colocar em um filtro
                 # a chave é composta, podendo indentificar simutaneamente vários critérios de filtragem. Os critéris sao separado por ";"



    period_group: str #  o grupo de períodos é algo razoavelmente arbitrário. Pode ser, por exemplo, "ANO" ou "MES", mas pode também ser "SEMESTRE:MARCO" indicado que as contabilizações
                 # são feitas a cada seis meses, mas que os semestres não começam em janeiro e sim em março.


    period: str # identifica um periodo de tempo denr do grupo.  Pode ser "2015", "2016/01", etc. O significado deve ser interpretado de acordo com o grupo, mas deve ser suficiente
                # para uma identificação global. Não adianta identificar o semestre sem identificar o ano, por exemplo.
                # As identificações devem ser tais que a ordem alfabética resulte na ordem correta

                 # contabilizações de perídos diferentes dentro de um meso contexto ficam no mesmo registro no BD

    event_type: str # pode ser algo como "INSTALACAO", "CANCELAMENTO", "ULTIMO PAGAMENTO", etc

    metric_name: str # algo como "CONTAGEM" ou "VALOR"

    metric_value: float # para "CONTAGEM" deve ser sempre 1

class ISPDateEvent(pydantic.BaseModel):

    context: str # uma chave que identifica, por exemplo uma área geográfica, uma caracterítica de um cliente ou qualquer outra coisa que normalmente se queira colocar em um filtro
                 # a chave é composta, podendo indentificar simutaneamente vários critérios de filtragem. Os critéris sao separado por ";"

    event_type: str # pode ser algo como "INSTALACAO", "CANCELAMENTO", "ULTIMO PAGAMENTO", etc

    metric_name: str # algo como "CONTAGEM" ou "VALOR"

    metric_value: float # para "CONTAGEM" deve ser sempre 1

    dt: datetime


class ISPContextMetrics(pydantic.BaseModel):
    id: Optional[FAMongoId] = Field(alias='_id')
    # campos que combinados indentificam o registro
    context: str
    event_type: str
    metric_name: str
    period_group: str

    period_metric: Dict[str, float]

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = FAMongoId()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    def count(self, iv: ISPEvent):
        if self.context != iv.context or self.event_type != iv.event_type or self.metric_name != iv.metric_name or self.period_group != iv.period_group:
            raise Exception("Contabilização de evento em contexto errado")

        v: float = self.period_metric.get(iv.period, 0.0)
        self.period_metric[iv.period] = v + iv.metric_value

def dateToPeriods(dt:datetime) -> Dict[str, str]:

    res: Dict[str, str] = [] # period_group, period

    res["SEMANA"] = str(dt.year) + "/" + str(dt.month + 1).zfill(2) + "/" + str( (dt.day-1) % 7 + 1)
    res["QUINZENA"] = str(dt.year) + "/" + str(dt.month + 1).zfill(2) + "/" + str((dt.day - 1) % 15 + 1)
    res["MES"] = str(dt.year)+"/"+str(dt.month+1)
    res["TRIMESTRE"] = str(dt.year) + "/" + str(dt.month % 4 + 1).zfill(2)
    res["SEMESTRE"] = str(dt.year) + "/" + str(dt.month % 4 + 1).zfill(2)
    res["ANO"] = str(dt.year)

    return res


async def count_event(iv: ISPEvent):
    mdb = getBotMongoDB()
    icm = await mdb.ISPContextMetrics.find_one\
                  (
                     {
                        "context": iv.context,
                        "event_type": iv.event_type,
                        "metric_name": iv.metric_name,
                        "period_group": iv.period_group
                     }
                  )
    if not icm:
        icm = ISPContextMetrics()
    icm.count(iv)

    icmDict = icm.dict(by_alias=True)
    await mdb.infra.replace_one({"_id": icm.id}, icmDict)

async def count_events(idv: ISPDateEvent):

    periods: Dict[str, str] = dateToPeriods(idv.dt)
    for period_group, period in periods.items():
        iv: ISPEvent = ISPEvent\
            (
                context=idv.context,
                period_group=period_group,
                period=period,
                event_type=idv.event_type,
                metric_name=idv.metric_name,
                metric_value=idv.metric_value
            )
        await count_event(iv)

