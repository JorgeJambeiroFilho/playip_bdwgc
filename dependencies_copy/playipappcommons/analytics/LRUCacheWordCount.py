from typing import Optional, cast

import pydantic
from bson import ObjectId
from pydantic import Field

from playipappcommons.basictaskcontrolstructure import BasicTaskControlStructure
from playipappcommons.famongo import FAMongoId
from playipappcommons.util.LRUCache import LRUCache

class CountWordsResult(BasicTaskControlStructure):

    num_processed: int = 0
    num_fails: int = 0

    num_updates: int = 0
    num_creations: int = 0
    num_cache_hits: int = 0


class WordFreq(pydantic.BaseModel):
    id: Optional[FAMongoId] = Field(alias='_id')
    role: str
    context_type: str
    context_value: str
    target_context_type: str
    target_value: Optional[str]
    freq:int = 0

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        if not self.id:
            self.id = FAMongoId()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    def count(self):
        self.freq += 1

class LRUCacheWordCount(LRUCache):

    def __init__(self, mdb, table, res:CountWordsResult, *args, **kargs):
        super().__init__(*args, **kargs)
        self.res = res
        self.mdb = mdb

        self.table = table

    def registerHit(self):
        self.res.num_cache_hits += 1

    async def load(self, key):
        #icm = None
        wf = await self.mdb[self.table].find_one \
           (
                {
                    "role": key[0],
                    "context_type": key[1],
                    "context_value": key[2],
                    "target_context_type": key[3],
                    "target_value": key[4]
                }
           )
        if wf is None:
            wf = WordFreq\
            (
                    role=key[0],
                    context_type=key[1],
                    context_value=key[2],
                    target_context_type=key[3],
                    target_value=key[4]
            )
            self.res.num_creations += 1
            return wf
        else:
            return WordFreq(**wf)

    async def save(self, key, obj):
        wf = cast(WordFreq, obj)
        wfDict = wf.dict(by_alias=True)
        self.res.num_updates += 1
        await self.mdb[self.table].replace_one({"_id": wf.id}, wfDict, upsert=True)
        print(self.res)

    # role é um dentre
    #   "target", usado no numerador
    #   "ref", usado no denominador
    #   "ref_unique", usado para fazer suavização
    # context_type diz que tipo de coisa a chave identifica. No caso é bairro, logradour, cidade, etc
    # context_value é o valor do contexto
    # target_context_type identifica o tipo contexto do target . No caso, também é bairro, logradoure, cidade, etc.
    # target_value é none se role não é target e tem a palavra sendo contada quando é

    # Eu poderia por exemplo, saber quantas palavras diferentes no campo "logradouro" existem dentro do bairro "Jardim Santa Rita" usando
    #    role = "ref_unique"
    #    context_type = "bairro"
    #    context_value = "Jardim Santa Rita"
    #    target_context_type = "palavra_em_logradouro"
    #    target_value = None
    # Para contar a frequencia da palavra "avenida" no contexto acima usaria
    #    role = "target"
    #    context_type = "bairro"
    #    context_value = "Jardim Santa Rita"
    #    target_context_type = "palavra_em_logradouro"
    #    target_value = "avenida"

    async def getByWord(self, role:str, context_type:str, context_value:str, target_context_type:str, target_value:str) -> WordFreq:
        key = (role, context_type, context_value, target_context_type, target_value)
        return await self.get(key)
