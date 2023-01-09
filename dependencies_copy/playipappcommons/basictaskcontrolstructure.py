import time

import pydantic

from playipappcommons.playipchatmongo import getMongoClient


class BasicTaskControlStructure(pydantic.BaseModel):
    key: str
    fail: bool = False
    complete: bool = False
    started: bool = False
    justStarted: bool = False # nunca é salvo como True (veja save()). Só é True quando acaba de dar o start. Veja getControlStructure().
    message: str = "ok"
    aborted: bool = False
    num_processed: int = 0
    num_fails: int = 0
    last_action: float = 0
    max_inactive_time: float = 60.0 # cada processo deve colocar um número adequado aqui
                                     # um processo nunca pode ficar maix que max_inactive_time milisegundos sem
                                     # atualizar last_action ou outras instâncias ou outras chamadas a mesma instância
                                     # concluirão que ele caiu

    def __init__(self, **kargs):
        super().__init__(**kargs)
        self.max_inactive_time: float = 60.0
    def isGoingOn(self):
        return self.started and not self.complete and time.time() - self.last_action <= self.max_inactive_time

    def isSuspended(self):
        return self.started and not self.complete and time.time() - self.last_action > self.max_inactive_time

    def isComplete(self):
        return self.complete

    def act(self):
        self.last_action = time.time()

    def done(self):
        self.complete = True
        self.started = False

    def start(self):
        self.complete = False
        self.started = True
        self.justStarted = True
        self.aborted = False
        self.message = "ok"
        self.act()

    def hasJustStarted(self) -> bool:
        return self.justStarted

    def saveSoftly(self, mdb):
        return saveSoftly(mdb, self)

    async def saveHardly(self, mdb):
        resDict = self.dict(by_alias=True)
        resDict["justStarted"] = False
        self.act()
        await mdb.control.replace_one({"key": self.key}, resDict, upsert=True)

    def isAborted(self):
        return self.aborted

    def abort(self):
        self.aborted = True

    def clearCounts(self):
        self.num_processed: int = 0
        self.num_fails: int = 0

    def clear(self):
        if self.isGoingOn():
            res = self.copy()
            res.message = "CannotClearRunningProcess"
            return res
        else:
            self.fail: bool = False
            self.complete: bool = False
            self.started: bool = False
            self.message: str = "ok"
            self.aborted: bool = False
            self.last_action: float = 0
            self.message = "ok"
            self.clearCounts()

taskControlFactories = {}


def registerTaskControlStructureFactory(key: str, creatorFunction):
    global taskControlFactories
    taskControlFactories[key] = creatorFunction


def createTaskControlStructure(json) -> BasicTaskControlStructure:
    global taskControlFactories
    return taskControlFactories[json["key"]](json)


# retona True se o chamador deve parar de executar a operação caso esteja em um laço
async def saveOrEndIfAbortedCallback(session, bcs:BasicTaskControlStructure):
    mdb = session.client.PlayIPChatHelper

    resDict = await mdb.control.find_one({"key":bcs.key})
    if resDict:
        bcs_old: BasicTaskControlStructure = BasicTaskControlStructure(**resDict) # pega a estrutura que está na BD pois outro comando pode ter ocorrido em paralelo e ela tem a informação
        if bcs_old.isComplete(): # o comando paralelo completou a operação
            if not bcs.started: # assume que está sendo feito um clear, pois só ele faz sentido aqui
                await bcs.saveHardly(mdb) # a informação do clear é salva sobre o que tinha-se antes.
                bcs.done()
                return True # diz ao clear que a operação se completou
            else:
                bcs.done() # passa informação de que a te=arefa está completa para o chamador pela estrutura bcs
                return True  # diz ao chamador que a tarefa se completou
        else:
            if not bcs.started:
                if bcs_old.isGoingOn():
                    bcs.message = "CannotClearRunningProcess"
                    bcs.done()
                    return False # diz ao clear que não completou com sucesso, ele não tem um laço para interromper, então é só um informação mesmo
                else:
                    bcs.message = "ok"
                    bcs.done()
                    return True # diz ao clear que não completou, ele não tem um laço para interromper, então é só um informação mesmo


        if bcs_old.isAborted():
            bcs.abort()
            bcs.done() # se tinha abortado em paralelo, registra a aborto e consequente termino da tarefa na estrutura bcs do chamador

        await bcs.saveHardly(mdb)

        # nesse ponto bcs tem a informação de aborto tanto se ela veio da chamador ou paralelamente
        # mas se veio paralelamente, bcs.isComplete() é True, pois  bcs.done() já foi chamada
        if bcs.isAborted() and not bcs_old.isGoingOn() and not bcs_old.isSuspended():
            return False # o chamador mandou abortar, mas não havia nada rodando em paralelo
                         # se o comando de aborto veio paralelamente, então o chamador está executando, logo goiongOn vai dar True
                         # e o fluxo de execuçãop não chega aqui
        else:
            if bcs.isComplete():
                print("SaveSoftly complete")
            return bcs.isComplete()

    else:
        await bcs.saveHardly(mdb)
        return bcs.isComplete() # se o chamador tinha completado a tarefa retorna true é natural, embora ele já saiba

async def saveSoftly(mdb, bcs:BasicTaskControlStructure) -> bool:
    mdbcli = getMongoClient()
    async with await mdbcli.start_session() as session:
        res = await session.with_transaction(lambda s: saveOrEndIfAbortedCallback(s, bcs))
        return res


async def getControlStructureCallback(session, key:str, begin:bool) -> BasicTaskControlStructure:

    mdb = session.client.PlayIPChatHelper

    resDict = await mdb.control.find_one({"key":key})
    if resDict is None:
        res = createTaskControlStructure({"key":key})
    else:
        res: BasicTaskControlStructure = createTaskControlStructure(resDict)
    if not res.isGoingOn() and begin:
        res.start()
        await res.saveHardly(mdb)

    return res

async def getControlStructure(mdb, key:str, begin:bool) -> BasicTaskControlStructure:

    mdbcli = getMongoClient()
    async with await mdbcli.start_session() as session:
        res = await session.with_transaction(lambda s: getControlStructureCallback(s, key, begin))
        return res



