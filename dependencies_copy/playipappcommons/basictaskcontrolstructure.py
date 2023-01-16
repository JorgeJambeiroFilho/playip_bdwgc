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
    clearing: bool = False
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

    def isClearRequested(self):
        return self.clearing

    def isClearPendind(self):
        return self.clearing
    def isClearing(self):
        return self.clearing and time.time() - self.last_action <= self.max_inactive_time
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
        #self.clearing = False
        self.message = "ok"

    def startClear(self):
        self.clearing = True
        self.act()

    def start(self):
        self.complete = False
        self.started = True
        self.justStarted = True
        self.aborted = False
        self.message = "ok"
        self.clearing = False
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

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


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
        if bcs.isClearRequested():
            if bcs.isComplete():
                bcs.clearing = False
                await bcs.saveHardly(mdb)  # o clear se completou
                return True
            elif not bcs_old.isGoingOn() and not bcs_old.isClearing():
                await bcs.saveHardly(mdb)  # o clear ficou pendente, então o chamador deve fazer de novo
                return True
            elif bcs_old.isClearing():
                bcs.message = "ClearInProgress"
                return False  # Não deve começar a limpar de novo
            elif bcs_old.isAborted():
                bcs.message = "ProcessNotStoppedYet"
                return False  # diz ao clear que não completou com sucesso, e ele não deve realizar as operações de limpeza.
            else:
                bcs.message = "CannotClearRunningProcess"
                return False  # diz ao clear que não completou com sucesso, e ele não deve realizar as operações de limpeza.
        else:  # não é clear
            if bcs_old.isClearing():
                bcs.message = "ClearInProgress"
                return True  # deve parar
            elif bcs_old.isClearPendind():
                bcs.message = "NeedsClear"
                return True
            elif bcs_old.isComplete(): # o comando paralelo completou a operação
                bcs.done() # passa a informação de que acabou na estrutura bcs para que, sendo ela salva depois, a informação não seja apagada
                return True  # diz ao chamador que a tarefa se completou
            elif bcs_old.isAborted():
                bcs.abort() # passa a informação de que abortou na estrutura bcs para que, sendo ela salva depois, a informação não seja apagada
                if bcs.isComplete():
                    await bcs.saveHardly(mdb)
                return True
            elif bcs.isAborted():
                if bcs_old.isGoingOn() or bcs_old.isSuspended():
                    await bcs.saveHardly(mdb)
                    return True  # mandou abortar
                else:
                    return False # não havia o que abortar
            else:
                await bcs.saveHardly(mdb)
                if bcs.isComplete():
                    print("SaveSoftly complete")
                return bcs.isComplete()

    else:
        if bcs.isClearRequested() or bcs.isAborted():
            return False  # nem exitia, então não há o que limpar nem parar
        else:
            await bcs.saveHardly(mdb)
            return bcs.isComplete() # se o chamador tinha completado a tarefa retornar true é natural, embora ele já saiba

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



