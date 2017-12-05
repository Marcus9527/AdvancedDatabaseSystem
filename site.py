from datetime import datetime
from Variable import Variable
from lock import Lock
from transaction import Transaction

# TODO: class Variable
class Site:
    def __init__(self, siteNum):
        self.isRunning = True
        self.isRecovered = True
        self.siteNum = siteNum
        # key: VariableID, value: Variable, this is for fast query data by ID
        self.variables = {}
        # key: VariableID, value: Locks
        # Locks keep tracking of who locked, who is waiting and what's the current lock type.
        self.lockTable = {}
        # key: VariableID, value: boolean, ready or not
        # Note this isReady is for Recovery!
        self.isReady = {}
        self.timeStamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for i in range(1,21):
            v = Variable(i)
            if i % 2 == 0:
                self.variables["x" + str(i)] = v
                self.lockTable["x" + str(i)] = Lock()
                self.isReady["x" + str(i)] = True
            elif (i + 1) % 10 == self.siteNum:
                self.variables["x" + str(i)] = v
                self.lockTable["x" + str(i)] = Lock()
                self.isReady["x" + str(i)] = True

    def getTime(self):
        return self.timeStamp

    # def isVariablePresent(self, ID):
    #     if ID in self.variables:
    #         return True
    #     else:
    #         return False

    # def isVariableReadyRead(self, ID):
    #     return self.isReady[ID]

    def getAllVariables(self):
        return self.variables

    # for most getters below, ensure that call isVariablePresent first
    def getVariable(self, ID):
        return self.variables[ID]

    # def getDataForVar(self, ID):
    #     return self.variables[ID].getData()

    def isVariableFree(self, ID):
        if self.lockTable[ID].type == 0:
            return True
        else:
            return False

    def getLockType(self, ID):
        return self.lockTable[ID].type

    def lockVar(self, ID, transID, lockType):
        # TODO: in Lock:
        self.lockTable[ID].addLock(transID, lockType)

    def unLock(self, transID, ID):
        self.lockTable[ID].removeLock(transID)

    def getSiteNum(self):
        return self.siteNum

    def isUp(self):
        return self.isRunning

    def isVarValid(self, ID):
        return self.isReady[ID]

    def writeVarVal(self, ID, val):
        self.variables[ID].setData(val)

    def failSite(self):
        self.isRunning = False
        self.isRecovered = False
        self.lockTable.clear()
        for var in self.isReady:
            self.isReady[var] = False

    def isReplicated(self, ID):
        num = int(ID[1:])
        if num % 2 == 0:
            return True
        else:
            return False

    def recoverSite(self):
        # reset timestamp:
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.isRunning = True
        for ID in self.variables:
            self.lockTable[ID] = Lock()
        # note the isReplicated() here!
        for ID in self.isReady:
            if not self.isReplicated(ID):
                self.isReady[ID] = True
            else:
                self.isReady[ID] = False



