# author    : Lei Guo
# date      : Dec. 7, 2017
# Site class do the work which is issued by the DataManager.
# Any work about a certain site would eventually be done by calling the functions in Site.
# 1. Site number and status of the site
# 2. Variables, Lock table and a special isReady table in the site
# 3. Add lock or unlock a variable
# 4. Fail and recover this site

from datetime import datetime
from variable import Variable
from lock import Lock

class Site:
    def __init__(self, siteNum):
        self.isRunning = True
        self.isRecovered = True
        self.siteNum = siteNum
        # key: VariableID, value: Variable, this is for fast query data by ID
        self.variables = {}
        # key: VariableID, value: Lock(s)
        # Locks keep tracking of who locked, who is waiting and what's the current lock type.
        self.lockTable = {}
        # key: VariableID, value: boolean, ready or not
        # Note this isReady is to determine whether a variable's value is valid for read.
        self.isReady = {}
        self.timeStamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # initialize the site here.
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
    # getters:
    def getTime(self):
        return self.timeStamp

    def getAllVariables(self):
        return self.variables

    def getVariable(self, ID):
        return self.variables[ID]

    def getLockType(self, ID):
        return self.lockTable[ID].type

    def getSiteNum(self):
        return self.siteNum

    def isUp(self):
        return self.isRunning

    def isVariableFree(self, ID):
        if self.lockTable[ID].type == 0:
            return True
        else:
            return False

    # determine whether this variable's value is valid for read
    def isVarValid(self, ID):
        return self.isReady[ID]

    # determine whether a variable has replicate in other sites
    # used in deciding whether a variable is ready to be read right after recovery
    def isReplicated(self, ID):
        num = int(ID[1:])
        if num % 2 == 0:
            return True
        else:
            return False

    # add a lock to the variable
    # note that we have ensured that we could add the lock in the DM
    def lockVar(self, ID, transID, lockType):
        # TODO: in Lock:
        self.lockTable[ID].addLock(transID, lockType)

    # remove locks by a certain transaction
    def unLock(self, transID, ID):
        self.lockTable[ID].removeLock(transID)

    def writeVarVal(self, ID, val):
        self.variables[ID].setData(val)

    # fail this site and clear the lock table
    def failSite(self):
        self.isRunning = False
        self.isRecovered = False
        self.lockTable.clear()
        for var in self.isReady:
            self.isReady[var] = False

    # recover the site and initialize the lock table
    # determine whether the variables are valid for read or not
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
