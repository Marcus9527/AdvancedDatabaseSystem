from site import Site
from transaction import Transaction
from lock import Lock

# TODO: Might need to put a global lock table here!!!
class DataManager:
    def __init__(self):
        self.database = []
        # key: var; val: sites
        self.varSite = {}
        # global locakTable
        # map var to locks
        self.lockTable = {}
        # 1-10 sites:
        for i in range(1,11):
            self.database.append(Site(i))

        # 1-20 variables:
        for i in range(1,21):
            self.lockTable["x" + str(i)] = Lock()  # TODO: it's never used
            if i % 2 == 0:
                self.varSite["x" + str(i)] = [self.database[i-1] for i in range(1,11)]
            else:
                self.varSite["x" + str(i)] = [self.database[(i + 1) % 10 - 1]]

    def generateCacheForRO(self, trans):
        for site in self.database:
            if site.isUp():
                variables = site.getAllVariables()
                for var in variables:
                    if site.isVarValid(var):
                        value = variables[var].getData()
                        trans.cache[var] = value

    # def getRunningSites(self):
    #     runningSites = []
    #     for site in self.database:
    #         if site.isUp():
    #             runningSites.append(site.getSiteNum())
    #
    #     return runningSites

    def read(self,trans, ID):
        # first predicate whether need to cache:
        if trans.ro:
            if trans.cache:
                if ID in trans.cache:
                    print('read only, data: ', ID, '->', trans.cache[ID])
                    return (True, [])
                else:
                    # this is not the first read and the data is not present in cache
                    # abort!
                    return (False, [-2])
            else:
                # ID is the first read var
                # need to cache:
                self.generateCacheForRO(trans)

                if ID in trans.cache:
                    print('read only, data: ', ID, '->', trans.cache[ID])
                    return (True, [])
                else:
                    # even the first one is not in the database, may be because the site fails or !isValidVar()
                    # have to wait until valid
                    trans.cache.clear()
                    return (False, [-1])
        else:
            sites = self.varSite[ID]
            # find a site where this var is not write locked by other trans
            for site in sites:
                # only data conform to the below conditions could be read
                if site.isUp() and site.isVarValid(ID):
                    lockers = site.lockTable[ID].getLocker()
                    if site.getLockType(ID) == 2:
                        # if write lock:
                        if ID in lockers:
                            # read a data where write lock by himself
                            print('read(not read only) data which locked by himself: ', ID, '->', site.getAllVariables()[ID].getData())
                            return (True, [site.getSiteNum()])
                        else:
                            # write locked by other:
                            # dont have to try other sites
                            return (False, lockers)
                    else:
                        # not locked or read lock:
                        site.lockVar(ID, trans.id, 1)
                        print('get read lock and read(not read only) data: ', ID, '->', site.getAllVariables()[ID].getData())
                        return (True, [site.getSiteNum()])

            # have to wait for site recover or recovered site being update
            return (False, [-1])

    def write(self, transID, ID):
        # in sum, the transaction could get the write lock only when the data is unlocked, or
        # locked by himself
        print('varSite: {}'.format(self.varSite))
        sites = self.varSite[ID]
        blockers = set()
        couldWriteLock = True
        runningSite = 0
        siteNums = []
        for site in sites:
            if site.isUp():
                siteNums.append(site.getSiteNum())
                runningSite += 1
                lockers = site.lockTable[ID].getLocker()
                # if write locked:
                if site.getLockType(ID) == 2:
                    # writed locked by himself?
                    if ID in lockers:
                        couldWriteLock = True
                    else:
                        return (False, lockers)
                elif site.getLockType(ID) == 1:
                    # read locked only by himself:
                    if len(lockers) == 1 and lockers[0] == transID:
                        continue
                    else:
                        couldWriteLock = False
                        # blockers could have himself
                        for blockerID in lockers:
                            if blockerID != transID:
                                blockers.add(blockerID)
        if runningSite == 0:
            # no running site, must wait for recovery
            return (False, [-1])
        elif couldWriteLock:
            for site in sites:
                if site.isUp():
                    site.lockVar(ID,transID,2)
            print('T2: asasjgfasjdgasjfas')
            return (True, siteNums)
        else:
            return (False, list(blockers))


    # def readLock(self, ID, transaction, lockType):
    #     siteNum = -1
    #     for site in self.database:
    #         # when it comes to read, must check whether readytoread (if failed):
    #         if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID):
    #             site.lockVar(ID, transaction, lockType)
    #             return site.getSiteNum()
    #     return siteNum
    #
    # def writeLock(self, ID, transaction, lockType):
    #     siteNum = -1
    #     for site in self.database:
    #         # write doesn't have to check writeready.
    #         if site.isUp() and site.isVariablePresent(ID):
    #             site.lockVar(ID, transaction, lockType)
    #             return site.getSiteNum()
    #     return siteNum

    # def checkReadlock(self, ID):
    #     sites = self.varSite[ID]
    #     for site in sites:
    #         if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID):
    #             if site.isVariableLocked(ID) and site.getLockType(ID) == 1:
    #                 return True
    #     return False
    #
    # def checkWriteLock(self, ID):
    #     sites = self.varSite[ID]
    #     for site in sites:
    #         if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID):
    #             if site.isVariableLocked(ID) and site.getLockType(ID) == 2:
    #                 return True
    #     return False
    #
    # def checkReadWriteLock(self, ID):
    #     sites = self.varSite[ID]
    #     for site in sites:
    #         if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID):
    #             if site.isVariableLocked(ID) and (site.getLockType(ID) == 2 or site.getLockType(ID) == 1):
    #                 return True
    #     return False
    #
    # def isVarAvailableForRead(self, ID):
    #     sites = self.varSite[ID]
    #     for site in sites:
    #         if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID):
    #             return True
    #     return False
    #
    # def isVarAvailableForWrite(self, ID):
    #     sites = self.varSite[ID]
    #     for site in sites:
    #         if site.isUp() and site.isVariablePresent(ID):
    #             return True
    #     return False

    # TODO: RO has commitList?
    def commit(self, transId, commitList):
        if len(commitList) == 0:
            print('This transaction has nothing to commit')
            return
        else:
            for ID, val in commitList.items():
                self.writeValToDatabase(ID, val)
                print('Transaction {0} changed variable {1} to {2}'.format(transId, ID, val))


    def writeValToDatabase(self, ID, val):
        sites = self.varSite[ID]
        for site in sites:
            if site.isUp():
                site.writeVarVal(ID, val)

    # TODO: return unlocked variables?
    def releaseLocks(self, transID, lockDict):
        # what if some sites this data is read locked and some dont
        freeVar = set()
        for varID in lockDict:
            sites = self.varSite[varID]
            isFree = True
            for site in sites:
                if site.isUp():
                    site.unLock(transID, varID)
                    if not site.isVariableFree(varID):
                        isFree = False
            if isFree:
                freeVar.add(varID)
        return freeVar


    def dump(self, siteNum=None, ID=None):
        print("DM phase:")
        if siteNum is None and ID is None:
            msg = "dump all data"
            print(msg)
            for site in self.database:
                if site.isUp():
                    print("Site number: ", site.getSiteNum())
                    variables = site.getAllVariables()
                    for ID in variables:
                        var = variables[ID]
                        print("Variable: ", ID, ", Data: ", var.getData())

        elif siteNum is None:
            msg = "dump data " + str(ID) + " from all site"
            print(msg)
            sites = self.varSite[ID]
            for site in sites:
                if site.isUp():
                    print("Site number: ", site.getSiteNum())
                    var = site.getVariable(ID)
                    print("Variable: ", var.getID(), ", Data: ", var.getData())
        else:
            msg = "dump data on site " + str(siteNum)
            print(msg)
            site = self.database[siteNum-1]
            if site.isUp():
                print("Site number: ", site.getSiteNum())
                variables = site.getAllVariables()
                for ID in variables:
                    var = variables[ID]
                    print("Variable: ", var.getID(), ", Data: ", var.getData())

    def fail(self, siteNum):
        site = self.database[siteNum-1]
        if not site.isUp():
            print('site: {0} is already down!'.format(siteNum))
        else:
            site.failSite()
            print('The site {0} is down now!'.format(siteNum))

    def recover(self, siteNum):
        site = self.database[siteNum - 1]
        if site.isUp():
            print('site: {0} is already up!'.format(siteNum))
        else:
            site.recoverSite()
            print('The site {0} is up now!'.format(siteNum))