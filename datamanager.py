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
        for i in range(1,21):
            self.lockTable["x" + str(i)] = Lock()
            if i % 2 == 0:
                self.varSite["x" + str(i)] = [i for i in range(1,11)]
            else:
                self.varSite["x" + str(i)] = (i + 1) % 10

        for i in range(1,11):
            self.database.append(Site(i))

    def generateCacheForRO(self, trans):
        for site in self.database:
            if site.isUp():
                variables = site.getAllVariables()
                for var in variables:
                    if site.isVarValid(var):
                        value = variables[var]
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
                    # abort
                    return (False, [-2])
            else:
                # ID is the first read var
                # need to cache:
                self.generateCacheForRO(trans)

                if ID in trans.cache:
                    print('read only, data: ', ID, '->', trans.cache[ID])
                    return (True, [])
                else:
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
                            print('read(not read only) data which locked by himself: ', ID, '->', site.getAllVariables()[ID])
                            return (True, [site.getSiteNum()])
                        else:
                            # write locked by other:
                            # dont have to try other sites
                            return (False, lockers)
                    else:
                        # not locked or read lock:
                        site.lockVar(ID, trans, 1)
                        print('get read lock and read(not read only) data: ', ID, '->', site.getAllVariables()[ID])
                        return (True, site.getSiteNum())

            # have to wait for site recover or recovered site being update
            return (False, [-1])

    def write(self, trans, ID):
        sites = self.varSite[ID]
        blockers = set()
        couldWriteLock = True
        runningSite = 0
        for site in sites:
            if site.isUp():
                runningSite += 1
                lockers = site.lockTable[ID].getLocker()
                # if write locked:
                if site.getLockType(ID) == 2:
                    # writed locked by himself?
                    if ID in lockers:
                        return (True, site.getSiteNum())
                    else:
                        return (False, lockers)
                elif site.getLockType(ID) == 1:
                    # read locked only by himself:
                    if len(lockers) == 1 and lockers[0].id == trans.id:
                        continue
                    else:
                        couldWriteLock = False
                        # blockers could have himself
                        for blocker in lockers:
                            if blocker.id != trans.id:
                                blockers.add(blocker)
        if runningSite == 0:
            # no running site, must wait for recovery
            return (False, [-1])
        elif couldWriteLock:
            for site in sites:
                if site.isUp():
                    site.lockVar(ID,trans,2)
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

    def writeValToDatabase(self, ID, val):
        sites = self.varSite[ID]
        for site in sites:
            if site.isUp() and site.isVariablePresent(ID):
                site.write()



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
                        print("Variable: ", var.getID(), ", Data: ", var.getData())

        elif siteNum is None:
            msg = "dump data " + str(ID) + " from all site"
            print(msg)
            for site in self.database:
                if site.isUp():
                    print("Site number: ", site.getSiteNum())
                    if site.isVariablePresent(ID):
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


    def recover(self, siteNum):
        pass

    def fail(self, site_id):
        pass