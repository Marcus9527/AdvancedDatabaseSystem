# author    : Lei Guo
# date      : Dec. 7, 2017
# Data Manager does the work below:
# 1. The database: list of 10 Sites
# 2. (Attempt to) read/write: decide whether a certain transaction could or could not get the requested lock,
# 3. Fail/Recovering a certain site
# 4. Commit a certain transaction (write values to database)
# 5. Dump the values required of the current database

from site import Site

class DataManager:
    def __init__(self):
        self.database = []
        # key: var; val: sites
        self.varSite = {}
        # global locakTable
        # map var to locks
        # self.lockTable = {}
        # 1-10 sites:
        for i in range(1,11):
            self.database.append(Site(i))

        # 1-20 variables:
        for i in range(1,21):
            # self.lockTable["x" + str(i)] = Lock()  # it's never used
            if i % 2 == 0:
                self.varSite["x" + str(i)] = [self.database[i-1] for i in range(1,11)]
            else:
                self.varSite["x" + str(i)] = [self.database[(i + 1) % 10 - 1]]

    # for the read only transactions, cache all the current values of variables
    # it may has overhead in space but it is efficient for accessing certain values for transactions.
    def generateCacheForRO(self, trans):
        for site in self.database:
            if site.isUp():
                variables = site.getAllVariables()
                for var in variables:
                    if site.isVarValid(var):
                        value = variables[var].getData()
                        # add the variable's value to this transaction's cache
                        trans.cache[var] = value

    # when the Transaction manager attempts to read from the database
    # decide whether it could get the requested lock
    # if true, return site numbers; if false, return blockers.
    def read(self,trans, ID):
        # first predicate whether need to cache:
        if trans.ro:
            if trans.cache:
                if ID in trans.cache:
                    # if already in cache
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
                # then retrieve the value from cache
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

    # when the Transaction manager attempts to write to the database
    # decide whether it could get the requested lock
    # if true, return list of site numbers; if false, return blockers.
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
                print('site num: ', site.getSiteNum())
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
            print(transID, 'running site == 0')
            return (False, [-1])
        elif couldWriteLock:
            for site in sites:
                if site.isUp():
                    site.lockVar(ID,transID,2)
            return (True, siteNums)
        else:
            return (False, list(blockers))

    # commit a certain transaction
    def commit(self, transId, commitList):
        if len(commitList) == 0:
            print('This transaction has nothing to commit')
            return
        else:
            # commit(write) all the values in the commitList
            for ID, val in commitList.items():
                self.writeValToDatabase(ID, val)
                print('Transaction {0} changed variable {1} to {2}'.format(transId, ID, val))

    # called in commit
    def writeValToDatabase(self, ID, val):
        sites = self.varSite[ID]
        for site in sites:
            if site.isUp():
                site.writeVarVal(ID, val)

    # release locks after commit or abort.
    # return variables which turned Free after the release
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
                        # this variable should be free at each site
                        isFree = False
            if isFree:
                freeVar.add(varID)
        return freeVar

    # dump the values of variables
    # includes: dump all data in the database;
    # dump all data in a certain site;
    # dump a certain variable in all sites;
    def dump(self, siteNum=None, ID=None):
        print("DM phase:")
        if siteNum is None and ID is None:
            # dump all data in the database;
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
            # dump a certain variable in all sites;
            msg = "dump data " + str(ID) + " from all site"
            print(msg)
            sites = self.varSite[ID]
            for site in sites:
                if site.isUp():
                    print("Site number: ", site.getSiteNum())
                    var = site.getVariable(ID)
                    print("Variable: ", var.getID(), ", Data: ", var.getData())
        else:
            # dump a certain variable in all sites;
            msg = "dump data on site " + str(siteNum)
            print(msg)
            site = self.database[siteNum-1]
            if site.isUp():
                print("Site number: ", site.getSiteNum())
                variables = site.getAllVariables()
                for ID in variables:
                    var = variables[ID]
                    print("Variable: ", var.getID(), ", Data: ", var.getData())

    # fail a site
    def fail(self, siteNum):
        site = self.database[siteNum-1]
        if not site.isUp():
            print('site: {0} is already down!'.format(siteNum))
        else:
            site.failSite()
            print('The site {0} is down now!'.format(siteNum))

    # recover a site
    def recover(self, siteNum):
        site = self.database[siteNum - 1]
        if site.isUp():
            print('site: {0} is already up!'.format(siteNum))
        else:
            site.recoverSite()
            print('The site {0} is up now!'.format(siteNum))
