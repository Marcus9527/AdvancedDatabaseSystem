from Site import Site
from transaction import Transaction

# TODO: where to put waitQue?
class DataManager:
    database = []
    def __init__(self):
        for i in range(1,11):
            self.database.append(Site(i))

    def generateCacheForRO(self, transaction: Transaction):
        for site in self.database:
            for var in site.variables:
                # TODO: if a transaction is RO, cache all available data??
                transaction.cacheVarData(var)

    def getRunningSites(self):
        runningSites = []
        for site in self.database:
            if site.isUp():
                runningSites.append(site.getSiteNum())

        return runningSites

    def readLock(self, ID, transaction, lockType):
        siteNum = -1
        for site in self.database:
            # when it comes to read, must check whether readytoread (if failed):
            if site.isUp() and site.isVariablePresent(ID) and site.isVariableReadyRead(ID)):
                site.lockVar(ID, transaction, lockType)
                return site.getSiteNum()
        return siteNum

    def writeLock(self, ID, transaction, lockType):
        siteNum = -1
        for site in self.database:
            # write doesn't have to check writeready.
            if site.isUp() and site.isVariablePresent(ID):
                site.lockVar(ID, transaction, lockType)
                return site.getSiteNum()
        return siteNum

    def checkReadClock(self, ID):
        for site in self.database:
            if site.isUp() and site.isVariablePresent(ID):
                if (site.isVariableLocked(ID)
						and (site.getLockType(ID) == Lock.WRITE || site
								.getLockType(var) == Lock.READ)

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


    def recover(self, site_id):
        pass

    def fail(self, site_id):
        pass