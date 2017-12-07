# author    : Lei Guo
# date      : Dec. 7, 2017
# Lock class simply tracks the lock information for a variable
# includes a lockDict which records who (transactionID) hold which type of lock.

class Lock:
    def __init__(self):
        # 0: Free;
        # 1: Read;
        # 2: Write;
        self.type = 0
        # since var could have multiple Read locks
        # transactionID, lockType
        self.lockerDict = {}

    # When add lock is called, we ensure that it is legal
    def addLock(self, transID, lockType):
        self.lockerDict[transID] = lockType
        self.type = lockType

    def removeLock(self, transID):
        if transID in self.lockerDict:
            self.lockerDict.pop(transID)
            if not self.lockerDict:
                # if not lock in the lockDict, it is free
                self.type = 0

    # getters:
    def isFree(self):
        return self.type == 0

    def getType(self):
        return self.type

    def getLocker(self):
        return list(self.lockerDict.keys())
