class Lock:
    def __init__(self):
        # 0: not locked;
        # 1: Read;
        # 2: Write;
        self.type = 0
        # since var could have multiple Read locks
        # transactionID, locktype
        self.lockerDict = {}
        # # [transaction,_type]
        # self.wait_list = []

    # def release(self):
    #     self.type = 0

    # return 1: locked
    # return 0: waiting
    # TODO: Transaction should have a global dict {trans, variable}?
    # When add lock is called, we ensure that it is legal
    def addLock(self, transID, lockType):
        self.lockerDict[transID] = lockType
        self.type = lockType

    def removeLock(self, transID):
        if transID not in self.lockerDict:
            print('Error, this trans {0} is not in lockDict'.format(transID))
        else:
            self.lockerDict.pop(transID)
            if not self.lockerDict:
                self.type = 0

    # release current lock and give it to next transaction waiting
    def promote(self):
        pass

    def isFree(self):
        return self.type == 0

    def getType(self):
        return self.type

    def getLocker(self):
        return list(self.lockerDict.keys())


