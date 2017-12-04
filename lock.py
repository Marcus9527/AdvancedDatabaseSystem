class Lock:
    def __init__(self):
        # 0: not locked;
        # 1: Read;
        # 2: Write;
        self.type = 0
        # since var could have multiple Read locks
        # transaction, locktype
        self.lockerDict = {}
        # # [transaction,_type]
        # self.wait_list = []

    # def release(self):
    #     self.type = 0

    # return 1: locked
    # return 0: waiting
    # TODO: Transaction should have a global dict {trans, variable}?
    # When add lock is called, we ensure that it is legal
    def addLock(self, trans, lockType):
        self.lockerDict[trans] = lockType

    def removeLock(self, trans):
        self.lockerDict.pop(trans)

    # release current lock and give it to next transaction waiting
    def promote(self):
        pass

    def isFree(self):
        return self.type == 0

    def getType(self):
        return self.type

    def getLocker(self):
        return self.lockerDict.keys()


