class Lock:
    def __init__(self):
        # 0: not locked;
        # 1: Read;
        # 2: Write;
        self.cutType = 0
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
    def addLock(self, trans, lockType):
        if trans.ro:
            return
        self.lockerDict[trans] = lockType

    def removeLock(self, trans):
        self.lockerDict.pop(trans)

    # release current lock and give it to next transaction waiting
    def promote(self):
        pass

    def is_free(self):
        return self.type == 0

    def get_type(self):
        return self.type

    def get_locker(self):
        return self.locker_list


