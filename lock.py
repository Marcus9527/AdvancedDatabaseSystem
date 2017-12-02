class Lock:
    def __init__(self):
        # 0: not locked;
        # 1: RO;
        # 2: RW
        self.type = 0
        self.locker_list = []
        # [transaction,_type]
        self.wait_list = []

    def release(self):
        self.type = 0

    # return 1: locked
    # return 0: waiting
    def lock(self, _locker, _type):
        if self.type == 0:
            self.type = _type
            self.locker_list = [_locker]
            return "Locked"
        elif self.type == 1:
            if _type == 1:
                self.locker_list.append(_locker)
                return "Locked"
            else:
                self.wait_list.append(_locker)
                return "Waiting"
        else:
            self.wait_list.append([_locker, _type])
            return "Waiting"

    # release current lock and give it to next transaction waiting
    def promote(self):
        pass

    def is_free(self):
        return self.type == 0

    def get_type(self):
        return self.type

    def get_locker(self):
        return self.locker_list


