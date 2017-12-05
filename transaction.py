class Transaction:
    def __init__(self, _id, _time, _ro=False):
        # transaction index
        self.id = _id
        # read-only: True   read-write: False
        self.ro = _ro
        self.commit_list = {}
        self.locking = []
        # site touched by this transaction, used for validation during commit
        # read-only transaction don't need touch_set
        self.touch_set = set([])
        self.start_time = _time
        self.status = "normal"
        self.query_buffer = []
        # datas locked by this transaction
        self.lock_list = {}
        # key: ID, val: Value
        self.cache = {}
        self.ro_version = -1

    def __str__(self):
        s = "T"+str(self.id)
        if self.ro:
            s += "\ttype: ro"
        else:
            s += "\ttype: rw"
        s += "\t|\tstart @ "+str(self.start_time)
        s += "\t|\tstatus : "+str(self.status)
        return s
