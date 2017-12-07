class Transaction:
    def __init__(self, _id, _time, _ro=False):
        # transaction index
        self.id = _id

        # read-only: True
        # read-write: False
        self.ro = _ro

        # data successfully writen by transaction
        # key: ID, val: Value
        self.commit_list = {}

        # site touched by this transaction, used for validation during commit, ro transaction don't need touch_set
        self.touch_set = set([])

        self.start_time = _time

        # 'normal': transaction is not blocked
        # 'read'/'write': transaction is waiting for read/write lock
        self.status = "normal"

        # parameters for operation in wait(blocked operation)
        # for read operation: [data_id]
        # for write operation: [data_id, value]
        self.query_buffer = []

        # data locked by this transaction
        # key: ID, val: lock type('r'/'w')
        self.lock_list = {}

        # key: ID, val: Value
        self.cache = {}

        # should abort transaction due to site failure
        self.abort = False

    def __str__(self):
        s = "T"+str(self.id)
        if self.ro:
            s += "\ttype: ro"
        else:
            s += "\ttype: rw"
        s += "\t|\tstart @ "+str(self.start_time)
        s += "\t|\tstatus : "+str(self.status)
        s += "\t|\ttouched_set: "
        for ts in self.touch_set:
            s += str(ts)+" "
        s += "\t|\tvariable_locked: "
        for ld in self.lock_list:
            s += str(ld)+" "
        return s
