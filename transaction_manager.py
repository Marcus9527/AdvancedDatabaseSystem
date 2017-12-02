import re
import transaction
import lock
import datamanager as dm


class TransactionManager:
    def __init__(self):
        # transaction_index : transaction
        self.transaction_list = {}

        # variable_index : lock
        self.lock_table = []
        for i in range(0, 20):
            self.lock_table.append(lock.Lock())

        # transaction_index(a) : [transaction_index(b)] a wait bi
        self.wait_table = {}

        # transaction_index(b) : [transaction_index(ai)] (sequential list) b block ai
        self.block_table = {}
        self.DM = dm.DataManager()

    def parser(self, input_file):
        infile = open(input_file, 'r')
        lines = infile.readlines()
        line_num = 0
        time = 0
        for line in lines:
            line_num += 1
            time += 1
            print("\n"+str(time)+">>>")
            self.deadlock_detection()
            try:
                line = line.strip('\n')
                operation = re.split('\(|\)', line)
                operation_name = operation[0]

                if len(operation) < 2:
                    errmsg = "error: missing parameters for [" + operation_name + "] in line " + str(line_num)
                    raise ValueError(errmsg)
                else:
                    operation_arg = re.split(',', operation[1])

                if operation_name == "begin":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [begin] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0][1:])
                    self.begin(transaction_id, time)

                elif operation_name == "beginRO":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [begin] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0][1:])
                    self.begin(transaction_id, time, ro=True)

                elif operation_name == "R":
                    if len(operation_arg) != 2:
                        errmsg = "error: operation [R] requires exactly 2 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                    transaction_id = int(operation_arg[0][1:])
                    variable_id = int(operation_arg[1][1:])
                    self.read(transaction_id, variable_id)

                elif operation_name == "W":
                    if len(operation_arg) != 3:
                        errmsg = "error: operation [W] requires exactly 3 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0][1:])
                    variable_id = int(operation_arg[1][1:])
                    value = int(operation_arg[2])
                    self.write(transaction_id, variable_id, value)

                elif operation_name == "dump":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [dump] requires no more than 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    if len(operation_arg[0]) == 0:
                        self.dump()
                    elif operation_arg[0][0] == 'x':
                        variable_id = int(operation_arg[0][1:])
                        self.dump(variable=variable_id)
                    else:
                        site_id = int(operation_arg[0])
                        self.dump(site=site_id)

                elif operation_name == "end":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [end] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0][1:])
                    self.end(transaction_id)

                elif operation_name == "fail":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [fail] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    site_id = int(operation_arg[0])
                    self.fail(site_id)

                elif operation_name == "recover":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [fail] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    site_id = int(operation_arg[0])
                    self.recover(site_id)

                else:
                    errmsg = "error: can not recognize operation name: [" + operation_name + "] in line " + str(line_num)
                    raise ValueError(errmsg)

            except ValueError as err:
                print(err.args)

    def begin(self, transaction_id, time, ro=False):
        msg = "begin T"+str(transaction_id)
        if ro:
            msg += " read-only"
        msg += " at "+str(time)
        print(msg)
        t = transaction.Transaction(transaction_id, time)
        self.transaction_list[transaction_id] = t

    def read(self, transaction_id, variable_id):
        msg = "T"+str(transaction_id)+" read x"+str(variable_id)
        print(msg)
        read_result = self.DM.read(variable_id)
        if read_result[0]:
            site_touched = read_result[1]
            self.transaction_list[transaction_id].touch_set.add(site_touched)
        else:
            blocker = read_result[1]
            if transaction_id in self.wait_table:
                self.wait_table[transaction_id].append(blocker)
            else:
                self.wait_table[transaction_id] = [blocker]
            if blocker in self.block_table:
                self.block_table[blocker].append(transaction_id)
            else:
                self.block_table[blocker] =[transaction_id]
            self.transaction_list[transaction_id].status = "read"
            self.transaction_list[transaction_id].query_buffer = [transaction_id, variable_id]

    def write(self, transaction_id, variable_id, value):
        msg = "T"+str(transaction_id)+" write x"+str(variable_id)+" as "+str(value)
        print(msg)

    def dump(self, site=None, variable=None):
        print("TM phase: ")
        if site is None and variable is None:
            msg = "dump all data"
            print(msg)
        elif site is None:
            msg = "dump data x"+str(variable)+" from all site"
            print(msg)
        else:
            msg = "dump data on site "+str(site)
            print(msg)
        self.DM.dump(site, variable)

    def end(self, transaction_id):
        msg = "end T"+str(transaction_id)
        print(msg)

    def fail(self, site_id):
        msg = "site "+str(site_id)+" failed"
        print(msg)
        self.DM.fail(site_id)

    def recover(self, site_id):
        msg = "recover site " + str(site_id)
        print(msg)
        self.DM.recover(site_id)

    def abort(self, transaction_id):
        msg = "abort transaction "+str(transaction_id)
        print(msg)

    def deadlock_detection(self):
        msg = "detecting deadlock"
        print(msg)
        # 0: not visited    1: visiting     2:finished
        visited = {}
        for t in self.transaction_list:
            visited[t] = 0
        for t in visited:
            if visited[t] == 0:
                stack = [t]
                # visited[t] = 1
                while len(stack) != 0:
                    f = stack[-1]
                    if visited[f] == 0 and f in self.wait_table:
                        visited[f] = 1
                        for c in self.wait_table[f]:
                            if c not in self.transaction_list:
                                continue
                        # c = self.wait_table[f][0]
                            if visited[c] == 1:
                                print("There's a circle. Let the killing begin")
                                cur = c
                                youngest_transaction = f
                                while cur != f:
                                    if self.transaction_list[cur].time > self.transaction_list[youngest_transaction].time:
                                        youngest_transaction = cur
                                    for next in self.wait_table[cur]:
                                        if visited[next] == 1:
                                            cur = next
                                print("Prey located, let's sacrifice transaction "+str(youngest_transaction))
                                self.abort(youngest_transaction)
                            elif visited[c] == 0:
                                stack.append(c)
                    else:
                        visited[f] = 2
                        stack.pop()


if __name__ == "__main__":
    TM = TransactionManager()
    TM.parser("input")
    TM.wait_table[1] = [2, 4]
    TM.wait_table[2] = [3]
    TM.wait_table[3] = [1]
    TM.wait_table[4] = [3]
    # TM.wait_table[3] = [4]
    TM.deadlock_detection()
    # for t in TM.transaction_list:
    #     print(TM.transaction_list[t])
