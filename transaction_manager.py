import re
import transaction
import lock
import datamanager as dm


class TransactionManager:
    def __init__(self):
        # transaction_index : transaction
        self.transaction_list = {}

        # # variable_index : lock
        # self.lock_table = []
        # for i in range(0, 20):
        #     self.lock_table.append(lock.Lock())

        # transaction_index(a) : [transaction_index(b)] a wait bi
        self.transaction_wait_table = {}
        self.data_wait_table = {}

        # transaction_index(b) : [transaction_index(ai)] (sequential list) b block ai
        self.block_table = {}
        self.DM = dm.DataManager()
        self.fail_history = {}

    def parser(self, input_file):
        infile = open(input_file, 'r')
        lines = infile.readlines()
        line_num = 0
        time = 0
        for line in lines:
            for t_id in self.transaction_list:
                print(self.transaction_list[t_id])
            line_num += 1
            time += 1
            print("\n"+str(time)+">>>")
            self.deadlock_detection(time)
            # try to resurrect transaction blocked by failed site
            self.resurrect(time)
            try:
                line = line.strip('\n')
                operation = re.split('[()]', line)
                operation_name = operation[0].strip()

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
                    transaction_id = int(operation_arg[0].strip()[1:])
                    self.begin(transaction_id, time)

                elif operation_name == "beginRO":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [begin] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0].strip()[1:])
                    self.begin(transaction_id, time, ro=True)

                elif operation_name == "R":
                    if len(operation_arg) != 2:
                        errmsg = "error: operation [R] requires exactly 2 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                    transaction_id = int(operation_arg[0].strip()[1:])
                    variable_id = operation_arg[1].strip()
                    self.read(transaction_id, variable_id, time)

                elif operation_name == "W":
                    if len(operation_arg) != 3:
                        errmsg = "error: operation [W] requires exactly 3 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0].strip()[1:])
                    variable_id = operation_arg[1].strip()
                    value = int(operation_arg[2].strip())
                    self.write(transaction_id, variable_id, value)

                elif operation_name == "dump":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [dump] requires no more than 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    if len(operation_arg[0]) == 0:
                        self.dump()
                    elif operation_arg[0][0] == 'x':
                        variable_id = operation_arg[0].strip()
                        self.dump(variable=variable_id)
                    else:
                        site_id = int(operation_arg[0].strip())
                        self.dump(site=site_id)

                elif operation_name == "end":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [end] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    transaction_id = int(operation_arg[0].strip()[1:])
                    self.end(transaction_id, time)

                elif operation_name == "fail":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [fail] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    site_id = int(operation_arg[0].strip())
                    self.fail(site_id, time)

                elif operation_name == "recover":
                    if len(operation_arg) != 1:
                        errmsg = "error: operation [fail] requires exactly 1 argument, "+str(len(operation_arg))
                        errmsg += " provided in line " + str(line_num)
                        raise ValueError(errmsg)
                    site_id = int(operation_arg[0].strip())
                    self.recover(site_id)

                else:
                    errmsg = "error: can not recognize operation name: [" + operation_name
                    errmsg += "] in line " + str(line_num)
                    raise ValueError(errmsg)

            except ValueError as err:
                print(err.args)

    def begin(self, transaction_id, time, ro=False):
        msg = "begin T"+str(transaction_id)
        if ro:
            msg += " read-only"
        msg += " at "+str(time)
        print(msg)
        t = transaction.Transaction(transaction_id, time, _ro=ro)
        self.transaction_list[transaction_id] = t

    def read(self, transaction_id, variable_id, sys_time):
        msg = "T"+str(transaction_id)+" attempt to read "+str(variable_id)
        print(msg)
        ro = self.transaction_list[transaction_id].ro
        ro_version = self.transaction_list[transaction_id].ro_version
        read_result = self.DM.read(self.transaction_list[transaction_id], variable_id)
        # lock successful
        if read_result[0]:
            if not ro:
                sites_touched = set(read_result[1])
                self.transaction_list[transaction_id].touch_set |= sites_touched
                self.transaction_list[transaction_id].status = "normal"
                # variable is already locked by transaction
                if variable_id not in self.transaction_list[transaction_id].lock_list:
                    self.transaction_list[transaction_id].lock_list[variable_id] = 'r'
                if transaction_id in self.transaction_wait_table:
                    del self.transaction_wait_table[transaction_id]
        # blocked
        else:
            # cache failed:
            if ro and read_result[1] == -1:
                blocker = -1
                if transaction_id in self.transaction_wait_table:
                    self.transaction_wait_table[transaction_id].add(blocker)
                else:
                    self.transaction_wait_table[transaction_id] = set([blocker])
                if blocker in self.block_table:
                    self.block_table[blocker].append(transaction_id)
                else:
                    self.block_table[blocker] = [transaction_id]
                self.transaction_list[transaction_id].status = "read"
                self.transaction_list[transaction_id].query_buffer = [variable_id]
            # data not in cache:
            elif ro and read_result[1] == -2:
                self.abort(transaction_id)
            else:
                blockers = read_result[1]
                if blockers[0] != -1:
                    if variable_id in self.data_wait_table:
                        self.data_wait_table[variable_id].append(transaction_id)
                    else:
                        self.data_wait_table[variable_id] = [transaction_id]
                for blocker in blockers:
                    if transaction_id in self.transaction_wait_table:
                        self.transaction_wait_table[transaction_id].add(blocker)
                    else:
                        self.transaction_wait_table[transaction_id] = set([blocker])
                    if blocker in self.block_table:
                        self.block_table[blocker].append(transaction_id)
                    else:
                        self.block_table[blocker] = [transaction_id]
                self.transaction_list[transaction_id].status = "read"
                self.transaction_list[transaction_id].query_buffer = [variable_id]

    def write(self, transaction_id, variable_id, value):
        msg = "T"+str(transaction_id)+" attempt to write "+str(variable_id)+" as "+str(value)
        print(msg)
        write_result = self.DM.write(transaction_id, variable_id)
        if write_result[0]:
            print('write success!')
            sites_touched = set(write_result[1])
            self.transaction_list[transaction_id].touch_set |= sites_touched
            self.transaction_list[transaction_id].commit_list[variable_id] = value
            self.transaction_list[transaction_id].status = "normal"
            # update lock info in transaction
            self.transaction_list[transaction_id].lock_list[variable_id] = 'w'
            if transaction_id in self.transaction_wait_table:
                del self.transaction_wait_table[transaction_id]
        else:
            # print("write blocked")
            if variable_id in self.data_wait_table:
                self.data_wait_table[variable_id].append(transaction_id)
            else:
                self.data_wait_table[variable_id] = [transaction_id]
            blockers = write_result[1]
            for blocker in blockers:
                if transaction_id in self.transaction_wait_table:
                    self.transaction_wait_table[transaction_id].add(blocker)
                else:
                    self.transaction_wait_table[transaction_id] = set([blocker])
                if blocker in self.block_table:
                    self.block_table[blocker].append(transaction_id)
                else:
                    self.block_table[blocker] = [transaction_id]
            # print(transaction_id)
            self.transaction_list[transaction_id].status = "write"
            # print(self.transaction_list[transaction_id].status)
            self.transaction_list[transaction_id].query_buffer = [variable_id, value]

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

    def end(self, transaction_id, sys_time):
        msg = "end T"+str(transaction_id)
        print(msg)
        trans = self.transaction_list[transaction_id]
        sites_touched = trans.touch_set
        start_time = trans.start_time
        end_time = sys_time
        if self.validation(sites_touched, start_time, end_time):
            self.commit(transaction_id, sys_time)
        else:
            self.abort(transaction_id, sys_time)

    def fail(self, site_id, sys_time):
        msg = "site "+str(site_id)+" failed"
        print(msg)
        self.DM.fail(site_id)
        if site_id in self.fail_history:
            self.fail_history[site_id].append(sys_time)
        else:
            self.fail_history[site_id] = [sys_time]

    def recover(self, site_id):
        msg = "recover site " + str(site_id)
        print(msg)
        self.DM.recover(site_id)
    #     check if some blocked transaction can be moving forward

    def commit(self, transaction_id, sys_time):
        msg = "commit transaction "+str(transaction_id)
        print(msg)
        trans = self.transaction_list[transaction_id]
        self.DM.commit(transaction_id,trans.commit_list)
        self.release_locks(transaction_id, sys_time)
        del self.transaction_list[transaction_id]
        if transaction_id in self.transaction_wait_table:
            del self.transaction_wait_table[transaction_id]
        if transaction_id in self.block_table:
            del self.block_table[transaction_id]

    def abort(self, transaction_id, sys_time):
        msg = "abort transaction "+str(transaction_id)
        print(msg)
        trans = self.transaction_list[transaction_id]
        self.release_locks(transaction_id, sys_time)
        del self.transaction_list[transaction_id]
        if transaction_id in self.transaction_wait_table:
            del self.transaction_wait_table[transaction_id]
        if transaction_id in self.block_table:
            del self.block_table[transaction_id]
        for data in self.data_wait_table:
            for i, t_id in enumerate(self.data_wait_table[data]):
                if t_id == transaction_id:
                    del self.data_wait_table[data][i]

        for t_id in self.transaction_list:
            print(self.transaction_list[t_id])

    def deadlock_detection(self, sys_time):
        msg = "detecting deadlock"
        print(msg)
        print("transaction_wait_table : ")
        print(self.transaction_wait_table)
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
                    if visited[f] == 0 and f in self.transaction_wait_table:
                        visited[f] = 1
                        ghost_transaction_list = []
                        for c in self.transaction_wait_table[f]:
                            if c != -1 and c not in self.transaction_list:
                                ghost_transaction_list.append(c)
                        for ghost_transaction in ghost_transaction_list:
                            self.transaction_wait_table[f].remove(ghost_transaction)
                        for c in self.transaction_wait_table[f]:
                            if c == -1:
                                continue
                            if visited[c] == 1:
                                print("There's a circle. Let the killing begin")
                                cur = c
                                youngest_transaction = f
                                while cur != f:
                                    if self.transaction_list[cur].start_time > self.transaction_list[youngest_transaction].start_time:
                                        youngest_transaction = cur
                                    for next_trans in self.transaction_wait_table[cur]:
                                        if visited[next_trans] == 1:
                                            cur = next_trans
                                print("Prey located, let's sacrifice transaction "+str(youngest_transaction))
                                self.abort(youngest_transaction, sys_time)
                            elif visited[c] == 0:
                                stack.append(c)
                    else:
                        visited[f] = 2
                        stack.pop()

    def release_locks(self, transaction_id, sys_time):
        msg = "release lock hold by T"+str(transaction_id)+" and give them to other blocked transactions"
        print(msg)
        locks = self.transaction_list[transaction_id].lock_list
        free_datas = self.DM.releaseLocks(transaction_id, locks)
        msg = "we are freed now! :"
        for fd in free_datas:
            msg += str(fd)
        print(msg)
        print("data_wait_table:")
        print(self.data_wait_table)
        retry_list = []
        for free_data in free_datas:
            if free_data in self.data_wait_table:
                for tid in self.data_wait_table[free_data]:
                    if tid not in retry_list:
                        retry_list.append(tid)
        for free_data in free_datas:
            if free_data in self.data_wait_table:
                del self.data_wait_table[free_data]
        for tid in retry_list:
            self.retry(tid)
        # for free_data in free_datas:
        #     # if free_data in self.data_wait_table:
        #     if free_data in self.data_wait_table and self.data_wait_table[free_data]:
        #         # some transaction(s) is/are waiting for this data to be freed
        #         next_transaction = self.data_wait_table[free_data][0]
        #         # print(self.transaction_list[next_transaction].status)
        #         if self.transaction_list[next_transaction].status == "write":
        #             value = self.transaction_list[next_transaction].query_buffer[1]
        #             self.write(next_transaction, free_data, value)
        #             self.transaction_list[next_transaction].status = "normal"
        #             del self.data_wait_table[free_data][0]
        #         elif self.transaction_list[next_transaction].status == "read":
        #             while self.data_wait_table[free_data] and self.transaction_list[next_transaction].status == 'read':
        #                 self.read(next_transaction, free_data, sys_time)
        #                 self.transaction_list[next_transaction].status = "normal"
        #                 del self.data_wait_table[free_data][0]
        #         # if there's no anyone else waiting for this free data
        #         if not self.data_wait_table[free_data]:
        #             del self.data_wait_table[free_data]

    def resurrect(self, sys_time):
        msg = "resurrect transactions blocked by failed site"
        print(msg)
        if -1 in self.block_table:
            for i, trans_id in enumerate(self.block_table[-1]):
                if self.transaction_list[trans_id].status == "read":
                    variable_id = self.transaction_list[trans_id].query_buffer[0]
                    del self.block_table[-1][i]
                    self.read(trans_id, variable_id, sys_time)
                else:
                    variable_id = self.transaction_list[trans_id].query_buffer[0]
                    value = self.transaction_list[trans_id].query_buffer[0]
                    del self.block_table[-1][i]
                    self.write(trans_id, variable_id, value)

    def validation(self, sites_touched, start_time, end_time):
        for site in sites_touched:
            if site in self.fail_history:
                for fail_time in self.fail_history[site]:
                    if start_time < fail_time < end_time:
                        return False
        return True

    def retry(self, transaction_id):
        trans = self.transaction_list[transaction_id]
        if self.transaction_list[transaction_id].status == "read":
            self.read(transaction_id, trans.query_buffer[0])
        elif self.transaction_list[transaction_id].status == "write":
            self.write(transaction_id, trans.query_buffer[0], trans.query_buffer[1])


if __name__ == "__main__":
    TM = TransactionManager()
    TM.parser("input")
    # TM.deadlock_detection(999)
    print(TM.block_table)
    print(TM.transaction_wait_table)
