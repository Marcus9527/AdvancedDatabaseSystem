# author    : Songnan Zhang
# date      : Dec. 7, 2017
# TransactionManager class: parsing input operations,
#                           delivering data/site related operations to DataManager,
#                           checking and dealing with deadlocks
#                           delivering read/write results to user

import re
import transaction
import datamanager as dm


# TransactionManager class
class TransactionManager:
    def __init__(self):
        self.DM = dm.DataManager()

        # transaction_index : transaction
        self.transaction_list = {}

        # transaction_index(a) : [transaction_index(b)] transaction a waits b1,b2, ..., bi
        self.transaction_wait_table = {}

        # data(x) : [transaction_index(a)] transaction a1, a2,..., ai are waiting for locking data x
        self.data_wait_table = {}

        # transaction_index(b) : [transaction_index(ai)] (sequential list) b blocks a1, a2, ..., ai
        self.block_table = {}

        # site fail time
        self.fail_history = {}

        # final result for each transaction (commit/abort)
        self.final_result = {}

        # final committed value for each data
        self.commit_summary = {}

    # read and parse input files
    def parser(self, input_file):
        infile = open(input_file, 'r')
        lines = infile.readlines()
        line_num = 0
        time = 0
        for line in lines:
            line_num += 1
            time += 1
            print("\n"+str(time)+">>>")
            self.deadlock_detection(time)
            self.resurrect(time)
            try:
                line = line.strip('\n')
                if line[0] in "/#'\"":
                    continue
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

                self.print_status()

            except ValueError as err:
                print(err.args)

    # create new transaction
    def begin(self, transaction_id, time, ro=False):
        msg = "begin T"+str(transaction_id)
        if ro:
            msg += "(read-only)"
        msg += " @ tick "+str(time)
        print(msg)
        t = transaction.Transaction(transaction_id, time, _ro=ro)
        self.transaction_list[transaction_id] = t

    # attempt to read
    # if DM.read return success, read will print the value
    # if it is the first read of a read-only transaction, current database will be cached
    # if DM.read return fail, data_wait_table, transaction_wait_table and block_table will be updated
    def read(self, transaction_id, variable_id, sys_time):
        msg = "T"+str(transaction_id)+" attempt to read "+str(variable_id)
        print(msg)
        ro = self.transaction_list[transaction_id].ro
        read_result = self.DM.read(self.transaction_list[transaction_id], variable_id)
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

    # attempt to write
    # if DM.write return success, value to be write will be maintained in transaction's commit list
    # if DM.write return fail, data_wait_table, transaction_wait_table and block_table will be updated
    def write(self, transaction_id, variable_id, value):
        msg = "T"+str(transaction_id)+" attempt to write "+str(variable_id)+" as "+str(value)
        print(msg)
        write_result = self.DM.write(transaction_id, variable_id)
        if write_result[0]:
            sites_touched = set(write_result[1])
            self.transaction_list[transaction_id].touch_set |= sites_touched
            self.transaction_list[transaction_id].commit_list[variable_id] = value
            self.transaction_list[transaction_id].status = "normal"
            self.transaction_list[transaction_id].lock_list[variable_id] = 'w'
            if transaction_id in self.transaction_wait_table:
                del self.transaction_wait_table[transaction_id]
        else:
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
            self.transaction_list[transaction_id].status = "write"
            self.transaction_list[transaction_id].query_buffer = [variable_id, value]

    # call corresponding DM.dump
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

    # call validation to check if transaction should be committed or aborted
    def end(self, transaction_id, sys_time):
        msg = "end T"+str(transaction_id)
        print(msg)
        trans = self.transaction_list[transaction_id]
        sites_touched = trans.touch_set
        start_time = trans.start_time
        end_time = sys_time
        if self.transaction_list[transaction_id].abort:
            self.abort(transaction_id, sys_time)
        else:
            self.commit(transaction_id, sys_time)

    # call DM.fail and maintain fail_history
    def fail(self, site_id, sys_time):
        msg = "site "+str(site_id)+" failed"
        print(msg)
        self.DM.fail(site_id)
        if site_id in self.fail_history:
            self.fail_history[site_id].append(sys_time)
        else:
            self.fail_history[site_id] = [sys_time]
        for transaction_id in self.transaction_list:
            if site_id in self.transaction_list[transaction_id].touch_set:
                self.transaction_list[transaction_id].abort = True

    # call DM.recover
    def recover(self, site_id):
        msg = "recover site " + str(site_id)
        print(msg)
        self.DM.recover(site_id)

    # TM.end decide transaction should be committed, call DM.commit and release locks locked by transaction
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
        for var in trans.commit_list:
            self.commit_summary[var] = trans.commit_list[var]
        self.final_result[transaction_id] = "commit"

    # TM.end decide transaction should be aborted, release locks locked by transaction
    def abort(self, transaction_id, sys_time):
        msg = "abort transaction "+str(transaction_id)
        print(msg)
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
        self.final_result[transaction_id] = "abort"

    # check for deadlock, abort the youngest transaction if there is a deadlock
    def deadlock_detection(self, sys_time):
        msg = "detecting deadlock @ tick "+str(sys_time)
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

    # release lock after a transaction commits or aborts, and give newly freed data to transactions in data_wait_list
    def release_locks(self, transaction_id, sys_time):
        msg = "release lock hold by T"+str(transaction_id)+" and give them to other blocked transactions"
        print(msg)
        locks = self.transaction_list[transaction_id].lock_list
        free_datas = self.DM.releaseLocks(transaction_id, locks)
        msg = "newly freed data:"
        for fd in free_datas:
            msg += " "+str(fd)
        print(msg)
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
            self.retry(tid, sys_time)

    # check if any transaction blocked due to site failure can be proceeded
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

    # check if a transaction should be aborted due to site failure
    def validation(self, sites_touched, start_time, end_time):
        for site in sites_touched:
            if site in self.fail_history:
                for fail_time in self.fail_history[site]:
                    if start_time < fail_time < end_time:
                        return False
        return True

    # retry blocked transaction
    def retry(self, transaction_id, sys_time):
        trans = self.transaction_list[transaction_id]
        if self.transaction_list[transaction_id].status == "read":
            self.read(transaction_id, trans.query_buffer[0], sys_time)
        elif self.transaction_list[transaction_id].status == "write":
            self.write(transaction_id, trans.query_buffer[0], trans.query_buffer[1])

    # print TM info
    def print_status(self):
        print("transaction_wait_table : ", self.transaction_wait_table.__str__())
        print("block_table            : ", self.block_table.__str__())
        print("data_wait_table        : ", self.data_wait_table.__str__())
        for t_id in self.transaction_list:
            print(self.transaction_list[t_id])

    # print final TM info
    def print_final_status(self):
        print("\n[summary]")
        for transaction_id in self.final_result:
            print("T"+str(transaction_id)+" :", self.final_result[transaction_id])
        for var in self.commit_summary:
            print(var, "final value: ", self.commit_summary[var])
