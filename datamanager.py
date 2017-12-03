class DataManager:
    def __init__(self):
        pass

    def dump(self, site=None, variable=None):
        print("DM phase:")
        if site is None and variable is None:
            msg = "dump all data"
            print(msg)
        elif site is None:
            msg = "dump data x"+str(variable)+" from all site"
            print(msg)
        else:
            msg = "dump data on site "+str(site)
            print(msg)

    def recover(self, site_id):
        pass

    def fail(self, site_id):
        pass

    # return either (success, read_from_site) or (fail, blocker)
    def read(self, transaction_id, variable_id, ro, version, sys_time):
        success = False
        if success:
            return True, [10]
        else:
            return False, [1, 2]

    # if success return (True, write_from_site)
    # else return (False, blocker)
    # (blocker = -1 means all sites storing that variable is failed)
    def write(self, transaction_id, variable_id, value):
        success = (value == 101 or value == 202)
        if success:
            return True, [1]
        else:
            if transaction_id == 1:
                return False, [2]
            else:
                return False, [1]

    # return True if no site in sites have failed during [start_time, end_time]
    def validation(self, sites, start_time, end_time):
        success = True
        if success:
            return True
        else:
            return False

    # new_data is a dict
    # eg: 1:102, 2:300, 7:50
    # means update value of x1 to 102, x2 to 300 and x7 to 50
    def commit(self, new_data):
        pass

    # locks is a dict
    # eg:   transaction_id = 2  locks = {3:'w', 5,'r'}
    # mean release T2's write lock on x3 and read lock on x5
    def release_locks(self, transaction_id, locks):
        return [3, 5]

