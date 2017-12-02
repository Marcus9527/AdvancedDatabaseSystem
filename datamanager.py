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

    # return either (success, read_from_site) or (fail, blocked_by)
    def read(self, transaction_id, variable_id, ro):
        success = False
        if success:
            return True, [10]
        else:
            return False, [5]

    def write(self, transaction_id, variable_id, value):
        success = False
        if success:
            return True, [7]
        else:
            return False, [5]

    # return True if no site in sites have failed during [start_time, end_time]
    def validation(self, sites, start_time, end_time):
        success = True
        if success:
            return True
        else:
            return False

    def commit(self, new_data):
        pass

