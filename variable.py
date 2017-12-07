# author    : Lei Guo
# date      : Dec. 7, 2017
# Lock class simply tracks the lock information for a variable
# includes a lockDict which records who (transactionID) hold which type of lock.

class Variable:
    def __init__(self, num):
        self.Idx = num
        self.ID = 'x' + str(num)
        self.data = num * 10

    # getter s and setters
    def getID(self):
        return self.ID

    def getData(self):
        return self.data

    def setData(self, newData):
        self.data = newData
