class Variable:
    def __init__(self, num):
        self.Idx = num
        self.ID = 'x' + str(num)
        self.data = num * 10

    def getID(self):
        return self.ID

    def getIdx(self):
        return self.Idx

    def getData(self):
        return self.data

    def setData(self, newData):
        self.data = newData

    def setID(self, newID):
        self.ID = newID
