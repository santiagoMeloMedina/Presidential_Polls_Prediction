
from datetime import datetime
import constant.graph as GRAPH

class Node:
    def __init__(self, var, state, modeldate, winstate_inc, winstate_chal, **kwargs):
        winstate_inc, winstate_chal = float(winstate_inc), float(winstate_chal)
        self.var = var
        self.state = state
        self.date = datetime.strptime(modeldate, '%m/%d/%Y').date()
        self.winner = 1 if winstate_inc > winstate_chal else 2
    
    def getVar(self):
        return self.var
    
    def getState(self):
        return self.state

    def getDate(self):
        return self.date

    def getWinner(self):
        return self.winner