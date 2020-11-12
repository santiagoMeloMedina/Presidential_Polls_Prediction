
import service.cleaning as cleaning
from constant.data import COLS
from configuration.connection import spark
import service.prediction as prediction
from constant.data import DIVISION
import json
import constant.message as MSG

class PollsData:
    def __init__(self, dataframe):
        self.df = dataframe
        self.models = 1
        self.group = {"INC": {}, "CHAL": {}}
        self.states = []
        self.result = {"INC": {}, "CHAL": {}}
        self.progress = {"current": 0, "limit": 0}
    
    def setDataframe(self, df):
        self.df = df
        return self
    
    def getDataframe(self):
        return self.df
    
    def advanceUnitProgress(self):
        """ This function calculates an advance on the prossesing progress and display an 
            equivalent loading bar """
        size, barSize = 100, 40
        self.progress["current"] += 1
        scaled = size/self.progress["limit"]*self.progress["current"]
        bar = barSize/size*scaled
        print("[{}] {:.2f}%".format(int(bar)*'#'+' '*(barSize-int(bar)), scaled), end="\r")
    
    def cleanData(self):
        """ This function deliver the dataframe to the cleaning function and stores the 
            resulting structure for the prediction model processing """
        incumbent, challenger = cleaning.dataCleaning(self.df)
        ## States in the dataframe are stored with the size of data available for each one
        self.states = incumbent.groupBy("state").count().collect()
        for state in self.states:
            self.group["INC"][state["state"]] = incumbent.filter(incumbent.state==state["state"])
            self.group["CHAL"][state["state"]] = challenger.filter(challenger.state==state["state"])
        ## Limit for progress bar is stablished
        self.progress["limit"] = len(self.states)*len(DIVISION["PREDICTION"])*self.models
        return self

    def prediction(self):
        """ This function outputs stores the results from the prediction models according to the 
            structure defined """
        for div in DIVISION["PREDICTION"]:
            data = DIVISION["PREDICTION"][div]
            for state in self.states:
                df = self.group[div][state["state"]]
                isotonic = prediction.isotonicRegression(df, data["LABEL"], data["FEATURES"], data["ADJUST"])
                self.advanceUnitProgress()
                models = [(isotonic, "isotonic")]
                comparison = prediction.compareResult(models)
                self.result[div][state["state"]] = {
                    "models": {
                        "isotonic": isotonic
                    },
                    "comparison": comparison
                }
        print(MSG.FINISHED_LOADING)
        return self
    
    def output(self, name):
        """ This function the results stored from the prediction in a JSON file """
        data = json.dumps(self.result, indent = 4)
        file = open("output/{}.json".format(name), 'w')
        file.write(str(data))
        file.close()
        return self