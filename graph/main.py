
from model.pollsData import PollsData
import service.graphing as Graphing

def main():
    polls = PollsData()
    polls.cleanData()
    data = polls.getDataframe()
    Graphing.buildGraph(data)
    new_data = Graphing.addMetricsToFrame(data)
    polls.setDataframe(new_data)
    polls.prediction()
    polls.output("result")
    

if __name__ == "__main__":
    main()