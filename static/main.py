
from model.pollsData import PollsData

def main():
    polls = PollsData()
    polls.cleanData()
    polls.prediction()
    polls.output("result")


if __name__ == "__main__":
    main()