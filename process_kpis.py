import argparse
import json

parser = argparse.ArgumentParser()

parser.add_argument('--log',
                    dest='log',
                    required=True,
                    action='store',
                    )


def calculate_latency(inputFile, outputFile):
    packetSentEvents = []
    packetReceivedEvents = []
    latencies = []

    with open(inputFile, "r") as f:
        headerLine = json.loads(f.readline())

        print "Processing KPIs for experiment {0} executed on {1}".format(headerLine['experimentId'], headerLine['date'])

        # first fetch the events of interest
        for line in f:
            candidate = json.loads(line)

            # filter out the events of interest
            if candidate['event'] == 'packetSent':
                packetSentEvents += [candidate]
            elif candidate['event']  == 'packetReceived':
                packetReceivedEvents += [candidate]

        # match the events according to the token
        for packetReceived in packetReceivedEvents:
            tokenReceived = packetReceived['packetToken']
            for packetSent in packetSentEvents:
                if packetSent['packetToken'] == tokenReceived:
                    latencies += [ {
                        "source" : packetSent['source'],
                        "destination" : packetSent['destination'],
                        "latency" : packetReceived['timestamp'] - packetSent['timestamp'],
                        "hops"    : packetSent['hopLimit'] - packetReceived['hopLimit'],
                    } ]
                    break

        #print latencies

    #with open(outputFile, "a") as f:
        # dump calculated latencies
    #    pass
    #    f.write('\n')

def calculate_reliability(inputFile, outputFile):
    pass

def main():

    args = parser.parse_args()
    inputFile = args.log
    outputFile = args.log + ".kpi"
    calculate_latency(inputFile, outputFile)
    calculate_reliability(inputFile, outputFile)

if __name__ == '__main__':
    main()