import argparse
import json

parser = argparse.ArgumentParser()

parser.add_argument('--log',
                    dest='log',
                    required=True,
                    action='store',
                    )


def calculate_latency(inputFile):
    packetSentEvents = []
    packetReceivedEvents = []
    latencies = []

    with open(inputFile, "r") as f:
        headerLine = json.loads(f.readline())

        print "Processing latency for experiment {0} executed on {1}".format(headerLine['experimentId'], headerLine['date'])

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

                    assert packetSent['source'] == packetReceived['destination']
                    assert packetSent['destination'] == packetReceived['source']

                    latencies += [ {
                        "source" : packetSent['source'],
                        "destination" : packetSent['destination'],
                        "latency" : packetReceived['timestamp'] - packetSent['timestamp'],
                        "hops"    : packetSent['hopLimit'] - packetReceived['hopLimit'],
                    } ]
                    break

    return { "latencies" : latencies }

def calculate_reliability(inputFile):
    packetSentEvents = []
    packetReceivedEvents = []

    with open(inputFile, "r") as f:
        headerLine = json.loads(f.readline())

        print "Processing reliability for experiment {0} executed on {1}".format(headerLine['experimentId'],
                                                                             headerLine['date'])

        # first fetch the events of interest
        for line in f:
            candidate = json.loads(line)

            # filter out the events of interest
            if candidate['event'] == 'packetSent':
                packetSentEvents += [candidate]
            elif candidate['event'] == 'packetReceived':
                packetReceivedEvents += [candidate]

    reliability = len(packetReceivedEvents) / float(len(packetSentEvents))

    return {"reliability": reliability}

def calculate_join_times(inputFile):
    joinInstants = []


    with open(inputFile, "r") as f:
        headerLine = json.loads(f.readline())

        print "Processing join times for experiment {0} executed on {1}".format(headerLine['experimentId'],
                                                                             headerLine['date'])

        # first fetch the events of interest
        for line in f:
            candidate = json.loads(line)

            # filter out the events of interest
            if candidate['event'] == 'secureJoinCompleted':
                joinInstants += [candidate['timestamp']]

    return {"joinInstants": joinInstants}

def calculate_sync_times(inputFile):
    syncInstants = []


    with open(inputFile, "r") as f:
        headerLine = json.loads(f.readline())

        print "Processing sync times for experiment {0} executed on {1}".format(headerLine['experimentId'],
                                                                             headerLine['date'])

        # first fetch the events of interest
        for line in f:
            candidate = json.loads(line)

            # filter out the events of interest
            if candidate['event'] == 'synchronizationCompleted':
                syncInstants += [candidate['timestamp']]

    return {"syncInstants": syncInstants}

def main():

    args = parser.parse_args()
    inputFile = args.log
    outputFile = args.log + ".kpi"

    kpis = {}

    kpis.update(calculate_latency(inputFile))
    kpis.update(calculate_reliability(inputFile))
    kpis.update(calculate_join_times(inputFile))
    kpis.update(calculate_sync_times(inputFile))

    with open(outputFile, "w") as f:
        json.dump(kpis, f)
        f.write('\n')


if __name__ == '__main__':
    main()