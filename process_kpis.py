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
    processedSet = set()

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
            # check if this token was already processed, useful to filter out duplicates
            if tuple(tokenReceived) not in processedSet:
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

                        processedSet.add(tuple(tokenReceived))
                        break

    return { "latencies" : latencies }

def calculate_reliability(inputFile):
    packetSentEvents = []
    packetReceivedEvents = []
    commandSendPacketEvents = []

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
            elif candidate['event'] == 'command' and candidate['type'] == 'sendPacket':
                commandSendPacketEvents += [candidate]

    # filter out the duplicates in packetReceivedEvents
    packetReceivedTokens = []
    for packetReceivedEvent in packetReceivedEvents:
        packetReceivedTokens += [tuple(packetReceivedEvent['packetToken'])]
    packetReceivedTokens = set(packetReceivedTokens)

    # filter out the duplicates in packetSentEvents
    packetSentTokens = []
    for packetSentEvent in packetSentEvents:
        packetSentTokens += [tuple(packetSentEvent['packetToken'])]
    packetSentTokens = set(packetSentTokens)

    commandSendPacketTokens = []
    for command in commandSendPacketEvents:
        commandSendPacketTokens += [tuple(command['packetToken'])]
    commandSendPacketTokens = set(commandSendPacketTokens)

    assert commandSendPacketTokens >= packetSentTokens
    assert commandSendPacketTokens >= packetReceivedTokens
    assert packetSentTokens        >= packetReceivedTokens

    print packetSentTokens - packetReceivedTokens

    reliabilitySent = 1 - len(packetSentTokens - packetReceivedTokens) / float(len(packetSentTokens))
    reliabilityCommanded = 1 - len(commandSendPacketTokens - packetReceivedTokens) / float(len(commandSendPacketTokens))

    return {
        "reliabilitySent"       : reliabilitySent,
        "reliabilityCommanded"  : reliabilityCommanded
    }

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