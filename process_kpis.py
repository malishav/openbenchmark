import argparse
import json
import numpy
import os
import glob

parser = argparse.ArgumentParser()

parser.add_argument('--logDir',
                    dest='logDir',
                    required=True,
                    action='store',
                    )
def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)

def calculate_latency(inputDir):
    latencies = {}
    returnDict = {}
    lenProcessed = 0

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):
        processedSet = set()
        packetSentEvents = []
        packetReceivedEvents = []

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

                            hopsTraversed = packetSent['hopLimit'] - packetReceived['hopLimit'] + 1
                            latency = packetReceived['timestamp'] - packetSent['timestamp']

                            if hopsTraversed in latencies:
                                latencies[hopsTraversed] += [latency]
                            else:
                                latencies[hopsTraversed] = [latency]

                            processedSet.add(tuple(tokenReceived))
                            break
            lenProcessed += len(processedSet)

    for key in latencies:
        returnDict['{0}_hop_latency'.format(key)] = {
                                                     'mean' : mean(latencies[key]),
                                                     'min' : min(latencies[key]),
                                                     'max' : max(latencies[key]),
                                                     '99%' : numpy.percentile(latencies[key], 99)
                                                     }

    print "Number of packets received: {0}".format(lenProcessed)
    print returnDict
    return returnDict

def calculate_reliability(inputDir):
    packetSentEvents = []
    packetReceivedEvents = []
    commandSendPacketEvents = []

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):
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

    reliabilitySent = 1 - len(packetSentTokens - packetReceivedTokens) / float(len(packetSentTokens))
    reliabilityCommanded = 1 - len(commandSendPacketTokens - packetReceivedTokens) / float(len(commandSendPacketTokens))

    returnDict = {
        "reliabilitySent"       : reliabilitySent,
        "reliabilityCommanded"  : reliabilityCommanded
    }

    print "Number of packets commanded: {0}".format(len(commandSendPacketTokens))
    print returnDict

    return returnDict

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
    inputDir = args.logDir
    outputFile = os.path.join(args.logDir,  args.logDir.strip('/') + ".kpi")

    kpis = {}

    kpis.update(calculate_latency(inputDir))
    kpis.update(calculate_reliability(inputDir))
    #kpis.update(calculate_join_times(inputDir))
    #kpis.update(calculate_sync_times(inputDir))

    with open(outputFile, "w") as f:
        json.dump(kpis, f, indent=4)
        f.write('\n')


if __name__ == '__main__':
    main()