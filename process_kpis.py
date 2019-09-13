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
    # indexed by number of hops
    latenciesUpstream = {}

    returnDict = {}

    # total number of processed upstream packets
    lenProcessedUpstream = 0

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):
        processedSetUpstream = set()
        processedSetDownstream = set()
        processedSetP2P = set()
        packetSentEventsUpstream = []
        packetReceivedEventsUpstream = []
        packetSentEventsDownstream = []
        packetReceivedEventsDownstream = []
        packetSentEventsP2P = []
        packetReceivedEventsP2P = []

        with open(inputFile, "r") as f:
            headerLine = json.loads(f.readline())

            # root is by convention always openbenchmark00 node
            root = headerLine['nodes']['openbenchmark00']['eui64']
            print root

            print "Processing latency for experiment {0} executed on {1}".format(headerLine['experimentId'], headerLine['date'])

            # first fetch the events of interest
            for line in f:
                candidate = json.loads(line)

                # filter out the events of interest: for upstream latency we care only about packets destined for the root
                if candidate['event'] == 'packetSent' and candidate['destination'] == root:
                    packetSentEventsUpstream += [candidate]
                elif candidate['event']  == 'packetReceived' and candidate['source' == root]:
                    packetReceivedEventsUpstream += [candidate]
                # for downstream, we care about packets originated by the root
                elif candidate['event'] == 'packetSent' and candidate['source'] == root:
                    packetSentEventsDownstream += [candidate]
                elif candidate['event'] == 'packetReceived' and candidate['destination'] == root:
                    packetReceivedEventsDownstream += [candidate]
                # anything else is peer to peer
                elif candidate['event'] == 'packetSent':
                    packetSentEventsP2P += [candidate]
                elif candidate['event'] == 'packetReceived':
                    packetReceivedEventsP2P += [candidate]

            # match the events according to the token
            for packetReceived in packetReceivedEventsUpstream:
                tokenReceived = packetReceived['packetToken']
                # check if this token was already processed, useful to filter out duplicates
                if tuple(tokenReceived) not in processedSetUpstream:
                    for packetSent in packetSentEventsUpstream:
                        if packetSent['packetToken'] == tokenReceived:

                            assert packetSent['source'] == packetReceived['destination']
                            assert packetSent['destination'] == packetReceived['source']

                            hopsTraversed = packetSent['hopLimit'] - packetReceived['hopLimit'] + 1
                            latency = packetReceived['timestamp'] - packetSent['timestamp']

                            if hopsTraversed in latenciesUpstream:
                                latenciesUpstream[hopsTraversed] += [latency]
                            else:
                                latenciesUpstream[hopsTraversed] = [latency]

                            processedSetUpstream.add(tuple(tokenReceived))
                            break
            lenProcessedUpstream += len(processedSetUpstream)

            # TODO Downstream Latencies

            # TODO P2P Latencies

    for key in latenciesUpstream:
        returnDict['{0}_hop_latency_upstream'.format(key)] = {
                                                     'mean' : mean(latenciesUpstream[key]),
                                                     'min' : min(latenciesUpstream[key]),
                                                     'max' : max(latenciesUpstream[key]),
                                                     '99%' : numpy.percentile(latenciesUpstream[key], 99)
                                                     }

    print "Number of upstream packets processed: {0}".format(lenProcessedUpstream)
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

def calculate_join_times(inputDir):
    maxJoinTimes = []

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):

        joinDict = {}

        with open(inputFile, "r") as f:
            headerLine = json.loads(f.readline())

            print "Processing join times for experiment {0} executed on {1}".format(headerLine['experimentId'],
                                                                                 headerLine['date'])

            # first fetch the events of interest
            for line in f:
                candidate = json.loads(line)

                # filter out the events of interest
                if candidate['event'] == 'secureJoinCompleted':
                    source = candidate['source']

                    if source not in joinDict:
                        joinDict[source] = candidate['timestamp']
                    else:
                        # node must have rebooted, skip any but the first join instant
                        pass

        print "Number of join instants: {0}".format(len(joinDict.values()))
        maxJoinTimes += [max(joinDict.values())] if len(joinDict.values()) != 0 else []

    return {
        "joinTime" : {
                        'mean' : mean(maxJoinTimes),
                        'min'  : min(maxJoinTimes),
                        'max'  : max(maxJoinTimes),
                        '99%'  : numpy.percentile(maxJoinTimes, 99)
                    }
        }

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
    kpis.update(calculate_join_times(inputDir))
    #kpis.update(calculate_sync_times(inputDir))

    with open(outputFile, "w") as f:
        json.dump(kpis, f, indent=4)
        f.write('\n')


if __name__ == '__main__':
    main()