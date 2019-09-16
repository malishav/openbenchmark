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
            print "Processing {0}".format(inputFile)
            headerLine = json.loads(f.readline())

            # root is by convention always openbenchmark00 node
            root = headerLine['nodes']['openbenchmark00']['eui64']

            print "Processing latency for experiment {0} executed on {1}. Root eui64={2}.".format(headerLine['experimentId'], headerLine['date'], root)

            # first fetch the events of interest
            for line in f:
                candidate = json.loads(line)

                # filter out the events of interest: for upstream latency we care only about packets destined for the root
                if candidate['event'] == 'packetSent' and candidate['destination'] == root:
                    packetSentEventsUpstream += [candidate]
                elif candidate['event']  == 'packetReceived' and candidate['source'] == root:
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
    reliabilitySentUpstream = []
    reliabilitySentDownstream = []
    reliabilitySentP2P = []
    reliabilitySerialPort = []
    returnDict = {}

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):

        packetSentTokensUpstream = set()
        packetReceivedTokensUpstream = set()
        packetSentTokensDownstream = set()
        packetReceivedTokensDownstream = set()
        packetSentTokensP2P = set()
        packetReceivedTokensP2P = set()
        commandSendPacketTokens = set()

        with open(inputFile, "r") as f:
            headerLine = json.loads(f.readline())

            # root is by convention always openbenchmark00 node
            root = headerLine['nodes']['openbenchmark00']['eui64']

            print "Processing reliability for experiment {0} executed on {1}. Root eui64={2}".format(headerLine['experimentId'],
                                                                                 headerLine['date'], root)
            # first fetch the events of interest
            for line in f:
                candidate = json.loads(line)

                # filter out the events of interest
                if candidate['event'] == 'packetSent':
                    token = tuple(candidate['packetToken'])
                    if candidate['destination'] == root:
                        # upstream packet
                        packetSentTokensUpstream.add(token)
                    elif candidate['source'] == root:
                        # downstream packet
                        packetSentTokensDownstream.add(token)
                    else:
                        # P2P packet
                        packetSentTokensP2P.add(token)

                elif candidate['event'] == 'packetReceived':
                    token = tuple(candidate['packetToken'])
                    if candidate['source'] == root:
                        # upstream packet
                        packetReceivedTokensUpstream.add(token)
                    elif candidate['destination'] == root:
                        # downstream packet
                        packetReceivedTokensDownstream.add(token)
                    else:
                        # P2P packet
                        packetReceivedTokensP2P.add(token)

                elif candidate['event'] == 'command' and candidate['type'] == 'sendPacket':

                    token = tuple(candidate['packetToken'])
                    commandSendPacketTokens.add(token)

        reliabilitySentUpstream     += [1 - len(packetSentTokensUpstream - packetReceivedTokensUpstream) / float(len(packetSentTokensUpstream))] if len(packetSentTokensUpstream) else []
        reliabilitySentDownstream   += [1 - len(packetSentTokensDownstream - packetReceivedTokensDownstream) / float(len(packetSentTokensDownstream))] if len(packetSentTokensDownstream) else []
        reliabilitySentP2P          += [1 - len(packetSentTokensP2P - packetReceivedTokensP2P) / float(len(packetSentTokensP2P))] if len(packetSentTokensP2P) else []

        reliabilitySerialPort        += [1 - len(commandSendPacketTokens - (packetSentTokensUpstream | packetSentTokensDownstream | packetSentTokensP2P)) / float(len(commandSendPacketTokens))]

        print "Experiment {0} Number of packets commanded: {1}".format(headerLine['experimentId'], len(commandSendPacketTokens))


    if len(reliabilitySentUpstream):
        returnDict['reliabilitySentUpstream'] = {
                                            'mean' : mean(reliabilitySentUpstream),
                                            'min' : min(reliabilitySentUpstream),
                                            'max' : max(reliabilitySentUpstream),
                                            '99%' : numpy.percentile(reliabilitySentUpstream, 99)
                                        }
    if len(reliabilitySentDownstream):
        returnDict['reliabilitySentDownstream'] = {
                                            'mean' : mean(reliabilitySentDownstream),
                                            'min' : min(reliabilitySentDownstream),
                                            'max' : max(reliabilitySentDownstream),
                                            '99%' : numpy.percentile(reliabilitySentDownstream, 99)
                                        }

    if len(reliabilitySentP2P):
        returnDict['reliabilitySentP2P'] = {
                                            'mean' : mean(reliabilitySentP2P),
                                            'min' : min(reliabilitySentP2P),
                                            'max' : max(reliabilitySentP2P),
                                            '99%' : numpy.percentile(reliabilitySentP2P, 99)
                                        }

    if len(reliabilitySerialPort):
        returnDict['reliabilitySerialPort'] = {
                                            'mean' : mean(reliabilitySerialPort),
                                            'min' : min(reliabilitySerialPort),
                                            'max' : max(reliabilitySerialPort),
                                            '99%' : numpy.percentile(reliabilitySerialPort, 99)
                                        }

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