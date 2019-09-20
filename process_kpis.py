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

def _match_packets_and_calculate_latency(packetSentEvents, packetReceivedEvents, direction=''):
    # indexed by number of hops
    latencies = {}
    latencies['average'] = []
    processedSet = set()
    hopsTraversedSum = 0
    returnDict = {}

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

                    key = "{0}hop".format(hopsTraversed)
                    if key in latencies:
                        latencies[key] += [latency]
                    else:
                        latencies[key] = [latency]

                    latencies['average'] += [latency]

                    hopsTraversedSum += float(hopsTraversed)

                    processedSet.add(tuple(tokenReceived))
                    break

    assert len(processedSet) == len(latencies['average'])

    for key in latencies:
        if len(latencies[key]):
            returnDict['latency_{0}_{1}'.format(direction, key)] = mean(latencies[key])

    if len(latencies['average']):
        # hops traversed per packet is the sum of all hops each packet traversed divided by total number of packets
        returnDict['hopsTraversedPerPacket_{0}'.format(direction)] = hopsTraversedSum / float(len(latencies['average']))

    return returnDict

def calculate_latency(inputDir):
    # indexed by number of hops
    latenciesUpstream = {}

    returnDict = {}

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):

        # filtered lines of interest
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


        upstreamDictPerRun      = _match_packets_and_calculate_latency(packetSentEventsUpstream, packetReceivedEventsUpstream, 'upstream')
        downstreamDictPerRun    = _match_packets_and_calculate_latency(packetSentEventsDownstream, packetReceivedEventsDownstream, 'downstream')
        P2PDictPerRun           = _match_packets_and_calculate_latency(packetSentEventsP2P, packetReceivedEventsP2P, 'P2P')

        for key in upstreamDictPerRun:
            if key in returnDict:
                returnDict[key] += [upstreamDictPerRun[key]]
            else:
                returnDict[key] = [upstreamDictPerRun[key]]

        for key in downstreamDictPerRun:
            if key in returnDict:
                returnDict[key] += [downstreamDictPerRun[key]]
            else:
                returnDict[key] = [downstreamDictPerRun[key]]

        for key in P2PDictPerRun:
            if key in returnDict:
                returnDict[key] += [P2PDictPerRun[key]]
            else:
                returnDict[key] = [P2PDictPerRun[key]]

    for key in returnDict:
        returnDict[key] =                 {
                                                     'mean'         : mean(returnDict[key]),
                                                     'min'          : min(returnDict[key]),
                                                     'max'          : max(returnDict[key]),
                                                     '99%'          : numpy.percentile(returnDict[key], 99),
                                                      'numRuns'  : len(returnDict[key])
                                                     }

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

        ru = [1 - len(packetSentTokensUpstream - packetReceivedTokensUpstream) / float(len(packetSentTokensUpstream))] if len(packetSentTokensUpstream) else []
        rd = [1 - len(packetSentTokensDownstream - packetReceivedTokensDownstream) / float(len(packetSentTokensDownstream))] if len(packetSentTokensDownstream) else []
        rp2p = [1 - len(packetSentTokensP2P - packetReceivedTokensP2P) / float(len(packetSentTokensP2P))] if len(packetSentTokensP2P) else []
        rs = [1 - len(commandSendPacketTokens - (packetSentTokensUpstream | packetSentTokensDownstream | packetSentTokensP2P)) / float(len(commandSendPacketTokens))]

        reliabilitySentUpstream     += ru
        reliabilitySentDownstream   += rd
        reliabilitySentP2P          += rp2p
        reliabilitySerialPort        += rs

        print "Experiment: {0}, root_eui64: {1} number of packets commanded: {2}, reliabilitySentUpstream: {3}, reliabilitySentDownstream: {4}, reliabilitySentP2P: {5}, reliabilitySerialPort: {6}".format(headerLine['experimentId'],root, len(commandSendPacketTokens), ru, rd, rp2p, rs)

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

def calculate_duty_cycle(inputDir):

    maximalDutyCyclePerRun = []
    minimalDutyCyclePerRun = []
    averageDutyCyclePerRun = []

    for inputFile in glob.glob(os.path.join(inputDir, '*.log')):

        dutyCycleDict = {}

        with open(inputFile, "r") as f:
            headerLine = json.loads(f.readline())

            # first fetch the events of interest
            for line in f:
                candidate = json.loads(line)

                # filter out the events of interest
                if candidate['event'] == 'radioDutyCycleMeasurement':
                    source = candidate['source']
                    dutyCycle = float(candidate['dutyCycle'].strip("%"))

                    if source not in dutyCycleDict:
                        dutyCycleDict[source] = {
                                                    'dutyCycle' : dutyCycle
                                                }
                    else:
                        # just update the value with the latest cumulative measurement
                        dutyCycleDict[source]['dutyCycle'] = dutyCycle

        maximalDutyCyclePerRun += [ max([dutyCycleDict[key]['dutyCycle'] for key in dutyCycleDict]) ]
        minimalDutyCyclePerRun += [ min([dutyCycleDict[key]['dutyCycle'] for key in dutyCycleDict]) ]
        averageDutyCyclePerRun += [ mean([dutyCycleDict[key]['dutyCycle'] for key in dutyCycleDict]) ]

    returnDict = {
        "maxDutyCycle" : {
                    'mean' : mean(maximalDutyCyclePerRun),
                    'min'  : min(maximalDutyCyclePerRun),
                    'max'  : max(maximalDutyCyclePerRun),
                    '99%'  : numpy.percentile(maximalDutyCyclePerRun, 99)
                },
        "minDutyCycle": {
                    'mean': mean(minimalDutyCyclePerRun),
                    'min': min(minimalDutyCyclePerRun),
                    'max': max(minimalDutyCyclePerRun),
                    '99%': numpy.percentile(minimalDutyCyclePerRun, 99)
                },
        "averageDutyCycle": {
                    'mean': mean(averageDutyCyclePerRun),
                    'min': min(averageDutyCyclePerRun),
                    'max': max(averageDutyCyclePerRun),
                    '99%': numpy.percentile(averageDutyCyclePerRun, 99)
                },
    }

    print returnDict

    return returnDict

def main():

    args = parser.parse_args()
    inputDir = args.logDir
    outputFile = os.path.join(args.logDir,  args.logDir.split('/')[-1] + ".kpi")
    kpis = {}

    kpis.update(calculate_latency(inputDir))
    kpis.update(calculate_reliability(inputDir))
    kpis.update(calculate_join_times(inputDir))
    kpis.update(calculate_duty_cycle(inputDir))

    with open(outputFile, "w") as f:
        json.dump(kpis, f, indent=4)
        f.write('\n')


if __name__ == '__main__':
    main()