import argparse
import paho.mqtt.client as mqtt
import json
import random
import string
import os
import re
import threading
import time
import traceback
import signal
import Queue

from experiment_provisioner.main import Main as ExpProvisioner

SCENARIO_TO_DIR = {
	'demo-scenario' 		: os.path.join(os.path.dirname(__file__), "scenario-config", 'demo-scenario'),
	'building-automation' 	: os.path.join(os.path.dirname(__file__), "scenario-config", 'building-automation'),
	'home-automation' 		: os.path.join(os.path.dirname(__file__), "scenario-config", 'home-automation'),
	'industrial-monitoring' : os.path.join(os.path.dirname(__file__), "scenario-config", 'industrial-monitoring')
}

SCENARIO_CONFIG_FILENAME = '_config.json'

class OrchestratorV1():

	# MQTT topics
	OPENBENCHMARK_API_VERSION = "1.0.0"
	OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC = 'openbenchmark/command/startBenchmark'
	OPENBENCHMARK_STARTBENCHMARK_RESPONSE_TOPIC = 'openbenchmark/response/startBenchmark'

	def __init__(self, broker):

		self.broker = broker
		self.goOn = True
		self.threads = []

		signal.signal(signal.SIGINT, self._signal_handler)

		print "Broker: {0}".format(broker)

		try:
			# mqtt client
			self.mqttClient = mqtt.Client('OpenBenchmark/Orchestrator')
			self.mqttClient.on_connect = self._on_mqtt_connect
			self.mqttClient.on_message = self._on_mqtt_message
			self.mqttClient.connect(self.broker)
			self.mqttClient.loop_start()

		except Exception as e:
			traceback.print_exc()
			self.close()

		while self.goOn:
			# keep the main thread alive
			time.sleep(5)

	def close(self):
		self.goOn = False
		if self.mqttClient:
			self.mqttClient.loop_stop()
		for (thread1, thread2) in self.threads:
			thread1.close()
			thread2.close()

	def _on_mqtt_connect(self, client, userdata, flags, rc):
		self.mqttClient.subscribe(self.OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC)

	def _signal_handler(self, sig, frame):
		print "Shutting down the main process."
		self.close()

	def _on_mqtt_message(self, client, userdata, message):

		print "------------------------------------------"
		print "Message received on topic: {0}".format(message.topic)
		print message.payload
		print "------------------------------------------"

		# assume this is the startBenchmark command
		assert message.topic == self.OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC

		try:
			payload 		= json.loads(message.payload)

			api_version     = payload['api_version']
			assert api_version  == self.OPENBENCHMARK_API_VERSION

			token 			= payload['token']
			date 			= payload['date']
			firmwareName 	= payload['firmware']
			testbed 		= payload['testbed']
			nodes 			= payload['nodes']
			scenario 		= payload['scenario']

			experimentId = ''.join(random.choice(string.ascii_lowercase) for i in range(8))

			performanceEventThread = PerformanceEventHandler(broker=self.broker,
															 experimentId=experimentId,
															 scenario=scenario,
															 testbed=testbed,
															 firmware=firmwareName,
															 date=date,
															 nodes=nodes)

			orchestrateThread = OrchestrateExperiment(broker=self.broker,
													   experimentId=experimentId,
													   performanceEventHandler=performanceEventThread,
													   scenarioDir=SCENARIO_TO_DIR[scenario],
													   testbed=testbed,
													   firmwareName=firmwareName,
													   nodes=nodes)



			self.threads += [ (orchestrateThread, performanceEventThread) ]

			# respond with success
			self.mqttClient.publish(
				topic=self.OPENBENCHMARK_STARTBENCHMARK_RESPONSE_TOPIC,
				payload=json.dumps(
					{
						'token': token,
						'success': True,
						'experimentId': experimentId
					}
				),
			)

		except Exception as e:
			print "Malformed request or internal error. Responding with fail."
			traceback.print_exc()
			# respond with fail
			self.mqttClient.publish(
				topic=self.OPENBENCHMARK_STARTBENCHMARK_RESPONSE_TOPIC,
				payload=json.dumps(
					{
						'token': token,
						'success': False,
					}
				),
			)

class PerformanceEventHandler(threading.Thread):

	LOG_DIRNAME = os.path.join(os.path.dirname(__file__), "logs")

	def __init__(self, broker, experimentId, scenario, testbed, firmware, date, nodes):

		# initialize the parent class
		threading.Thread.__init__(self)

		# store vars
		self.broker = broker
		self.experimentId = experimentId
		self.scenario = scenario
		self.testbed = testbed
		self.firmware = firmware
		self.date = date
		self.nodes = nodes

		# flag to permit exit from infinite loop
		self.goOn = True

		# MQTT message queue
		self.messageQueue = Queue.Queue(maxsize=50)

		# give this thread a name
		self.name = 'PerformanceEventHandler@' + self.testbed + '@' + self.experimentId

		try:
			# mqtt client
			self.mqttClient = mqtt.Client(self.name)
			self.mqttClient.on_connect = self._on_mqtt_connect
			self.mqttClient.on_message = self._on_mqtt_message
			self.mqttClient.connect(self.broker)
			self.mqttClient.loop_start()

		except Exception as e:
			traceback.print_exc()
			self.close()

		if not os.path.exists(self.LOG_DIRNAME):
			os.makedirs(self.LOG_DIRNAME)

		self.logFile = "{0}_{1}_{2}_{3}.log".format(self.scenario, self.testbed, self.firmware, self.experimentId)
		self.logfile = os.path.join(self.LOG_DIRNAME, self.logFile)

		headerLine = {
			'date' 			: self.date,
			'experimentId'  : self.experimentId,
			'testbed'		: self.testbed,
			'firmware'      : self.firmware,
			'nodes'			: self.nodes,
			'scenario'		: scenario
		}

		with open(self.logFile, "w") as f:
			json.dump(headerLine, f)
			f.write('\n')

		# start myself
		self.start()

	# ======================== thread ==========================================

	def run(self):
		while self.goOn:
			# blocking call
			payload = self.messageQueue.get(block=True)
			# TODO implement real-time KPI calculation here
			print payload
			with open(self.logFile, "a") as f:
				json.dump(payload, f)
				f.write('\n')

	def close(self):
		self.goOn = False

	def append_line(self, line):
		with open(self.logFile, "a") as f:
			f.write(line + '\n')

	def _on_mqtt_connect(self, client, userdata, flags, rc):
		# subscribe to the handled topics
		self.mqttClient.subscribe("openbenchmark/experimentId/{0}/nodeId/+/performanceData".format(self.experimentId))

	def _on_mqtt_message(self, client, userdata, message):
		try:
			payload = message.payload.decode('utf8')
			payload = json.loads(payload)

			try:
				self.messageQueue.put(payload, block=False)
			except:
				print "queue overflow"
		except:
			print "Could not decode payload"

class OrchestrateExperiment(threading.Thread):

	ORCHESTRATE_MAX_FAILURE_COUNTER = 5

	def __init__(self, broker, experimentId, performanceEventHandler, scenarioDir, testbed, firmwareName, nodes):

		# initialize the parent class
		threading.Thread.__init__(self)

		# local vars
		self.broker = broker
		self.experimentId = experimentId
		self.performanceEventHandler = performanceEventHandler
		self.testbed = testbed
		self.scenarioConfigFile = os.path.join(scenarioDir, SCENARIO_CONFIG_FILENAME)
		self.scenarioTestbedFile = os.path.join(scenarioDir, '_{0}{1}'.format(self.testbed, SCENARIO_CONFIG_FILENAME))
		self.firmwareName = firmwareName
		self.requestNodes = nodes
		self.timeNow = 0
		self.failureCounter = 0

		# sync primitives
		self.timeLock = threading.Lock()

		# Queue to match responses to requests
		self.messageQueue = Queue.Queue(maxsize=1)

		# flag to permit exit from read loop
		self.goOn = True

		# give this thread a name
		self.name = 'Orchestrate@' + self.scenarioConfigFile + '@' + self.experimentId

		print "Initializing thread: {0}.".format(self.name)

		with open(self.scenarioConfigFile, 'r') as f:

			scenario = json.load(f)
			self.totalDurationSec        = scenario['duration_min'] * 60
			self.numberOfNodes           = scenario['number_of_nodes']
			self.networkFormationTimeSec = scenario['nf_time_padding_min'] * 60
			self.scenarioNodes           = scenario['nodes']

		with open(self.scenarioTestbedFile, 'r') as testbedFile:
			scenarioTestbed = json.load(testbedFile)

			# merge testbed specific dict of nodes with the generic one
			for k,v in scenarioTestbed.iteritems():
				self.scenarioNodes[k].update(v)

		# if the request arrives with more than one eui64 address attached to a given host name, append suffix to it
		# suffixed host matches the testbed-specific scenario file
		suffixedHosts = {}
		for host, v in self.requestNodes.iteritems():
			if len(v) > 1:
				for index, eui64 in enumerate(v):
					suffixedHosts["{0}-{1}".format(host, index)] = eui64
			else:
				suffixedHosts[host] = v[0]

		# map eui-64 received in the request with the generic identifier
		for genericId in self.scenarioNodes.keys():
			expectedTestbedHost = self.scenarioNodes[genericId]['node_id']
			self.scenarioNodes[genericId]['eui64'] = suffixedHosts[expectedTestbedHost]
			print "Mapping {0} -> {1} -> {2}.".format(genericId,
													  expectedTestbedHost,
													  self.scenarioNodes[genericId]['eui64'])

		for genericId in self.scenarioNodes.keys():
			# verify that all nodes have been mapped
			assert self.scenarioNodes[genericId]['eui64']

		print "========================================="
		print "broker                  = {0}".format(self.broker)
		print "experimentId            = {0}".format(self.experimentId)
		print "testbed                 = {0}".format(self.testbed)
		print "firmwareName            = {0}".format(self.firmwareName)
		print "requestNodes            = {0}".format(self.requestNodes)
		print "-----------------------------------------"
		print "Scenario                = {0}".format(self.scenarioConfigFile)
		print "totalDurationSec        = {0}".format(self.totalDurationSec)
		print "numberOfNodes           = {0}".format(self.numberOfNodes)
		print "networkFormationTimeSec = {0}".format(self.networkFormationTimeSec)
		print "========================================="

		try:
			# mqtt client
			self.mqttClient = mqtt.Client(self.name)
			self.mqttClient.on_connect = self._on_mqtt_connect
			self.mqttClient.on_message = self._on_mqtt_message
			self.mqttClient.connect(self.broker)
			self.mqttClient.loop_start()

		except Exception as e:
			traceback.print_exc()
			self.close()

		# start myself
		self.start()

	# ======================== thread ==========================================

	def run(self):

		try:
			# log
			print("Experiment started. Thread: {0}".format(self.name))

			while self.goOn:  # open serial port

				# add some delay on startup to allow SUT to subscribe to topics
				time.sleep(5)

				# set tx power of each node to the one in the scenario file
				for genericId in self.scenarioNodes.keys():
					self.configureTransmitPower(source=self.scenarioNodes[genericId]['eui64'],
												power=self.scenarioNodes[genericId]['transmission_power_dbm'])
					# give SUT some time to react
					time.sleep(1)

				# now is the time to trigger network formation
				self.triggerNetworkFormation(source=self.scenarioNodes['openbenchmark00']['eui64'])

				# once network formation is triggered, sleep for N mins allowing the network to form
				print "Going to sleep for {0} minutes".format(self.networkFormationTimeSec/60.0)
				time.sleep(self.networkFormationTimeSec)

				while self.timeNow < self.totalDurationSec:
					# start orchestration
					nextInstant = self.nextTrafficInstant(self.scenarioNodes)

					if nextInstant:
						(source, destination, timeInst, confirmable, payloadSize, packetsInBurst) = nextInstant

						# remove the source from the nodes list
						del self.scenarioNodes[source]['traffic_sending_points'][0]

						time.sleep(timeInst - self.timeNow)

						with self.timeLock:
							self.timeNow = timeInst

						self.triggerSendPacket(source=self.scenarioNodes[source]['eui64'],
											   destination=self.scenarioNodes[destination]['eui64'],
											   confirmable=confirmable,
											   packetsInBurst=packetsInBurst,
											   payloadSize=payloadSize)
					else:
						# end of the experiment, get out of the while timeNow loop
						break

				print "End of the experiment. Shutting down thread {0}".format(self.name)
				self.close()

		except Exception as err:
			traceback.print_exc()
			self.close()

	# ======================== public ==========================================

	def close(self):
		self.goOn = False
		with self.timeLock:
			self.timeNow = self.totalDurationSec + 1

	def configureTransmitPower(self, source, power):

		token = ''.join(random.choice(string.ascii_lowercase) for i in range(8))

		payload = {
			'token' : token,
			'source': source,
			'power' : int(power),
		}

		topic = "openbenchmark/experimentId/{0}/command/configureTransmitPower".format(self.experimentId)

		logLine = {
			"event" : "command",
			"type"  : "configureTransmitPower",
			"topic" : topic,
		}
		logLine.update(payload)

		print json.dumps(logLine)
		self.performanceEventHandler.append_line(json.dumps(logLine))

		self._mqtt_publish_and_wait(
			topic=topic,
			payload=json.dumps(payload),
			token=token,
		)

	def triggerNetworkFormation(self, source):

		token = ''.join(random.choice(string.ascii_lowercase) for i in range(8))

		payload = {
			"token"  : token,
			"source" : source,
		}

		topic = "openbenchmark/experimentId/{0}/command/triggerNetworkFormation".format(self.experimentId)

		logLine = {
			"event": "command",
			"type": "triggerNetworkFormation",
			"topic": topic,
		}
		logLine.update(payload)

		print json.dumps(logLine)
		self.performanceEventHandler.append_line(json.dumps(logLine))

		self._mqtt_publish_and_wait(
			topic=topic,
			payload=json.dumps(payload),
			token=token,
		)

	def triggerSendPacket(self, source, destination, confirmable, packetsInBurst, payloadSize):

		token = ''.join(random.choice(string.ascii_lowercase) for i in range(8))
		packetToken = [0, random.randint(0, 255), random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)]

		payload = {
			"token"            : token,
			"source"           : source,
			"destination"      : destination,
			"packetsInBurst"   : int(packetsInBurst),
			"packetToken"      : packetToken,
			"packetPayloadLen" : int(payloadSize),
			"confirmable"      : bool(confirmable),
		}

		topic = "openbenchmark/experimentId/{0}/command/sendPacket".format(self.experimentId)

		logLine = {
			"event": "command",
			"type": "sendPacket",
			"topic": topic,
		}
		logLine.update(payload)

		print json.dumps(logLine)
		self.performanceEventHandler.append_line(json.dumps(logLine))

		self._mqtt_publish_and_wait(
			topic=topic,
			payload=json.dumps(payload),
			token=token,
		)

	''' Returns a tuple
	(source, destination, time, confirmable, packetsInBurst)
	'''
	def nextTrafficInstant(self, nodes):
		# first element in the sorted traffic_sending_points list of each node
		candidatesList = []
		for k,v in nodes.iteritems():

			try:
				candidate = v['traffic_sending_points'][0]
				candidate['source'] = k

				source = k
				destination = candidate['destination']
				timeInst = candidate['time_sec']
				confirmable = candidate['confirmable']
				payloadSize = candidate['payload_size']
				packetsInBurst = candidate.get('packets_in_burst', 1)

			except IndexError:
				# meaning we are out of events to send for this node
				source = None
				destination = None
				timeInst = None
				confirmable = None
				payloadSize = None
				packetsInBurst = None
			except:
				traceback.print_exc()
				self.close()
				sys.exit()
			else:
				if source is not None:
					# convert to tuple for easier manip
					candidatesList.append(
						(source, destination, timeInst, confirmable, payloadSize, packetsInBurst)
					)
		try:
			# sort by time instant
			nextInstant = min(candidatesList, key = lambda k:k[2])
		except:
			nextInstant = None

		return nextInstant

	# ======================== private ==========================================

	def _on_mqtt_connect(self, client, userdata, flags, rc):
		if rc != 0:
			print "Unsuccesful MQTT connection. Return code: {0}".format(rc)
			self.close()

		# subscribe to the handled topics
		self.mqttClient.subscribe("openbenchmark/experimentId/{0}/command/echo".format(self.experimentId))
		self.mqttClient.subscribe("openbenchmark/experimentId/{0}/response/sendPacket".format(self.experimentId))
		self.mqttClient.subscribe("openbenchmark/experimentId/{0}/response/configureTransmitPower".format(self.experimentId))
		self.mqttClient.subscribe("openbenchmark/experimentId/{0}/response/triggerNetworkFormation".format(self.experimentId))

	def _on_mqtt_message(self, client, userdata, message):
		if message.topic == "openbenchmark/experimentId/{0}/command/echo".format(self.experimentId):

			try:

				payload = message.payload.decode('utf8')
				assert payload, "Could not decode payload"

				tokenReceived = json.loads(payload)['token']

				# respond with success
				client.publish(
					topic="openbenchmark/experimentId/{0}/response/echo",
					payload=json.dumps(
						{
							'token': tokenReceived,
							'success': True,
						}
					),
				)
			except:
				traceback.print_exc()

		# this is a response to some of the messages sent, basic processing to verify if it's a success
		else:
			try:
				m = re.search("openbenchmark/experimentId/{0}/response/([a-zA-Z]+)".format(self.experimentId), message.topic)
				assert m, "Invalid topic, could not parse: '{0}'".format(topic)

				subTopic = m.group(1)

				print("Received response {0}".format(subTopic))

				payload = message.payload.decode('utf8')
				assert payload, "Could not decode payload"

				payload = json.loads(payload)
				tokenReceived = payload['token']
				success = payload['success']

				try:
					self.messageQueue.put((tokenReceived, success), block=False)
				except:
					print "queue overflow"

			except:
				self.failureCounter += 1
				traceback.print_exc()
				if self.failureCounter >= self.ORCHESTRATE_MAX_FAILURE_COUNTER:
					print("Too many failures, shutting down.")
					self.close()

	def _mqtt_publish_and_wait(self, topic, payload, token):

		self.mqttClient.publish(
			topic=topic,
			payload=payload,
		)

		try:
			(tokenReceived, success) = self.messageQueue.get(block=True, timeout=1)

			assert tokenReceived == token
			assert success

		except:
			print "Failure indicated or timeout occured."
			self.failureCounter += 1
			traceback.print_exc()
			if self.failureCounter >= self.ORCHESTRATE_MAX_FAILURE_COUNTER:
				print("Too many failures, shutting down.")
				self.close()

class OpenBenchmark:

	def __init__(self):
		pass

	def add_parser_args(self, parser):
		parser.add_argument('--user-id',   # User ID is tied to the OpenBenchmark account
			dest       = 'user_id',
			default    = 0,
			required   = False,
			action     = 'store'
		)
		parser.add_argument('--simulator', 
			dest       = 'simulator',
			default    = False,
			action     = 'store_true'
		)
		parser.add_argument('--action', 
			dest       = 'action',
			choices    = ['check', 'reserve', 'terminate', 'flash', 'sut-start', 'ov', 'orchestrator', 'orchestratorV1'],
			required   = True,
			action     = 'store'
		)
		parser.add_argument('--testbed', 
			dest       = 'testbed',
			choices    = ['iotlab', 'wilab', 'opensim'],
			default    = 'iotlab',
			action     = 'store'
		)
		parser.add_argument('--firmware', 
			dest       = 'firmware',
			required   = False,
			action     = 'store',
		)
		parser.add_argument('--branch', 
			dest       = 'branch',
			required   = False,
			action     = 'store',
		)
		parser.add_argument('--scenario',
			dest       = 'scenario',
			choices    = ['demo-scenario', 'building-automation', 'home-automation', 'industrial-monitoring'],
			default    = 'demo-scenario',
			action     = 'store'
		)
		parser.add_argument('--broker',
			dest       = 'broker',
			default    = 'argus.paris.inria.fr',
			action     = 'store'
		)

	def get_args(self):
		parser = argparse.ArgumentParser()
		self.add_parser_args(parser)
		args = parser.parse_args()

		self._validate(args, parser)

		return {
			'user_id'   : args.user_id,
			'simulator' : args.simulator,
			'action'    : args.action,
			'testbed'   : args.testbed,
			'firmware'  : args.firmware,
			'branch'    : args.branch,
			'scenario'  : args.scenario,
			'broker'	: args.broker
		}

	def _validate(self, args, parser):
		if args.action != 'sut-start' and args.simulator:
			parser.error('--simulator is only a valid parameter for --action=sut-start')

		if args.testbed == 'opensim' and args.action not in ['flash', 'sut-start', 'ov', 'orchestrator', 'terminate']:
			parser.error('OpenSim testbed simulator supports only `sut-start`, `ov`, `orchestrator`, and `terminate` actions')


def main():
	openbenchmark = OpenBenchmark()
	args = openbenchmark.get_args()

	user_id   = args['user_id']
	simulator = args['simulator']
	action    = args['action']
	testbed   = args['testbed']
	scenario  = args['scenario']
	broker    = args['broker']

	firmware  = args['firmware']
	branch    = args['branch']


	if action == 'orchestratorV1':
		OrchestratorV1(broker=broker)
	else:
		ExpProvisioner(user_id, simulator, action, testbed, scenario, firmware, branch)

if __name__ == '__main__':
	main()