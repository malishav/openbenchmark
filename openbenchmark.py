import argparse
import paho.mqtt.client as mqtt
import json
import random
import string
import os
import threading
import time
import traceback
import signal

from experiment_provisioner.main import Main as ExpProvisioner

SCENARIO_TO_FILE = {
	'demo-scenario' 		: os.path.join(os.path.dirname(__file__), "scenario-config", 'demo-scenario', '_config.json'),
	'building-automation' 	: os.path.join(os.path.dirname(__file__), "scenario-config", 'building-automation', '_config.json'),
	'home-automation' 		: os.path.join(os.path.dirname(__file__), "scenario-config", 'home-automation', '_config.json'),
	'industrial-monitoring' : os.path.join(os.path.dirname(__file__), "scenario-config", 'industrial-monitoring', '_config.json')
}

class OrchestratorV2():

	# MQTT topics
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
			time.sleep(5)
			pass

	def close(self):
		self.goOn = False
		if self.mqttClient:
			self.mqttClient.loop_stop()
		for thread in self.threads:
			thread.close()

	def _on_mqtt_connect(self, client, userdata, flags, rc):
		self.mqttClient.subscribe(self.OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC)

	def _signal_handler(self, sig, frame):
		print "Shutting down the deamon."
		self.close()

	def _on_mqtt_message(self, client, userdata, message):

		print "Message received on topic: {0}".format(message.topic)
		print message.payload

		# assume this is the startBenchmark command
		assert message.topic == self.OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC

		try:
			payload 		= json.loads(message.payload)
			token 			= payload['token']
			date 			= payload['date']
			firmwareName 	= payload['firmware']
			testbed 		= payload['testbed']
			nodes 			= payload['nodes']
			scenario 		= payload['scenario']

			experimentId = ''.join(random.choice(string.ascii_lowercase) for i in range(10))

			self.threads += [ OrchestrateExperiment(broker=self.broker,
								  experimentId=experimentId,
								  scenarioFile=SCENARIO_TO_FILE[scenario],
								  testbed=testbed,
								  firmwareName=firmwareName,
								  nodes=nodes) ]

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
		except KeyError as key:
			print "Malformed request. Responding with fail."
			# respond with success
			self.mqttClient.publish(
				topic=self.OPENBENCHMARK_STARTBENCHMARK_RESPONSE_TOPIC,
				payload=json.dumps(
					{
						'token': token,
						'success': False,
					}
				),
			)
		except Exception as e:
			traceback.print_exc()
			self.close()

class OrchestrateExperiment(threading.Thread):

	def __init__(self, broker, experimentId, scenarioFile, testbed, firmwareName, nodes):

		# initialize the parent class
		threading.Thread.__init__(self)

		# local vars
		self.broker = broker
		self.experimentId = experimentId
		self.scenarioFile = scenarioFile
		self.testbed = testbed
		self.firmwareName = firmwareName
		self.nodes = nodes
		self.timeNow = 0

		# sync primitives
		self.timeLock = threading.Lock()

		# flag to permit exit from read loop
		self.goOn = True

		# give this thread a name
		self.name = 'Orchestrate@' + self.scenarioFile + '@' + self.experimentId

		with open(self.scenarioFile, 'r') as f:
			scenario = json.load(f)
			self.totalDurationSec        = scenario['duration_min'] * 60
			self.numberOfNodes           = scenario['number_of_nodes']
			self.payloadSize             = scenario['payload_size']
			self.networkFormationTimeSec = scenario['nf_time_padding_min'] * 60
			self.nodes                   = scenario['nodes']

		print "========================================="
		print "Thread {0} starting".format(self.name)
		print "broker                  = {0}".format(self.broker)
		print "experimentId            = {0}".format(self.experimentId)
		print "testbed                 = {0}".format(self.testbed)
		print "firmwareName            = {0}".format(self.firmwareName)
		print "nodes                   = {0}".format(self.nodes)
		print "========================================="
		print "Scenario                = {0}".format(self.scenarioFile)
		print "totalDurationSec        = {0}".format(self.totalDurationSec)
		print "numberOfNodes           = {0}".format(self.numberOfNodes)
		print "payloadSize             = {0}".format(self.payloadSize)
		print "networkFormationTimeSec = {0}".format(self.networkFormationTimeSec)
		print "========================================="

		# start myself
		self.start()

	def close(self):
		self.goOn = False
		with self.timeLock:
			self.timeNow = self.totalDurationSec + 1
	# ======================== thread ==========================================

	def run(self):

		try:
			# log
			print("Experiment started. Thread: {0}".format(self.name))

			while self.goOn:  # open serial port

				while self.timeNow < self.totalDurationSec:
					# start orchestration
					nextInstant = self.nextTrafficInstant(self.nodes)

					if nextInstant:
						(source, destination, timeInst, confirmable, packetsInBurst) = nextInstant

						# remove the source from the nodes list
						del self.nodes[source]['traffic_sending_points'][0]

						time.sleep(timeInst - self.timeNow)

						with self.timeLock:
							self.timeNow = timeInst

						# TODO send MQTT command
						print "Sending MQTT command to: {0} @ {1}".format(source, timeInst)
					else:
						# end of the experiment, get out of the while timeNow loop
						break

				print "End of the experiment. Shutting down thread {0}".format(self.name)
				self.close()

		except Exception as err:
			traceback.print_exc()
			sys.exit()

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
				packetsInBurst = candidate.get('packets_in_burst', 1)

			except IndexError:
				source = None
				destination = None
				timeInst = None
				confirmable = None
				packetsInBurst = None
			except:
				traceback.print_exc()
				self.close()
				sys.exit()
			else:
				if source is not None:
					# convert to tuple for easier manip
					candidatesList.append(
						(source, destination, timeInst, confirmable, packetsInBurst)
					)
		try:
			# sort by time instant
			nextInstant = min(candidatesList, key = lambda k:k[2])
		except:
			nextInstant = None

		return nextInstant

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
			choices    = ['check', 'reserve', 'terminate', 'flash', 'sut-start', 'ov', 'orchestrator', 'orchestratorV2'],
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


	if action == 'orchestratorV2':
		print "starting orchestratorV2"
		OrchestratorV2(broker=broker)
	else:
		ExpProvisioner(user_id, simulator, action, testbed, scenario, firmware, branch)

if __name__ == '__main__':
	main()