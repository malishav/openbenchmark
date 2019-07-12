import argparse
import paho.mqtt.client as mqtt
import json
import random
import string
import os
import threading
import time
import traceback

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

	def _on_mqtt_connect(self, client, userdata, flags, rc):
		self.mqttClient.subscribe(self.OPENBENCHMARK_STARTBENCHMARK_REQUEST_TOPIC)

	def _on_mqtt_message(self, client, userdata, message):

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

			OrchestrateExperiment(broker=self.broker,
								  experimentId=experimentId,
								  scenarioFile=SCENARIO_TO_FILE[scenario],
								  testbed=testbed,
								  firmwareName=firmwareName,
								  nodes=nodes)

			# respond with success
			self.mqttClient.publish(
				topic=self.OPENBENCHMARK_STARTBENCHMARK_RESPONSE_TOPIC,
				payload=json.dumps(
					{
						'token': token,
						'success': True,
						'experimentId': ran
					}
				),
			)
		except Exception as e:
			traceback.print_exc()
			self.close()

class OrchestrateExperiment(threading.Thread):

	def __init__(self, broker, experimentId, scenarioFile, testbed, firmwareName, nodes):

		# local vars
		self.broker = broker
		self.experimentId = experimentId
		self.scenarioFile = scenarioFile
		self.testbed = testbed
		self.firmwareName = firmwareName
		self.nodes = nodes

		# flag to permit exit from read loop
		self.goOn = True

		# initialize the parent class
		threading.Thread.__init__(self)

		# give this thread a name
		self.name = 'Orchestrate@' + self.scenarioFile + '@' + self.experimentId

		# start myself
		self.start()

	def close(self):

		self.goOn = False
	# ======================== thread ==========================================

	def run(self):

		try:
			# log
			print("Experiment started. Thread: {0}".format(self.name))

			while self.goOn:  # open serial port
				time.sleep(5)
				pass
				# start orchestration
		except Exception as err:
			traceback.print_exc()
			sys.exit()

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