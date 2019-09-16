import os
import json
import random
from collections import OrderedDict


class Wizard:

	def __init__(self):

		self.info = {}

		scenarioDict = {
			0 : 'demo-scenario',
			1 : 'building-automation',
			2 : 'home-automation',
			3 : 'industrial-monitoring'
		}

		print("WELCOME to Scenario Generator. Please provide the config parameters:")
		print("-----------------------------")

		identifier = input("Pick a scenario (0 - {0}, 1 - {1}, 2 - {2}, 3 - {3}): ".format('demo-scenario', 'building-automation', 'home-automation', 'industrial-monitoring'))
		self.info['identifier'] = scenarioDict[identifier]

		self.info['duration_min'] = input("Duration in minutes (e.g. 30): ")
		self.info['number_of_nodes'] = input("Number of nodes (e.g. 10): ")
		self.info['nf_time_padding_min'] = 30

		self.nodes = None

		if self.info['identifier'] == 'industrial-monitoring':
			self.nodes = self.generate_industrial_monitoring(self.info['number_of_nodes'], self.info['duration_min'] * 60)
		elif self.info['identifier'] == 'home-automation':
			self.nodes = self.generate_home_automation(self.info['number_of_nodes'], self.info['duration_min'] * 60)
		elif self.info['identifier'] == 'demo-scenario':
			self.nodes = self.generate_industrial_monitoring(self.info['number_of_nodes'], self.info['duration_min'] * 60)
		elif self.info['identifier'] == 'building-automation':
			self.nodes = self.generate_building_automation(self.info['number_of_nodes'], self.info['duration_min'] * 60)

		self.testbeds = self._generate_testbed_specific_template()

		self._output_json(self.info, self.nodes, self.testbeds)

	def generate_industrial_monitoring(self, numNodes, experimentDuration):
		nodes = {}
		assert numNodes > 1

		rootId = "openbenchmark00"
		nodes[rootId] = {
			'role' : 'gateway',
			'traffic_sending_points' : [],
		}

		for i in range(numNodes-1):
			genericId = "openbenchmark{0}".format("%02d" % (i + 1))
			trafficSendingPoints = []

			coinToss = random.random()

			if coinToss < 0.1: # this is a bursty sensor
				role = 'bursty-sensor'
				# period between 1 minute and 1 hour
				period = random.randint(1 * 60, 60 * 60)
				numPackets = 5
				payloadSize = 80
				destination = rootId
				confirmable = False
			else: # this is a monitoring sensor
				role = 'sensor'
				# period between 10 and 60 seconds
				period = random.randint(10, 60)
				numPackets = 1
				payloadSize = 10
				destination = rootId
				confirmable = False

			currentInstant = 0
			while currentInstant < experimentDuration:
				trafficSendingPoints += [
					{	 'time_sec'          : currentInstant + period,
						 'payload_size'      : payloadSize,
						 'destination'       : destination,
						 'confirmable'       : confirmable,
						 'packets_in_burst'  : numPackets,
					}
				]
				currentInstant += period

			# now remove the element that overflowed
			trafficSendingPoints.pop()

			nodes[genericId] = {
				'role': role,
				'traffic_sending_points': trafficSendingPoints,
			}

		return nodes

	def generate_home_automation(self, numNodes, experimentDuration):
		nodes = {}
		assert numNodes > 1
		actuators = []

		rootId = "openbenchmark00"
		nodes[rootId] = {
			'role' : 'gateway',
			'traffic_sending_points' : [],
		}

		for i in range(numNodes-1):
			genericId = "openbenchmark{0}".format("%02d" % (i + 1))
			trafficSendingPoints = []

			coinToss = random.random()

			if coinToss < 0.21: # this is an event sensor
				role = 'event-sensor'
				poissonMean = 10.0 / 3600 # 10 packets per hour

				currentInstant = 0
				while currentInstant < experimentDuration:
					nextPacketArrival = random.expovariate(poissonMean)
					trafficSendingPoints += [
						{	'time_sec'            : currentInstant + nextPacketArrival,
							  'payload_size'      : 10,
							  'destination'       : rootId,
							  'confirmable'       : True,
							  'packets_in_burst'  : 1,
							  }
					]
					currentInstant += nextPacketArrival

				# now remove the element that overflowed
				trafficSendingPoints.pop()

			elif coinToss < 0.21 + 0.3 : # this is an actuator
				actuators += [genericId]
				role = 'actuator'
				period = random.randint(3 * 60, 5 * 60)

				currentInstant = 0
				while currentInstant < experimentDuration:
					trafficSendingPoints += [
						{	'time_sec'            : currentInstant + period,
							  'payload_size'      : 10,
							  'destination'       : rootId,
							  'confirmable'       : True,
							  'packets_in_burst'  : 1,
							  }
					]
					currentInstant += period

				# now remove the element that overflowed
				trafficSendingPoints.pop()

			else: # this is a monitoring sensor
				role = 'monitoring-sensor'
				period = random.randint(3 * 60, 5 * 60)

				currentInstant = 0
				while currentInstant < experimentDuration:
					trafficSendingPoints += [
						{
							'time_sec'           : currentInstant + period,
						    'payload_size'       : 10,
						    'destination'        : rootId,
						    'confirmable'        : False,
						    'packets_in_burst'   : 1,
						 }
					]
					currentInstant += period

				# now remove the element that overflowed
				trafficSendingPoints.pop()

			nodes[genericId] = {
				'role': role,
				'traffic_sending_points': trafficSendingPoints,
			}

		# Root remains
		role = 'control-unit'
		poissonMean = 10.0 / 3600  # 10 packets per hour
		trafficSendingPoints = []

		currentInstant = 0
		while currentInstant < experimentDuration:
			nextPacketArrival = random.expovariate(poissonMean)
			trafficSendingPoints += [
				{'time_sec': currentInstant + nextPacketArrival,
				 'payload_size': 10,
				 'destination': random.choice(actuators),
				 'confirmable': False,
				 'packets_in_burst': 5,
				 }
			]
			currentInstant += nextPacketArrival

		# now remove the element that overflowed
		trafficSendingPoints.pop()

		nodes[rootId] = {
			'role': role,
			'traffic_sending_points': trafficSendingPoints,
		}

		return nodes

	def _generate_testbed_specific_template(self):

		testbeds =  {
			"iotlab": {},
			"wilab":  {},
			"opensim":{}
		}

		for key in self.nodes:
			for testbed in testbeds:
				testbeds[testbed][key] = {"node_id": "", "transmission_power_dbm": 0}
				testbeds[testbed][key] = {"node_id": "", "transmission_power_dbm": 0}

		return testbeds

	def _output_json(self, info, nodes, testbeds):
		identifier = self.info['identifier']
		path       = os.path.join(os.path.dirname(__file__), "..", identifier)

		if not os.path.exists(path):
			os.mkdir(path)

		with open(os.path.abspath(os.path.join(path, '_config.json')), 'w') as f:
			content = OrderedDict()
			content['identifier']          = info['identifier']
			content['duration_min']        = info['duration_min']
			content['number_of_nodes']     = info['number_of_nodes']
			content['nf_time_padding_min'] = info['nf_time_padding_min']
			content['nodes']               = OrderedDict(nodes)
			f.write(json.dumps(content, indent=4))

		for testbed in self.testbeds:
			with open(os.path.abspath(os.path.join(path, '_{0}_config.json'.format(testbed))), 'w') as f:
				f.write(json.dumps(self.testbeds[testbed], indent=4, sort_keys=True))



def main():
	Wizard()

if __name__ == '__main__':
	main()
