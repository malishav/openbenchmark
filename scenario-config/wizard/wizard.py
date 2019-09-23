import os
import json
import random
import math
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

		assert len(self.nodes) == self.info['number_of_nodes']

		self.testbeds = self._generate_testbed_specific_template()

		self._output_json(self.info, self.nodes, self.testbeds)

	def _generate_periodic_instants(self, period, experimentDuration, payloadSize, destination, confirmable, numPacketsInBurst):

		trafficSendingPoints = []

		currentInstant = 0
		while currentInstant < experimentDuration:
			trafficSendingPoints += [
				{	'time_sec'          : currentInstant + period,
					  'payload_size'      : payloadSize,
					  'destination'       : destination,
					  'confirmable'       : confirmable,
					  'packets_in_burst'  : numPacketsInBurst,
					  }
			]
			currentInstant += period
		# now remove the element that overflowed
		trafficSendingPoints.pop()

		return trafficSendingPoints

	def _generate_poisson_instants(self, mean, experimentDuration, payloadSize, destination, confirmable, numPacketsInBurst):

		trafficSendingPoints = []

		currentInstant = 0

		while currentInstant < experimentDuration:
			nextPacketArrival = random.expovariate(mean)
			trafficSendingPoints += [
				{'time_sec': currentInstant + nextPacketArrival,
				 'payload_size': payloadSize,
				 'destination': random.choice(destination) if type(destination) is list else destination,
				 'confirmable': confirmable,
				 'packets_in_burst': numPacketsInBurst,
				 }
			]
			currentInstant += nextPacketArrival
		# now remove the element that overflowed
		trafficSendingPoints.pop()

		return trafficSendingPoints

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

			nodes[genericId] = {
				'role': role,
				'traffic_sending_points': self._generate_periodic_instants(period,
																	  experimentDuration,
																	  payloadSize,
																	  destination,
																	  confirmable,
																	  numPackets),
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

			coinToss = random.random()

			if coinToss < 0.21: # this is an event sensor
				role = 'event-sensor'
				poissonMean = 10.0 / 3600 # 10 packets per hour

				trafficSendingPoints = self._generate_poisson_instants(
					poissonMean,
					experimentDuration,
					10,
					rootId,
					True,
					1
				)

			elif coinToss < 0.21 + 0.3 : # this is an actuator
				actuators += [genericId]
				role = 'actuator'
				period = random.randint(3 * 60, 5 * 60)

				trafficSendingPoints = self._generate_periodic_instants(
					period,
					experimentDuration,
					10,
					rootId,
					True,
					1
				)

			else: # this is a monitoring sensor
				role = 'monitoring-sensor'
				period = random.randint(3 * 60, 5 * 60)

				trafficSendingPoints = self._generate_periodic_instants(period,
																		experimentDuration,
																		10,
																		rootId,
																		False,
																		1)

			nodes[genericId] = {
				'role': role,
				'traffic_sending_points': trafficSendingPoints,
			}

		# Root remains
		role = 'control-unit'
		poissonMean = 10.0 / 3600  # 10 packets per hour

		trafficSendingPoints = self._generate_poisson_instants(poissonMean,
															   experimentDuration,
															   10,
															   actuators,
															   False,
															   5)

		nodes[rootId] = {
			'role': role,
			'traffic_sending_points': trafficSendingPoints,
		}

		return nodes

	def generate_building_automation(self, numNodes, experimentDuration):
		nodes = {}
		rolePerArea = ['area-controller', 'actuator','actuator', 'monitoring-sensor', 'monitoring-sensor','monitoring-sensor', 'event-sensor','event-sensor', 'event-sensor', 'event-sensor']
		assert numNodes > 1

		rootId = "openbenchmark00"
		nodes[rootId] = {
			'role' : 'zone-controller',
			'traffic_sending_points' : [],
		}

		numAreas = int(math.ceil((numNodes - 1) / 10.0))  # 3 MS, 4 ES, 2 A, 1 AC

		# assert that the last area consists of at least two nodes: an actuator and area controller
		assert (numAreas-1) * 10 + 3 < numNodes, "Increase number of nodes for the last area to have at least 3."

		for area in range(numAreas):
			areaController = None
			for i, role in enumerate(rolePerArea, start=1):

				id = area * 10 + i

				if id > numNodes - 1:
					break

				genericId = "openbenchmark{0}".format("%02d" % (id))

				if role == "area-controller":

					areaController = genericId

					# quick-n-dirty: actuators are hard-coded to be id+1 and id+2 from the area-controller
					actuators = ["openbenchmark{0}".format("%02d" % (id+1)), "openbenchmark{0}".format("%02d" % (id+2))]

					# traffic from AC to ZC
					period = random.randint(4, 8) # CBR 4-8 seconds

					trafficSendingPoints = self._generate_periodic_instants(period,
																			 experimentDuration,
																			 10,
																			 rootId,
																			 False,
																			 1)


					# traffic from AC to A
					# FIXME change back to 10.0/3600
					poissonMean = 100.0 / 3600  # 10 packets per hour

					trafficSendingPoints += self._generate_poisson_instants(poissonMean,
																			experimentDuration,
																			10,
																			actuators,
																			True,
																			1)

					# sort the list by time_sec
					trafficSendingPointsSorted = sorted(trafficSendingPoints, key = lambda j: j['time_sec'])

					nodes[genericId] = {
						'role': role,
						'traffic_sending_points': trafficSendingPointsSorted,
					}

				if role == "actuator" or role == "monitoring-sensor":

					assert areaController

					period = random.randint(25, 35)

					trafficSendingPoints = self._generate_periodic_instants(period,
																			experimentDuration,
																			10,
																			areaController,
																			True,
																			1)

					nodes[genericId] = {
						'role': role,
						'traffic_sending_points': trafficSendingPoints,
					}

				if role == "event-sensor":
					assert areaController

					poissonMean = 10.0 / 3600  # 10 packets per hour

					trafficSendingPoints = self._generate_poisson_instants(poissonMean,
																		   experimentDuration,
																		   10,
																		   areaController,
																		   True,
																		   1)

					nodes[genericId] = {
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
