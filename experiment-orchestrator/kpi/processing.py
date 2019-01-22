import sys
sys.path.append('..')

import time
import json
import threading
from logger import Logger
from mqtt_client._condition_object import ConditionObject


class KPIProcessing:

	def __init__(self):
		self.logger           = Logger.create()
		self.condition_object = ConditionObject.create()
		self.cv               = self.condition_object.exp_event_cv
		self.queue            = self.condition_object.exp_event_queue

		self.buffer           = []   # Array of dictionaries {"eui64": type `string`, "event_payload": type `dict`}
		self.start_timestamp  = int(round(time.time() * 1000))
		self.window_size      = 2*60*1000    # 2 minutes expressed in milliseconds

		self.event_to_method  = {
			"packetSent"    : None,
			"packetReceived": None,
			"networkFormationCompleted": self._networkFormationTime,
			"syncronizationCompleted": self._synchronizationPhase,
			"secureJoinCompleted": self._secureJoinPhase,
			"bandwidthAssigned": self._bandwidthAssignment,
			"radioDutyCycleMeasurement": self._radioDutyCycle,
			"clockDriftMeasurement": None
		}


	def start(self):   # Should be started upon startBenchmark command
		threading.Thread(target=self._epe_monitor).start()


	def _epe_monitor(self):
		while True:
			if self.queue.empty():
				self.cv.acquire()
				self.cv.wait()     # Released when the queue has a new item
				self.cv.release()

			complete_payload = self.queue.get()
			self._process_event(complete_payload)

	def _process_event(self, complete_payload):
		# Should implement logic for event payload processing and updating log file
		# self.logger.log('kpi', event_payload)
		event_obj = json.loads(complete_payload)
		self._kpi_calculate(event_obj)
		self.buffer.append(event_obj)


	# Methods for calculating KPI
	def _kpi_calculate(self, event_obj):
		self.event_to_method[event_obj['event']](event_obj)

	def _networkFormationTime(self, event_obj):
		self.logger.log('kpi', {
				'kpi'      : 'networkFormationTime',
				'eui64'    : event_obj['eui64'],
				'timestamp': event_obj['timestamp']
			}
		)

	def _synchronizationPhase(self, event_obj):
		self.logger.log('kpi', {
				'kpi'      ; 'syncronizationPhase',
				'eui64'    : event_obj['eui64'],
				'timestamp': event_obj['timestamp']
			}
		)

	def _secureJoinPhase(self, event_obj):
		self.logger.log('kpi', {
				'kpi'      : 'secureJoinPhase',
				'eui64'    : event_obj['eui64'],
				'timestamp': event_obj['timestamp']
			}
		)

	def _bandwidthAssignment(self, event_obj):
		self.logger.log('kpi', {
				'kpi'      : 'bandwidthAssignment',
				'eui64'    : event_obj['eui64'],
				'timestamp': event_obj['timestamp']
			}
		)

	def _radioDutyCycle(self, event_obj):
		self.logger.log('kpi', {
				'kpi'      : 'radioDutyCycle',
				'eui64'    : event_obj['eui64'],
				'timestamp': event_obj['timestamp'],
				'dutyCycle': event_obj['dutyCycle']
			}
		)