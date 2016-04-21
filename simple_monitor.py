from __future__ import division
from operator import attrgetter

import json
import logging

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.lib import dpid as dpid_lib

SLEEP_PERIOD = 2
simple_switch_instance_name = 'simple_switch_api_app'
url = '/{dpid}'

class simple_monitor(app_manager.RyuApp):
	OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
	_NAME = 'simple_monitor'
	_CONTEXTS = { 'wsgi': WSGIApplication }
	def __init__(self, *args, **kwargs):
		super(simple_monitor, self).__init__(*args, **kwargs)
		self.datapaths = {}
		self.port_stats = {}
		self.port_speed = {}
		self.stats = {}
		self.monitor_thread = hub.spawn(self._monitor)
		wsgi = kwargs['wsgi']
		wsgi.register(SimpleSwitchController, {simple_switch_instance_name : self})
	@set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
	def _state_change_handler(self, ev):
		datapath = ev.datapath
		if ev.state == MAIN_DISPATCHER:
			if not datapath.id in self.datapaths:
				self.logger.debug('register datapath: %016x', datapath.id)
				self.datapaths[datapath.id] = datapath
		elif ev.state == DEAD_DISPATCHER:
			if datapath.id in self.datapaths:
				self.logger.debug('unregister datapath: %016x', datapath.id)
				del self.datapaths[datapath.id]

	def _monitor(self):
		while True:
			self.stats['port'] = {}
			for dp in self.datapaths.values():
				self._request_stats(dp)
			hub.sleep(SLEEP_PERIOD)
			if self.stats['port']:
				self.show_stat('port', self.stats['port'])
				hub.sleep(1)

	def _request_stats(self, datapath):
		self.logger.debug('send port stats request: %016x', datapath.id)
		ofproto = datapath.ofproto
		parser = datapath.ofproto_parser
		req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
		datapath.send_msg(req)
	def _save_stats(self, dist, key, value, length):
		if key not in dist:
			dist[key] = []
		dist[key].append(value)

		if len(dist[key]) > length:
			dist[key].pop(0)
      
	def _get_speed(self, now, pre, period):
		if period:
			return (now - pre) / (period)/1024
		else:
			return 0
          
	def _get_time(self, sec, nsec):
		return sec + nsec / (10 ** 9)

	def _get_period(self, n_sec, n_nsec, p_sec, p_nsec):
		return self._get_time(n_sec, n_nsec) - self._get_time(p_sec, p_nsec)          
   
	def show_stat(self, type, bodys):
		if(type == 'port'):
			print('datapath             port   mac   ''rx-pkts  rx-bytes rx-error '
					'tx-pkts  tx-bytes tx-error  port-speed(KB/s)' )
			print('----------------   -------- --------------- ''-------- -------- -------- '
			'-------- -------- -------- ''----------------')
			format = '%016x %8x %8x %8d %8d %8d %8d %8d %8d %8.1f'
			for dpid in bodys.keys():
				for stat in sorted(bodys[dpid], key=attrgetter('port_no')):
					if stat.port_no != ofproto_v1_3.OFPP_LOCAL:
						if  self.port_speed[dpid]:
							print(format % (
							dpid, stat.port_no,1,
							stat.rx_packets, stat.rx_bytes, stat.rx_errors,
							stat.tx_packets, stat.tx_bytes, stat.tx_errors,
							abs(self.port_speed[dpid][stat.port_no][-1])))
  
	@set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
	def _port_stats_reply_handler(self, ev):
		body = ev.msg.body
		self.stats['port'][ev.msg.datapath.id] = body
		for stat in sorted(body, key=attrgetter('port_no')):
			if stat.port_no != ofproto_v1_3.OFPP_LOCAL:
				key = (ev.msg.datapath.id, stat.port_no)
				value = (stat.tx_bytes, stat.rx_bytes, stat.rx_errors,
							stat.duration_sec, stat.duration_nsec)
							
				self._save_stats(self.port_stats, key, value, 5)
				self.port_speed.setdefault(ev.msg.datapath.id,{})
				pre = 0
				period = SLEEP_PERIOD
				tmp = self.port_stats[key]
				if len(tmp) > 1:
					pre = tmp[-2][0] + tmp[-2][1]
					period = self._get_period(tmp[-1][3], tmp[-1][4],
												tmp[-2][3], tmp[-2][4])

					speed = self._get_speed(
					self.port_stats[key][-1][0] + self.port_stats[key][-1][1],
					pre, period)

					self._save_stats(self.port_speed[ev.msg.datapath.id], stat.port_no, speed, 5)
class SimpleSwitchController(ControllerBase):
	def __init__(self, req, link, data, **config):
		super(SimpleSwitchController, self).__init__(req, link, data, **config)
		self.simpl_switch_spp = data[simple_switch_instance_name]
	@route('simpleswitch', url, methods=['GET'], requirements={'dpid': dpid_lib.DPID_PATTERN})
	def list_port_speed(self, req, **kwargs):
		simple_switch = self.simpl_switch_spp
		dpid = dpid_lib.str_to_dpid(kwargs['dpid'])
			
		if dpid not in simple_switch.port_speed:
			return Response(status=404)
		port_speed = simple_switch.port_speed.get(dpid,{})
		body = json.dumps(port_speed)
		return Response(content_type='application/json', body=body)
			
				 
			 
		
