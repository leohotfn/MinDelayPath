# coding:utf-8

import time

from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.topology import event, switches
from ryu.lib.packet import packet, arp, ethernet, ipv4, ipv6, ether_types
from ryu.base.app_manager import lookup_service_brick
from ryu.lib import hub


class MinDelayPathController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    DELAY_DETECT_PERIOD = 5     # 延迟探测时间间隔，单位秒

    def __init__(self, *args, **kwargs):
        super(MinDelayPathController, self).__init__(*args, **kwargs)

        # datapath dict，{datapath_id: datapath, }
        self.datapath_dict = {}

        # 相邻交换机节点，{s1: {s2: s1's-port-to-s1}, }
        self.switch_link_dict = {}

        # lldp 延迟，{s1: {s2: controller-s1-s2-controller's delay }}
        self.lldp_delay_dict = {}

        # echo 报文延迟，{s1: controller-s1's delay}
        self.echo_delay_dict = {}

        # 相邻交换机之间的链路往返延迟，{ s1: {s2: s1-to-s2's delay}, }
        self.link_delay_dict = {}

        # 主机与交换机的连接信息，{host_mac: (datapath_id, datapath_in_port), }
        self.hosts_dict = {}

        # 主机 IP 与 MAC 的映射关系，{host-ip: host-mac, }
        self.host_arp_dict = {}

        self.switches_module = lookup_service_brick("switches")

        self.detect_thread = hub.spawn(self.delay_detect_loop)

    def get_paths(self, src, dst):
        """
        使用 DFS 算法，获取从 src 到 dst 的所有路径。
        :param src: 源交换机节点。
        :param dst: 目标交换机节点。
        :return: 包含了所有路径的 list。
        """
        paths_list = []

        if src == dst:
            paths_list.append([src])
            return paths_list

        stack = [(src, [src])]
        while stack:
            (node, path) = stack.pop()
            for next in set(self.switch_link_dict[node].keys()) - set(path):
                if next is dst:
                    paths_list.append(path + [next])
                else:
                    stack.append((next, path + [next]))

        return paths_list

    def get_link_delay(self, s1, s2):
        """
        获取相邻的两两交换机之间，s1 到 s2 的链路延迟。由于 s1 到 s2 与 s2 到 s1 的延迟可能不相等，因此取两值的平均值。
        :param s1: 交换机1。
        :param s2: 交换机2。
        :return: 交换机 s1 到 s2 的链路延迟。
        """
        delay1 = None
        if s1 in self.link_delay_dict.keys():
            delay1 = self.link_delay_dict[s1].get(s2, None)
        delay1 = delay1 if delay1 is not None else float("inf")

        delay2 = None
        if s2 in self.link_delay_dict.keys():
            delay2 = self.link_delay_dict[s2].get(s1, None)
        delay2 = delay2 if delay2 is not None else float("inf")

        return (delay1 + delay2) / 2

    def get_path_delay(self, path):
        """
        获取路径 path 的延迟，即 path 上交换机之间的链路延迟之和。
        :param path: 路径。
        :return: 路径 path 的延迟。
        """
        delay = 0
        for i in xrange(len(path) - 1):
            delay += self.get_link_delay(path[i], path[i + 1])

        return delay

    def add_ports_to_paths(self, paths_list, first_port, last_port):
        """
        paths_list 是通过 get_paths 获取到的两个交换机之间的所有路径。
        该函数的作用是把 paths_list 上每一个交换机使用的端口对接起来。
        :param paths_list: 通过 get_paths 获取到的两个交换机之间的所有路径。
        :param first_port:  paths_list 上第一个交换机的输入端口。
        :param last_port:   paths_list 上最后一个交换机ID输出端口。
        :return:  两个交换机之间的所有路径，且带有输入输出端口。假设 paths_list 为 [[1,2,3]]，则返回值为：
        [{1:(交换机1的输入端口, 交换机1到交换机2的输出端口), 2:(交换机2的输入端口, 交换机2到交换机3的输出端口), 3:(交换机3的输入端口, 交换机3的输出端口)}]
        """
        paths_with_port_list = []
        for path in paths_list:
            paths_with_port = {}
            in_port = first_port
            for s1, s2 in zip(path[:-1], path[1:]):
                out_port = self.switch_link_dict[s1][s2]
                paths_with_port[s1] = (in_port, out_port)
                in_port = self.switch_link_dict[s2][s1]
            paths_with_port[path[-1]] = (in_port, last_port)
            paths_with_port_list.append(paths_with_port)

        return paths_with_port_list

    def install_paths(self, src, first_port, dst, last_port, ip_src, ip_dst):
        """
        从交换机 src 到交换机 dst 选出延迟最低的路径，并为该路径中所有交换机安装流表项。
        :param src: 源交换机。
        :param first_port: 交换机 src 在该路径上的输入端口。
        :param dst: 目标交换机。
        :param last_port: 交换机 dst 在该路径上的输出端口。
        :param ip_src: 源主机 IP。
        :param ip_dst: 目标主机 IP。
        :return: 延迟最低的路径上第一个交换机的输入端口。
        """
        # 获取两两交换机之间的所有路径，并按照延迟排序
        paths_list = self.get_paths(src, dst)
        paths_list.sort(key=lambda x: self.get_path_delay(x))
        paths_with_ports = self.add_ports_to_paths(paths_list, first_port, last_port)
        optimal_path = paths_with_ports[0]

        for switch_id, ports in optimal_path.iteritems():
            datapath = self.datapath_dict[switch_id]
            ofp = datapath.ofproto
            ofp_parser = datapath.ofproto_parser
            in_port, out_port = ports

            # 匹配 IP 报文
            match_ip = ofp_parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=ip_src,
                ipv4_dst=ip_dst
            )
            # 匹配 ARP 报文
            match_arp = ofp_parser.OFPMatch(
                eth_type=0x0806,
                arp_spa=ip_src,
                arp_tpa=ip_dst,
            )
            actions = [
                ofp_parser.OFPActionOutput(out_port)
            ]
            self.add_flow(datapath, 32768, match_ip, actions, hard_timeout=10)
            self.add_flow(datapath, 1, match_arp, actions, hard_timeout=10)

        return optimal_path[src][1]

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0, hard_timeout=0):
        """
        发送流表项到交换机 datapath 中。
        :param datapath: 目标交换机。
        :param priority: 流表项的优先级。
        :param match: 流表项的匹配域。
        :param actions: 流表项的执行动作。
        :param buffer_id: buffer ID。
        :param idle_timeout:
        :param hard_timeout:
        """
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        instructions = [
            ofp_parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS,
                                             actions)
        ]

        if buffer_id:
            mod = ofp_parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                        priority=priority, match=match,
                                        idle_timeout=idle_timeout, hard_timeout=hard_timeout,
                                        instructions=instructions)
        else:
            mod = ofp_parser.OFPFlowMod(datapath=datapath, priority=priority,
                                        match=match,
                                        idle_timeout=idle_timeout, hard_timeout=hard_timeout,
                                        instructions=instructions)

        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        datapath = ev.datapath
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        self.logger.info("[state_change_handler] datapath:%s, event state:%s", datapath.id, ev.state)

        if ev.state == MAIN_DISPATCHER:
            if datapath.id and datapath.id not in self.datapath_dict:
                self.datapath_dict[datapath.id] = datapath

            # 添加 table-miss 流表项
            match_table_miss = ofp_parser.OFPMatch()
            actions = [
                ofp_parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)
            ]
            self.add_flow(datapath, 0, match_table_miss, actions)

        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapath_dict:
                del self.datapath_dict[datapath.id]
            if datapath.id in self.switch_link_dict:
                del self.switch_link_dict[datapath.id]
            if datapath.id in self.lldp_delay_dict:
                del self.lldp_delay_dict[datapath.id]
            if datapath.id in self.echo_delay_dict:
                del self.echo_delay_dict[datapath.id]
            if datapath.id in self.link_delay_dict:
                del self.link_delay_dict[datapath.id]

    @set_ev_cls(event.EventLinkAdd, MAIN_DISPATCHER)
    def link_add_handler(self, ev):
        """
        链路新增处理函数。
        """
        s1 = ev.link.src
        s2 = ev.link.dst

        self.logger.info("[link_add_handler] %s ——> %s", s1.dpid, s2.dpid)

        self.switch_link_dict.setdefault(s1.dpid, {})
        self.switch_link_dict[s1.dpid][s2.dpid] = s1.port_no
        self.switch_link_dict.setdefault(s2.dpid, {})
        self.switch_link_dict[s2.dpid][s1.dpid] = s2.port_no

    @set_ev_cls(event.EventLinkDelete, MAIN_DISPATCHER)
    def link_delete_handler(self, ev):
        """
        链路删除处理函数。
        """
        s1 = ev.link.src
        s2 = ev.link.dst

        self.logger.info("[link_delete_handler] %s ——> %s", s1.dpid, s2.dpid)

        self.switch_link_dict.setdefault(s1.dpid, {})
        if s2.dpid in self.switch_link_dict[s1.dpid]:
            del self.switch_link_dict[s1.dpid][s2.dpid]
        self.switch_link_dict.setdefault(s2.dpid, {})
        if s1.dpid in self.switch_link_dict[s2.dpid]:
            del self.switch_link_dict[s2.dpid][s1.dpid]

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """
        packet-in 报文处理函数。
        """
        msg = ev.msg
        datapath = msg.datapath
        datapath_id = datapath.id
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)
        arp_pkt = pkt.get_protocol(arp.arp)

        # lldp 数据包在另外一个 handler 中处理
        if eth_pkt.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        # 丢弃 IPv6 数据报文
        if pkt.get_protocol(ipv6.ipv6):
            match_drop = ofp_parser.OFPMatch(eth_type=eth_pkt.ethertype)
            actions = []
            self.add_flow(datapath, 1, match_drop, actions)

        src_mac = eth_pkt.src
        dst_mac = eth_pkt.dst

        self.logger.info("[packet_in_handler] datapath(ID:%d, port:%d), src:%s, dst:%s",
                         datapath_id,
                         in_port,
                         src_mac,
                         dst_mac)

        if src_mac not in self.hosts_dict:
            self.hosts_dict[src_mac] = (datapath_id, in_port)

        # 输出端口的默认值是泛洪
        out_port = ofp.OFPP_FLOOD

        if arp_pkt:
            src_ip = arp_pkt.src_ip
            dst_ip = arp_pkt.dst_ip

            if arp_pkt.opcode == arp.ARP_REPLY:
                self.host_arp_dict[src_ip] = src_mac
                src_switch, src_switch_port = self.hosts_dict[src_mac]
                dst_switch, dst_switch_port = self.hosts_dict[dst_mac]

                out_port = self.install_paths(src_switch, src_switch_port, dst_switch, dst_switch_port, src_ip, dst_ip)
                self.install_paths(dst_switch, dst_switch_port, src_switch, src_switch_port, dst_ip, src_ip)
            elif arp_pkt.opcode == arp.ARP_REQUEST:
                if dst_ip in self.host_arp_dict:
                    self.host_arp_dict[src_ip] = src_mac
                    dst_mac = self.host_arp_dict[dst_ip]
                    src_switch, src_switch_port = self.hosts_dict[src_mac]
                    dst_switch, dst_switch_port = self.hosts_dict[dst_mac]

                    out_port = self.install_paths(src_switch, src_switch_port, dst_switch, dst_switch_port, src_ip,
                                                  dst_ip)
                    self.install_paths(dst_switch, dst_switch_port, src_switch, src_switch_port, dst_ip, src_ip)

        actions = [
            ofp_parser.OFPActionOutput(out_port)
        ]
        data = None
        if msg.buffer_id == ofp.OFP_NO_BUFFER:
            data = msg.data
        out = ofp_parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(out)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def lldp_packet_in_handler(self, ev):
        """
        packet-in 报文处理函数。只处理 lldp 报文。
        """
        recv_timestamp = time.time()

        if self.switches_module is None:
            self.switches_module = lookup_service_brick("switches")
        assert self.switches_module is not None

        if not self.switches_module.link_discovery:
            return

        msg = ev.msg
        try:
            src_dpid, src_port_no = switches.LLDPPacket.lldp_parse(msg.data)
            dst_dpid = msg.datapath.id

            for port in self.switches_module.ports.keys():
                if src_dpid == port.dpid and src_port_no == port.port_no:
                    send_timestamp = self.switches_module.ports[port].timestamp

                    self.lldp_delay_dict.setdefault(src_dpid, {})
                    if send_timestamp:
                        self.lldp_delay_dict[src_dpid][dst_dpid] = recv_timestamp - send_timestamp

        except switches.LLDPPacket.LLDPUnknownFormat:
            return

    def send_echo_request(self):
        """
        对每个交换机发送 echo request 报文。
        """
        for datapath in self.datapath_dict.itervalues():
            ofp_parser = datapath.ofproto_parser
            echo_req = ofp_parser.OFPEchoRequest(datapath, data="%.12f" % time.time())
            datapath.send_msg(echo_req)

    @set_ev_cls(ofp_event.EventOFPEchoReply, MAIN_DISPATCHER)
    def echo_reply_handler(self, ev):
        """
        echo-reply 报文处理函数。
        """
        now_timestamp = time.time()
        try:
            delay = (now_timestamp - eval(ev.msg.data)) / 2
            self.echo_delay_dict[ev.msg.datapath.id] = delay
        except:
            return

    def delay_detect_loop(self):
        """
        延迟探测线程函数。
        """
        while self.is_active:
            self.send_echo_request()
            self.calculate_delay()

            self.show_link_delay()

            hub.sleep(MinDelayPathController.DELAY_DETECT_PERIOD)

            # TODO：根据延迟，实时更新交换机的流表

    def show_link_delay(self):
        """
        输出链路延迟到 log 中。
        """
        if not self.link_delay_dict:
            return

        show_msg = "----------switch link delay----------\n"
        for dp1 in self.link_delay_dict.keys():
            for dp2 in self.link_delay_dict[dp1].keys():
                delay = self.link_delay_dict[dp1][dp2]
                show_msg += "\t%d ————> %d : %.6f ms\n" % (dp1, dp2, delay * 1000)
        show_msg += "-------------------------------------\n"
        self.logger.info(show_msg)

    def calculate_delay(self):
        """
        计算交换机之间的延迟。
        """
        for dp1 in self.switch_link_dict.keys():
            self.link_delay_dict.setdefault(dp1, {})

            for dp2 in self.switch_link_dict[dp1].keys():
                if dp1 == dp2:
                    delay = 0
                else:
                    try:
                        lldp_delay1 = self.lldp_delay_dict[dp1][dp2]
                        lldp_delay2 = self.lldp_delay_dict[dp2][dp1]
                        echo_delay1 = self.echo_delay_dict[dp1]
                        echo_delay2 = self.echo_delay_dict[dp2]

                        delay = (lldp_delay1 + lldp_delay2 - echo_delay1 - echo_delay2) / 2
                        delay = max(delay, 0)
                    except:
                        # 若无延迟数据，则表明该路径可能不通
                        delay = float("inf")

                self.link_delay_dict[dp1][dp2] = delay



