"""
6LoWPAN layer including reassembly/fragmentation
"""

# =========================== imports =========================================

from abc import abstractmethod
import copy
import math
import random

# Simulator-wide modules
import SimEngine
import MoteDefines as d
from ast import literal_eval as make_tuple # added Fadoua


# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class Sixlowpan(object):

    def __init__(self, mote):

        # store params
        self.mote                 = mote

        # singletons (quicker access, instead of recreating every time)
        self.settings             = SimEngine.SimSettings.SimSettings()
        self.engine               = SimEngine.SimEngine.SimEngine()
        self.log                  = SimEngine.SimLog.SimLog().log

        # local variables
        self.fragmentation        = globals()[self.settings.fragmentation](self)

        # neighbor_cache: indexed by IPv6 address, maintain MAC addresses
        self.neighbor_cache       = {}

        # # added Fadoua, this is how to declare a variable dictionary to be global
        try:
            _ = Nbr_DATA_pkts_crossing
        except NameError:
            global Nbr_DATA_pkts_crossing
            Nbr_DATA_pkts_crossing = {}


    #======================== public ==========================================

    def sendPacket(self, packet, link_local=False):
        assert sorted(packet.keys()) == sorted(['type','app','net'])
        assert packet['type'] in [
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_DATA,
        ]
        assert 'srcIp' in packet['net']
        assert 'dstIp' in packet['net']

        goOn = True

        # put hop_limit field to the net header
        packet['net']['hop_limit'] = d.IPV6_DEFAULT_HOP_LIMIT

        # FIXME: this option will be removed when IPv6 address is properly
        # expressed in this simulator.
        if link_local is True:
            packet['net']['link_local'] = True
        else:
            packet['net']['link_local'] = False

        # mark a downward packet like 'O' option in RPL Option defined by RFC
        # 6553
        if self.mote.dagRoot:
            packet['net']['downward'] = True
        else:
            packet['net']['downward'] = False

       
        # added Fadoua
        # self.update_preferred_parent_data_pkts()



        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_TX,
            {
                '_mote_id':       self.mote.id,
                'packet':         packet,
            }
        )

        # add source route, if needed
        if goOn:
            if (
                    (self.mote.dagRoot)
                    and
                    (
                        ('link_local' not in packet['net'])
                        or
                        (packet['net']['link_local'] is False)
                    )
                    and
                    (packet['net']['dstIp'] not in self.neighbor_cache)
                ):
                sourceRoute = self.mote.rpl.computeSourceRoute(packet['net']['dstIp'])
                if sourceRoute==None:

                    # we cannot find a next-hop; drop this packet
                    self.mote.drop_packet(
                        packet  = packet,
                        reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                    )

                    # stop handling this packet
                    goOn = False
                else:
                    assert 1 < len(sourceRoute)
                    packet['net']['dstIp'] = sourceRoute.pop(0)
                    packet['net']['sourceRoute'] = sourceRoute

        # find link-layer destination
        if goOn:
            dstMac = self.find_nexthop_mac_addr(packet)
            if dstMac == None:
                # we cannot find a next-hop; drop this packet
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                )
                # stop handling this packet
                goOn = False

        # add MAC header
        if goOn:
            packet['mac'] = {
                'srcMac': self.mote.id,
                'dstMac': dstMac
            }

        # cut packet into fragments
        if goOn:
            frags = self.fragmentation.fragmentPacket(packet)

        # enqueue each fragment
        if goOn:
            for frag in frags:
                self.mote.tsch.enqueue(frag)

    #**************************************************************** added Fadoua
    def count_DATA_pkts_motes_without_dedicated_cell(self, packet):
        # for quick access; get preferred parent
        neighbor_id = packet['mac']['srcMac']
        dedicated_cells = self.mote.tsch.getDedicatedCells(neighbor_id)

        if len(dedicated_cells) == 0: # if we don't have a dedicated cell yet
            tup = str(((self.mote.id, neighbor_id)))
            
            if (packet['type'] == d.PKT_TYPE_DATA): # added Fadoua: if a DATA packet and no dedicated cell to transmit
                
                if (tup not in Nbr_DATA_pkts_crossing.keys()):
                    Nbr_DATA_pkts_crossing[tup] = str(1)
                else:
                    Nbr_DATA_pkts_crossing[tup] = str(int(Nbr_DATA_pkts_crossing[tup]) + 1)
            else:
                pass

        return(Nbr_DATA_pkts_crossing)
    
    #**************************************************************** added Fadoua
    def update_preferred_parent_data_pkts(self):
        # at this level, the condition to change the preferredParent is the rank_difference
        # What I would like to do also, is to trigger the preferredParent change whenever I have too 
        # many DATA packets being sent from a mote and its preferredParent through the shared cell
        # because the process of dedicated cell allocation is not being concluded.
        # so we count the number of DATA packets exchanged between mote and its preferredParent, 
        # if this number exceeds a threshold, trigger the preferredParent change
  
        current_mote_id= self.mote.id
        preferred_parent = self.mote.rpl.getPreferredParent()
       
        for tuple_motes, val in self.settings.count.items():

            tup= make_tuple(tuple_motes)
            dst, src = tup # we need make sure that dst is a preferred parent of src
            
            # print('src', src, 'dst', dst)

            if ((current_mote_id == src) and (dst == preferred_parent) and (int(val) > d.MAX_DATA_PKTS_THROUGHT_SHARED_CELL)): # sending a packet
                # print('*** valeur of data pkts between', src, dst, 'is',  val)
                
                self.mote.rpl.update_preferred_parent('EXTRA')

                # I should reset the counters once the preferred parent is changed 

            
            else: 
                pass
                # do nothing at this level




    def recvPacket(self, packet):

        assert packet['type'] in [
            d.PKT_TYPE_DATA,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_FRAG,
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
        ]

        goOn = True
        
        # added Fadoua
        # count= {}
        if packet['type'] == d.PKT_TYPE_DATA:
            self.settings.count= self.count_DATA_pkts_motes_without_dedicated_cell(packet)

        # print('**********************************************')

        # addd Fadoua 
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        schedule       = self.mote.tsch.getSchedule()
        
        cell={}
        if slotOffset in self.mote.tsch.getSchedule():
        
            cell = schedule[slotOffset] 

        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_RX,
            {
                '_mote_id':     self.mote.id,
                'packet'  :     packet,
                'count'   :     self.settings.count, # added Fadoua
                'selectedCell':   cell
            
            }
        )

        # add the source mode to the neighbor_cache if it's on-link
        # FIXME: IPv6 prefix should be examined
        if (
                ('srcIp' in packet['net'])
                and
                (packet['mac']['srcMac'] == packet['net']['srcIp'])
                and
                (packet['net']['srcIp'] not in self.neighbor_cache)
            ):
            self.neighbor_cache[packet['net']['srcIp']] = packet['mac']['srcMac']

        # hand fragment to fragmentation sublayer. Returns a packet to process further, or else stop.
        if goOn:
            if packet['type'] == d.PKT_TYPE_FRAG:
                packet = self.fragmentation.fragRecv(packet)
                if not packet:
                    goOn = False

            # source routing header
            elif 'sourceRoute' in packet['net']:
                packet['net']['dstIp'] = packet['net']['sourceRoute'].pop(0)
                if len(packet['net']['sourceRoute']) == 0:
                    del packet['net']['sourceRoute']

        # handle packet
        if goOn:
            if  (
                    packet['type']!=d.PKT_TYPE_FRAG # in case of fragment forwarding
                    and
                    (
                        (packet['net']['dstIp'] == self.mote.id)
                        or
                        (packet['net']['dstIp'] == d.BROADCAST_ADDRESS)
                    )
                ):
                # packet for me

                # dispatch to upper component
                if   packet['type'] in [d.PKT_TYPE_JOIN_REQUEST,d.PKT_TYPE_JOIN_RESPONSE]:
                    self.mote.secjoin.receive(packet)
                elif packet['type'] == d.PKT_TYPE_DAO:
                    self.mote.rpl.action_receiveDAO(packet)
                elif packet['type'] == d.PKT_TYPE_DIO:
                    self.mote.rpl.action_receiveDIO(packet)
                elif packet['type'] == d.PKT_TYPE_DIS:
                    self.mote.rpl.action_receiveDIS(packet)
                elif packet['type'] == d.PKT_TYPE_DATA:
                    self.mote.app.recvPacket(packet)

            else:
                # packet not for me

                # forward
                self.forward(packet)

    def forward(self, rxPacket):
        # packet can be:
        # - an IPv6 packet (which may need fragmentation)
        # - a fragment (fragment forwarding)

        assert 'type' in rxPacket
        assert 'net' in rxPacket

        goOn = True

        if (
                ('hop_limit' in rxPacket['net'])
                and
                (rxPacket['net']['hop_limit'] < 2)
            ):
            # we shouldn't receive any frame having hop_limit of 0
            assert rxPacket['net']['hop_limit'] == 1
            self.mote.drop_packet(
                packet = rxPacket,
                reason = SimEngine.SimLog.DROPREASON_TIME_EXCEEDED
            )
            goOn = False

        # === create forwarded packet
        if goOn:
            fwdPacket             = {}
            # type
            fwdPacket['type']     = copy.deepcopy(rxPacket['type'])
            # app
            if 'app' in rxPacket:
                fwdPacket['app']  = copy.deepcopy(rxPacket['app'])
            # net
            fwdPacket['net']      = copy.deepcopy(rxPacket['net'])
            if 'hop_limit' in fwdPacket['net']:
                assert fwdPacket['net']['hop_limit'] > 1
                fwdPacket['net']['hop_limit'] -= 1

            # mac
            if fwdPacket['type'] == d.PKT_TYPE_FRAG:
                # fragment already has mac header (FIXME: why?)
                fwdPacket['mac']  = copy.deepcopy(rxPacket['mac'])
            else:
                # find next hop
                dstMac = self.find_nexthop_mac_addr(fwdPacket)
                if dstMac==None:
                    # we cannot find a next-hop; drop this packet
                    self.mote.drop_packet(
                        packet  = rxPacket,
                        reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                    )
                    # stop handling this packet
                    goOn = False
                else:
                    # add MAC header
                    fwdPacket['mac'] = {
                        'srcMac': self.mote.id,
                        'dstMac': dstMac
                    }

        # log
        if goOn:
            self.log(
                SimEngine.SimLog.LOG_SIXLOWPAN_PKT_FWD,
                {
                    '_mote_id':       self.mote.id,
                    'packet':         fwdPacket,
                }
            )

        # cut the forwarded packet into fragments
        if goOn:
            if fwdPacket['type']==d.PKT_TYPE_FRAG:
                fwdFrags = [fwdPacket] # don't re-frag a frag
            else:
                fwdFrags = self.fragmentation.fragmentPacket(fwdPacket)

        # enqueue all frags
        if goOn:
            for fwdFrag in fwdFrags:
                self.mote.tsch.enqueue(fwdFrag)

    #======================== private ==========================================

    def find_nexthop_mac_addr(self, packet):
        dstIp = packet['net']['dstIp']
        mac_addr = None

        if   dstIp == d.BROADCAST_ADDRESS:
            mac_addr = d.BROADCAST_ADDRESS

        elif self.mote.dagRoot:
            if   dstIp in self.neighbor_cache:
                # on-link
                mac_addr = self.neighbor_cache[dstIp]
            else:
                # off-link
                mac_addr = None
        else:
            if   self.mote.dodagId is None:
                # upward during secure join process
                mac_addr = self.mote.tsch.join_proxy
            elif (
                    (
                        ('link_local' in packet['net'])
                        and
                        (packet['net']['link_local'] is True)
                    )
                    or
                    (
                        ('downward' in packet['net'])
                        and
                        (packet['net']['downward'] is True)
                    )
                ):
                if dstIp in self.neighbor_cache:
                    # on-link
                    mac_addr = self.neighbor_cache[dstIp]
                else:
                    mac_addr = None
            else:
                # use the default router (preferred parent)
                mac_addr = self.mote.rpl.getPreferredParent()

        return mac_addr


class Fragmentation(object):
    """The base class for forwarding implementations of fragments
    """

    def __init__(self, sixlowpan):

        # store params
        self.sixlowpan            = sixlowpan

        # singletons (quicker access, instead of recreating every time)
        self.settings             = SimEngine.SimSettings.SimSettings()
        self.engine               = SimEngine.SimEngine.SimEngine()
        self.log                  = SimEngine.SimLog.SimLog().log

        # local variables
        self.mote                 = sixlowpan.mote
        self.next_datagram_tag    = random.randint(0, 2**16-1)
        # "reassembly_buffers" has mote instances as keys. Each value is a list.
        # A list is indexed by incoming datagram_tags.
        #
        # An element of the list a dictionary consisting of three key-values:
        # "net", "expiration" and "fragments".
        #
        # - "net" has srcIp and dstIp of the packet
        # - "fragments" holds received fragments, although only their
        # datagram_offset and lengths are stored in the "fragments" list.
        self.reassembly_buffers   = {}

    #======================== public ==========================================

    def fragmentPacket(self, packet):
        """Fragments a packet into fragments

        Returns a list of fragments, possibly with one element.

        First fragment (no app field):
            {
                'net': {
                    'srcIp':                src_ip_address,
                    'dstIp':                dst_ip_address,
                    'hop_limit':            hop_limit,
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                   ['sourceRoute':          [...]]
                }
            }

        Subsequent fragments (no app, no srcIp/dstIp):
            {
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                }
            }

        Last fragment (app, no srcIp/dstIp):
            {
                'app':                      (if applicable)
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                    'original_packet_type': original_packet_type,
                }
            }
        """
        assert packet['type'] in [
            d.PKT_TYPE_DATA,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
        ]
        assert 'type' in packet
        assert 'net'  in packet

        returnVal = []

        if  self.settings.tsch_max_payload_len < packet['net']['packet_length']:
            # the packet needs fragmentation

            # choose tag (same for all fragments)
            outgoing_datagram_tag = self._get_next_datagram_tag()
            number_of_fragments   = int(math.ceil(float(packet['net']['packet_length']) / self.settings.tsch_max_payload_len))
            datagram_offset       = 0

            for i in range(0, number_of_fragments):

                # common part of fragment packet
                fragment = {
                    'type':                d.PKT_TYPE_FRAG,
                    'net': {
                        'datagram_size':   packet['net']['packet_length'],
                        'datagram_tag':    outgoing_datagram_tag,
                        'datagram_offset': datagram_offset
                    }
                }

                # put additional fields to the first and the last fragment
                if   i == 0:
                    # first fragment

                    # copy 'net' header
                    for key, value in packet['net'].items():
                        fragment['net'][key] = value
                    if 'sourceRoute' in packet['net']:
                        fragment['net']['sourceRoute']      = copy.deepcopy(packet['net']['sourceRoute'])
                elif i == (number_of_fragments - 1):
                    # the last fragment

                    # add original_packet_type and 'app' field
                    fragment['app']                         = copy.deepcopy(packet['app'])
                    fragment['net']['original_packet_type'] = packet['type']

                # populate packet_length
                if  (
                        (i == 0) and
                        ((packet['net']['packet_length'] % self.settings.tsch_max_payload_len) > 0)
                    ):
                    # slop is in the first fragment
                    fragment['net']['packet_length'] = packet['net']['packet_length'] % self.settings.tsch_max_payload_len
                else:
                    fragment['net']['packet_length'] = self.settings.tsch_max_payload_len

                # update datagram_offset which will be used for the next fragment
                datagram_offset += fragment['net']['packet_length']

                # copy the MAC header
                fragment['mac'] = copy.deepcopy(packet['mac'])

                # add the fragment to a returning list
                returnVal += [fragment]

                # log
                self.log(
                    SimEngine.SimLog.LOG_SIXLOWPAN_FRAG_GEN,
                    {
                        '_mote_id': self.mote.id,
                        'packet':   fragment
                    }
                )

        else:
            # the input packet doesn't need fragmentation
            returnVal += [packet]

        return returnVal

    @abstractmethod
    def fragRecv(self, fragment):
        """This method is supposed to return a packet to be processed further

        This could return None.
        """
        raise NotImplementedError() # abstractmethod

    def reassemblePacket(self, fragment):
        srcMac                    = fragment['mac']['srcMac']
        datagram_size             = fragment['net']['datagram_size']
        datagram_offset           = fragment['net']['datagram_offset']
        incoming_datagram_tag     = fragment['net']['datagram_tag']
        buffer_lifetime           = d.SIXLOWPAN_REASSEMBLY_BUFFER_LIFETIME / self.settings.tsch_slotDuration

        self._delete_expired_reassembly_buffer()

        # make sure we can allocate a reassembly buffer if necessary
        if (srcMac not in self.reassembly_buffers) or (incoming_datagram_tag not in self.reassembly_buffers[srcMac]):
            # dagRoot has no memory limitation for reassembly buffer
            if not self.mote.dagRoot:
                total_reassembly_buffers_num = 0
                for i in self.reassembly_buffers:
                    total_reassembly_buffers_num += len(self.reassembly_buffers[i])
                if total_reassembly_buffers_num == self.settings.sixlowpan_reassembly_buffers_num:
                    # no room for a new entry
                    self.mote.drop_packet(
                        packet = fragment,
                        reason = SimEngine.SimLog.DROPREASON_REASSEMBLY_BUFFER_FULL,
                    )
                    return

            # create a new reassembly buffer
            if srcMac not in self.reassembly_buffers:
                self.reassembly_buffers[srcMac] = {}
            if incoming_datagram_tag not in self.reassembly_buffers[srcMac]:
                self.reassembly_buffers[srcMac][incoming_datagram_tag] = {
                    'expiration': self.engine.getAsn() + buffer_lifetime,
                    'fragments': []
                }

        if datagram_offset not in map(lambda x: x['datagram_offset'], self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments']):

            if fragment['net']['datagram_offset'] == 0:
                # store srcIp and dstIp which only the first fragment has
                self.reassembly_buffers[srcMac][incoming_datagram_tag]['net'] = copy.deepcopy(fragment['net'])
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_size']
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_offset']
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_tag']

            self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments'].append({
                'datagram_offset': datagram_offset,
                'fragment_length': fragment['net']['packet_length']
            })
        else:
            # it's a duplicate fragment
            return

        # check whether we have a full packet in the reassembly buffer
        total_fragment_length = sum([f['fragment_length'] for f in self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments']])
        assert total_fragment_length <= datagram_size
        if total_fragment_length < datagram_size:
            # reassembly is not completed
            return

        # construct an original packet
        packet = copy.copy(fragment)
        packet['type'] = fragment['net']['original_packet_type']
        packet['net'] = copy.deepcopy(self.reassembly_buffers[srcMac][incoming_datagram_tag]['net'])
        packet['net']['packet_length'] = datagram_size

        # reassembly is done, delete buffer
        del self.reassembly_buffers[srcMac][incoming_datagram_tag]
        if len(self.reassembly_buffers[srcMac]) == 0:
            del self.reassembly_buffers[srcMac]

        return packet

    # ======================= private =========================================    

    def _get_next_datagram_tag(self):
        ret = self.next_datagram_tag
        self.next_datagram_tag = (ret + 1) % 65536
        return ret

    def _delete_expired_reassembly_buffer(self):
        if len(self.reassembly_buffers) == 0:
            return

        for srcMac in self.reassembly_buffers.keys():
            for incoming_datagram_tag in self.reassembly_buffers[srcMac].keys():
                # delete expired reassembly buffer
                if self.reassembly_buffers[srcMac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.reassembly_buffers[srcMac][incoming_datagram_tag]

            # delete an reassembly buffer entry if it's empty
            if len(self.reassembly_buffers[srcMac]) == 0:
                del self.reassembly_buffers[srcMac]

class PerHopReassembly(Fragmentation):
    """
    RFC4944-like per-hop fragmentation and reassembly.
    """
    #======================== public ==========================================

    def fragRecv(self, fragment):
        """Reassemble an original packet
        """
        return self.reassemblePacket(fragment)


class FragmentForwarding(Fragmentation):
    """
    Fragment forwarding, per https://tools.ietf.org/html/draft-watteyne-6lo-minimal-fragment
    """

    def __init__(self, sixlowpan):
        super(FragmentForwarding, self).__init__(sixlowpan)
        self.vrb_table       = {}

    #======================== public ==========================================

    def fragRecv(self, fragment):

        srcMac                = fragment['mac']['srcMac']
        datagram_size         = fragment['net']['datagram_size']
        datagram_offset       = fragment['net']['datagram_offset']
        incoming_datagram_tag = fragment['net']['datagram_tag']
        packet_length         = fragment['net']['packet_length']
        entry_lifetime        = d.SIXLOWPAN_VRB_TABLE_ENTRY_LIFETIME / self.settings.tsch_slotDuration

        self._delete_expired_vrb_table_entry()

        # handle first fragments
        if datagram_offset == 0:

            if fragment['net']['dstIp'] != self.mote.id:

                dstMac = self.sixlowpan.find_nexthop_mac_addr(fragment)
                if dstMac == None:
                    # no route to the destination
                    return

            # check if we have enough memory for a new entry if necessary
            if self.mote.dagRoot:
                # dagRoot has no memory limitation for VRB Table
                pass
            else:
                total_vrb_table_entry_num = sum([len(e) for _, e in self.vrb_table.items()])
                assert total_vrb_table_entry_num <= self.settings.fragmentation_ff_vrb_table_size
                if total_vrb_table_entry_num == self.settings.fragmentation_ff_vrb_table_size:
                    # no room for a new entry
                    self.mote.drop_packet(
                        packet = fragment,
                        reason = SimEngine.SimLog.DROPREASON_VRB_TABLE_FULL,
                    )
                    return


            if srcMac not in self.vrb_table:
                self.vrb_table[srcMac] = {}

            # By specification, a VRB Table entry is supposed to have:
            # - incoming srcMac
            # - incoming datagram_tag
            # - outgoing dstMac (nexthop)
            # - outgoing datagram_tag

            if incoming_datagram_tag in self.vrb_table[srcMac]:
                # duplicate first fragment is silently discarded
                return
            else:
                self.vrb_table[srcMac][incoming_datagram_tag] = {}

            if fragment['net']['dstIp']  == self.mote.id:
                # this is a special entry for fragments destined to the mote
                self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] = None
            else:
                self.vrb_table[srcMac][incoming_datagram_tag]['dstMac']                = dstMac
                self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] = self._get_next_datagram_tag()

            self.vrb_table[srcMac][incoming_datagram_tag]['expiration'] = self.engine.getAsn() + entry_lifetime

            if 'missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy:
                self.vrb_table[srcMac][incoming_datagram_tag]['next_offset'] = 0

        # when missing_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the expected one, update the next_offset
        # - otherwise, delete the corresponding VRB table entry
        if (
                ('missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy) and
                (srcMac in self.vrb_table) and
                (incoming_datagram_tag in self.vrb_table[srcMac])
           ):
            if datagram_offset == self.vrb_table[srcMac][incoming_datagram_tag]['next_offset']:
                self.vrb_table[srcMac][incoming_datagram_tag]['next_offset'] += packet_length
            else:
                del self.vrb_table[srcMac][incoming_datagram_tag]
                if len(self.vrb_table[srcMac]) == 0:
                    del self.vrb_table[srcMac]

        # find entry in VRB table and forward fragment
        if (srcMac in self.vrb_table) and (incoming_datagram_tag in self.vrb_table[srcMac]):
            # VRB entry found!

            if self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] is None:
                # fragment for me: do not forward but reassemble. ret will have
                # either a original packet or None
                ret = self.reassemblePacket(fragment)

            else:
                # need to create a new packet in order to distinguish between the
                # received packet and a forwarding packet.
                fwdFragment = {
                    'type':       copy.deepcopy(fragment['type']),
                    'net':        copy.deepcopy(fragment['net']),
                    'mac': {
                        'srcMac': self.mote.id,
                        'dstMac': self.vrb_table[srcMac][incoming_datagram_tag]['dstMac']
                    }
                }

                # forwarding fragment should have the outgoing datagram_tag
                fwdFragment['net']['datagram_tag'] = self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag']

                # copy app field if necessary
                if 'app' in fragment:
                    fwdFragment['app'] = copy.deepcopy(fragment['app'])

                ret = fwdFragment

        else:
            # no VRB table entry is found
            ret = None

        # when last_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the last fragment of a packet, delete the corresponding entry
        # - otherwise, do nothing
        if (
                ('last_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy) and
                (srcMac in self.vrb_table) and
                (incoming_datagram_tag in self.vrb_table[srcMac]) and
                ((datagram_offset + packet_length) == datagram_size)
           ):
            del self.vrb_table[srcMac][incoming_datagram_tag]
            if len(self.vrb_table[srcMac]) == 0:
                del self.vrb_table[srcMac]

        return ret

    #======================== private ==========================================

    def _delete_expired_vrb_table_entry(self):
        if len(self.vrb_table) == 0:
            return

        for srcMac in self.vrb_table.keys():
            for incoming_datagram_tag in self.vrb_table[srcMac].keys():
                # too old
                if self.vrb_table[srcMac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.vrb_table[srcMac][incoming_datagram_tag]
            # empty
            if len(self.vrb_table[srcMac]) == 0:
                del self.vrb_table[srcMac]
