"""
"""

# =========================== imports =========================================

import copy
import random

# Mote sub-modules
import MoteDefines as d

# Simulator-wide modules
import SimEngine

# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class Tsch(object):

    MINIMAL_SHARED_CELL = {
       
        'slotOffset'   : 0,
        'channelOffset': 0,
        'neighbor'     : None, # None means "any"
        'cellOptions'  : [d.CELLOPTION_TX,d.CELLOPTION_RX,d.CELLOPTION_SHARED]
    }

    def __init__(self, mote):

        # store params
        self.mote                           = mote

        # singletons (quicker access, instead of recreating every time)
        self.engine                         = SimEngine.SimEngine.SimEngine()
        self.settings                       = SimEngine.SimSettings.SimSettings()
        self.log                            = SimEngine.SimLog.SimLog().log

        # local variables
        self.schedule                       = {}      # indexed by slotOffset, contains cell
        self.txQueue                        = []
        self.neighbor_table                  = []
        self.pktToSend                      = None
        self.waitingFor                     = None
        self.channel                        = None
        self.asnLastSync                    = None
        self.isSync                         = False
        self.join_proxy                     = None
        self.iAmSendingEBs                  = False
        self.clock                          = Clock(self.mote)
        self.otherCells                     = []
        # backoff state
        self.backoff_exponent               = d.TSCH_MIN_BACKOFF_EXPONENT
        self.backoff_remaining_delay        = 0

    #======================== public ==========================================

    # getters/setters

    def getSchedule(self):
        return self.schedule

    def getTxQueue(self):
        return self.txQueue

    def getIsSync(self):
        return self.isSync

    def setIsSync(self,val):
        # set
        self.isSync      = val

        if self.isSync:
            # log
            self.log(
                SimEngine.SimLog.LOG_TSCH_SYNCED,
                {
                    "_mote_id":   self.mote.id,
                    "_mote_x" : self.mote.x,
                    "_mote_y" : self.mote.y
                }
            )

            self.asnLastSync = self.engine.getAsn()
            self._start_keep_alive_timer() # Fadoua commented this out to remove keep alive function

            # transition: listeningForEB->active
            self.engine.removeFutureEvent(      # remove previously scheduled listeningForEB cells
                uniqueTag=(self.mote.id, '_tsch_action_listeningForEB_cell')
            )
            self.tsch_schedule_next_active_cell()    # schedule next active cell
        else:
            # log
            self.log(
                SimEngine.SimLog.LOG_TSCH_DESYNCED,
                {
                    "_mote_id":   self.mote.id,
                }
            )

            self.delete_minimal_cell()
            self.mote.sf.stop()
            self.join_proxy  = None
            self.asnLastSync = None
            self.clock.desync()
            self._stop_keep_alive_timer() # Fadoua commented this out to remove keep alive function

            # transition: active->listeningForEB
            self.engine.removeFutureEvent(      # remove previously scheduled listeningForEB cells
                uniqueTag=(self.mote.id, '_tsch_action_active_cell')
            )
            self.tsch_schedule_next_listeningForEB_cell()

    def _getCells(self, neighbor, cellOptions=None):
        """
        Returns a dict containing the cells
        The dict keys are the cell slotOffset
        :param neighbor:
        :param cellOptions:
        :rtype: dict
        """
        if neighbor is not None:
            assert type(neighbor) == int

        # configure filtering condition
        if (neighbor is None) and (cellOptions is not None):    # filter by cellOptions
            condition = lambda (_, c): sorted(c['cellOptions']) == sorted(cellOptions)
        elif (neighbor is not None) and (cellOptions is None):  # filter by neighbor
            condition = lambda (_, c): c['neighbor'] == neighbor
        elif (neighbor is None) and (cellOptions is None):      # don't filter
            condition = lambda (_, c): True
        else:                                                   # filter by cellOptions and neighbor
            condition = lambda (_, c): (
                    sorted(c['cellOptions']) == sorted(cellOptions) and
                    c['neighbor'] == neighbor
            )

        # apply filter
        return dict(filter(condition, self.schedule.items()))

    def getTxCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_TX],
        )

    def getRxCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_RX],
        )

    def getTxRxSharedCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_TX, d.CELLOPTION_RX, d.CELLOPTION_SHARED],
        )

    def getDedicatedCells(self, neighbor):
        return self._getCells(
            neighbor    = neighbor,
        )

    # activate

    def startSendingEBs(self):
        self.iAmSendingEBs  = True

    # minimal

    def add_minimal_cell(self):

        self.addCell(**self.MINIMAL_SHARED_CELL)

    def delete_minimal_cell(self):
        reason = 'minimal cell'
        self.deleteCell(reason ,**self.MINIMAL_SHARED_CELL)

    # schedule interface

    def addCell(self, slotOffset, channelOffset, neighbor, cellOptions):

        assert isinstance(slotOffset, int)
        assert isinstance(channelOffset, int)
        if neighbor!=None:
            assert isinstance(neighbor, int)
        assert isinstance(cellOptions, list)

        
        # print('********* Src', self.mote.id, 'Dst', neighbor)
        # print('*********   slotoffset', slotOffset, 'channelOffset', channelOffset)
        # print('*********   self.schedule.keys() of mote',self.mote.id, self.schedule.keys())
        # print('*********   self.schedule', self.schedule)

        


        # if (len(self.mote.tsch.schedule.keys()) >= d.MSF_MAX_TABLE_SIZE_USABLE ): # the schedule is full, I reached the max_usable_size of the table
        #     # we don't have available cells right now
        #     self.log(
        #         SimEngine.SimLog.LOG_MSF_ERROR_SCHEDULE_FULL,
        #         {
        #             '_mote_id'    : self.mote.id,
        #             'neighbor'    : neighbor

        #         }
        #     )
        #     return

        # else:

        # make sure I have no activity at that slotOffset already
        assert slotOffset not in self.schedule.keys()

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_ADD_CELL,
            {
                '_mote_id':       self.mote.id,
                'slotOffset':     slotOffset,
                'channelOffset':  channelOffset,
                'neighbor':       neighbor,
                'cellOptions':    cellOptions,
            }
        )

        
        #should I double-check here if the schedule is full then drop the adding request


        # add cell
        # Fadoua: here is where the actual adding of the cell happen
        self.schedule[slotOffset] = {
            'channelOffset':      channelOffset,
            'neighbor':           neighbor,
            'cellOptions':        cellOptions,
            # per-cell statistics
            'numTx':              0,
            'numTxAck':           0,
            'numRx':              0,
        }

        # reschedule the next active cell, in case it is now earlier
        if self.getIsSync():
            self.tsch_schedule_next_active_cell()

    def deleteCell(self, reason, slotOffset, channelOffset, neighbor, cellOptions):
        assert isinstance(slotOffset, int)
        assert isinstance(channelOffset, int)
        assert (neighbor is None) or (isinstance(neighbor, int))
        assert isinstance(cellOptions, list)

        # make sure I'm removing a cell that I have in my schedule
        
        # print('-------------------------------- Mote:', self.mote.id, 'neighbor', neighbor)
        # print('the slotOffset:',slotOffset)
        # print('self.schedule[slotOffset][neighbor]', self.schedule[slotOffset]['neighbor'] )
        # print('la raison de la supression est ', reason)
        # print('the schedule keys:', self.schedule.keys())

        

        # assert slotOffset in self.schedule.keys()
        
        if (slotOffset not in self.schedule.keys()): # this is added to see if it resolve the current issue !!!!!!!!!!!!!! 
            pass

        else:

            assert self.schedule[slotOffset]['channelOffset']  == channelOffset
            assert self.schedule[slotOffset]['neighbor']       == neighbor
            assert self.schedule[slotOffset]['cellOptions']    == cellOptions

            lockedSlots= list (self.mote.sf.locked_slots)



            # log
            self.log(
                SimEngine.SimLog.LOG_TSCH_DELETE_CELL,
                {
                    '_mote_id':       self.mote.id,
                    'reason'   :      reason,
                    'slotOffset':     slotOffset,
                    'slotOffsets_inTable' : self.schedule.keys(),
                    'channelOffset':  channelOffset,
                    'neighbor':       neighbor,
                    'cellOptions':    cellOptions,
                    'locked_slots': lockedSlots
                }
            )


        # #***********************************************************************************
        # # Fadoua: I needs test again if I have an on-going transmission, leave one guard cell
        # i = 0
        # txQueue = self.mote.tsch.getTxQueue()
        # pending_DATA_pkts = False
        # code_test = False
        # list_old_parents= self.mote.rpl.get_list_of_old_parents()

        
        
            # if simply delete the minimal cell
            if(reason == "minimal cell"):
                del self.schedule[slotOffset]

            else:

                source= self.engine.motes[neighbor]

                # print('mote', self.mote.id, 'schedule table keys', self.schedule.keys() )
                # print('neighbor', neighbor, 'schedule table keys',source.tsch.schedule.keys() )

                # source= self.engine.motes[neighbor]
                pp= source.rpl.getPreferredParent() # the current preferred parent
                hello= source.tsch.getDedicatedCells(self.mote.id)
                # print('**************************Helloooo list to test', hello)

                list_cell_to_neighbor= self.getDedicatedCells(neighbor)

                # for the root of the DAG
                if (self.mote.dagRoot is True):
    
                    pending_DATA_pkts = self.double_check_txQueue(neighbor)

                    if (pending_DATA_pkts == True):
                        if (len(list_cell_to_neighbor) <= 1):
                            pass
                        else:
                            del self.schedule[slotOffset]

                    elif ((pp == self.mote.id) and (len(list_cell_to_neighbor) <= 1)): # the current preferred parent of source is the root and we have only one dedicated cell -- do not supress it
                        pass       
                    
                    else:
                        del self.schedule[slotOffset]

                # not a DAG root
                else:
                
                    if (source.id != 0): 
                    
                        list_old_parents= source.rpl.get_list_of_old_parents()
                        # print('--------- old parent list of mote: ', source.id, 'is: ', list_old_parents )

                        if((pp==self.mote.id) or (self.mote.id in list_old_parents)): # in mote is a current preferred parent of source or mote was one of the old preferred parents of source
                        
                            # in case of routing loop -- routing loops are tolerated by RPL to some extend
                            # so whnever I have a routing loop, I need double-check both queues before taking
                            # a decision of cell deletion from schedule
                            # some times I have routing loops that occured in the past and now I need clean up the schedules
                            # decisions need to be made cerefully with regards to all possible cases

                            mote_list_old_parents= self.mote.rpl.get_list_of_old_parents()
                        
                            if (neighbor in mote_list_old_parents): # an old routing loop was there
                            
                                pending_DATA_pkts = source.tsch.double_check_txQueue(self.mote.id)
                                # print('pending_DATA_pkts', pending_DATA_pkts)
                                self.decision_to_delete_cell(neighbor, slotOffset, pending_DATA_pkts)
                            
                            else: 
                                pending_DATA_pkts = self.double_check_txQueue(neighbor)
                                # print('helooooooooooooooooo pending_DATA_pkts', pending_DATA_pkts)
                                self.decision_to_delete_cell(neighbor, slotOffset, pending_DATA_pkts)

                        else: # in case of transmitter
                        
                            pending_DATA_pkts = source.tsch.double_check_txQueue(self.mote.id)
                            # print('taaaaaaaaaaa pending_DATA_pkts', pending_DATA_pkts)
                            self.decision_to_delete_cell(neighbor, slotOffset, pending_DATA_pkts)


                    else: # if the source (neighbor) is the DAG root

                        pending_DATA_pkts = source.tsch.double_check_txQueue(self.mote.id)
                        self.decision_to_delete_cell(neighbor, slotOffset, pending_DATA_pkts)


        # reschedule the next active cell, in case it is now earlier
        if self.getIsSync():
            self.tsch_schedule_next_active_cell()

    # data interface with upper layers

    def double_check_txQueue(self, neighbor):
        
        source= self.engine.motes[neighbor]
        txQueue = source.tsch.getTxQueue()
        
        i=0
        
        pending_DATA_pkts = False
                
        while (i<len(txQueue) and (pending_DATA_pkts == False)):    

            if (txQueue[i]['type'] == 'DATA') and (txQueue[i]['mac']['dstMac'] == self.mote.id) and  (txQueue[i]['mac']['retriesLeft'] >= 0):
                pending_DATA_pkts = True
            else:
                pending_DATA_pkts = False 
            i += 1 

        return (pending_DATA_pkts)

    def decision_to_delete_cell (self, neighbor, slotOffset, pending_DATA_pkts):

        # assert slotOffset in self.schedule.keys()

        source= self.engine.motes[neighbor]
        SourcePreferredParent= source.rpl.getPreferredParent()


        list_cell_to_neighbor= self.getDedicatedCells(neighbor)
        # print('list_cell from mote', self.mote.id, 'to mote', neighbor, list_cell_to_neighbor)

        list_cell_to_mote= source.tsch.getDedicatedCells(self.mote.id)
        # print('list_cell_to_mote from mote', neighbor, 'to mote',self.mote.id , list_cell_to_mote)

        resultat = ''
        cause = ''
        shared_cells = self.mote.tsch.getTxRxSharedCells(neighbor)

        if (pending_DATA_pkts is True):
            cell = self.schedule[slotOffset]

            if (len(list_cell_to_neighbor) <= 1):# or (len(list_cell_to_mote) != len(list_cell_to_neighbor)) :# (len(list_cell_to_mote) <= 1): # I added the second part to make sure that I have always the same 
                # print('condition 1')
                resultat = 'not deleted'
                cause = 'list size less or equal to one'
                pass
            
            else:
                
                # print('condition 3')
                
                if (slotOffset in source.sf.locked_slots):
                    resultat = 'not deleted'
                    cause = 'locked slot'
                    pass
                
                elif (SourcePreferredParent == self.mote.id):
                    if ((d.CELLOPTION_SHARED in cell['cellOptions']) and (len(shared_cells) <= 1 )):
                        resultat = 'not deleted'
                        cause = 'shared slot and routing loop mote is pref p of neighbor'
                        pass
                    else:
                        del self.schedule[slotOffset]
                        resultat = 'deleted'
                        cause = 'no loops and tab size more than one'
                        # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)
                
                elif (self.mote.rpl.getPreferredParent() == neighbor):
                    if ((d.CELLOPTION_SHARED in cell['cellOptions']) and (len(shared_cells) <= 1 )):
                        resultat = 'not deleted'
                        cause = 'shared slot and routing loop neighbor is pref p of mote'
                        pass
                    else:
                        del self.schedule[slotOffset]
                        resultat = 'deleted'
                        cause = 'no loops and tab size more than one'
                        # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)

                else:
                    del self.schedule[slotOffset]
                    resultat = 'deleted'
                    cause = 'no loops and tab size more than one'
                    # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)

        
        


        # elif ((SourcePreferredParent == self.mote.id) and (len(list_cell_to_neighbor) <= 1)): # the preferred mote of source is the neighbor and we have only one dedicated cell -- do not supress it
        #     pass
        # elif ((self.mote.rpl.getPreferredParent() == neighbor) and (len(list_cell_to_neighbor) <= 1)): # the preferred mote of source is the neighbor and we have only one dedicated cell -- do not supress it
        #     pass 



        elif ((pending_DATA_pkts is False) and (SourcePreferredParent == self.mote.id)):
            if(len(list_cell_to_neighbor) <= 1): # the preferred mote of source is the neighbor and we have only one dedicated cell -- do not supress it
                pass
            else:
                cell = self.schedule[slotOffset]
                if ((d.CELLOPTION_SHARED in cell['cellOptions']) and (len(shared_cells) <= 1 )):
                    resultat = 'not deleted'
                    cause = 'shared slot and routing loop mote is pref p of neighbor'
                    pass
                else:
                    del self.schedule[slotOffset]
                    resultat = 'deleted'
                    cause = 'no loops, no pending data and tab size more than one'
                    # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)


        elif ((pending_DATA_pkts is False) and (self.mote.rpl.getPreferredParent() == neighbor)):
            if (len(list_cell_to_neighbor) <= 1): # the preferred mote of source is the neighbor and we have only one dedicated cell -- do not supress it
                resultat = 'not deleted'
                cause = 'shared slot and routing loop neighbor is pref p of mote'
                pass
            else:
                cell = self.schedule[slotOffset]
                if ((d.CELLOPTION_SHARED in cell['cellOptions']) and (len(shared_cells) <= 1 )):
                    resultat = 'not deleted'
                    cause = 'shared slot and routing loop mote is pref p of neighbor'
                    pass
                else:
                    del self.schedule[slotOffset]
                    resultat = 'deleted'
                    cause = 'no loops, no pending data and tab size more than one'
                    # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)

        

        else:   
            # print('condition 4')
            if (slotOffset in source.sf.locked_slots):
                resultat = 'not deleted'
                cause = 'locked slot'
                pass
            else:
                del self.schedule[slotOffset]
                resultat = 'deleted'
                cause = 'no loops, no pending data and tab size more than one'
                # print('cell deleted successfully from', self.mote.id, 'to neighbor', neighbor)


        

        # log
        self.log(
            SimEngine.SimLog.LOG_MSF_DELETING_CELLS_OP,
            {
                '_mote_id':       self.mote.id,
                'neighbor':       neighbor,
                'result':    resultat,
                'cause': cause
            }
        )




    def enqueue(self, packet, priority=False):

        assert packet['type'] != d.PKT_TYPE_EB
        assert 'srcMac' in packet['mac']
        assert 'dstMac' in packet['mac']

        goOn = True

        # check there is space in txQueue
        if goOn:
            if (
                    (priority is False)
                    and
                    (len(self.txQueue) >= d.TSCH_QUEUE_SIZE)
                ):
                # my TX queue is full

                # drop
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_TXQUEUE_FULL,
                )

                # couldn't enqueue
                goOn = False

        # check that I have cell to transmit on
        if goOn:
            
# #********************************************************************** normally, no need for this since we keep the gard cell

#             if (packet['type'] == d.PKT_TYPE_DATA) and (not self.getTxCells()) and (not self.getRxCells()): # added Fadoua: if a DATA packet and no dedicated cell to transmit
#                 # whether drop the DATA packet or call the add cell
#                 neighbor_id = packet['mac']['dstMac']
#                 self.mote.sf._request_adding_cells(
#                         neighbor_id    = neighbor_id,
#                         num_txrx_cells = 1
#                     )
# #**********************************************************************
            


            if (not self.getTxCells()) and (not self.getTxRxSharedCells()):
                # I don't have any cell to transmit on

                # drop
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_NO_TX_CELLS,
                )

                # couldn't enqueue
                goOn = False

        # if I get here, everyting is OK, I can enqueue
        if goOn:
            # set retriesLeft which should be renewed at every hop
            packet['mac']['retriesLeft'] = d.TSCH_MAXTXRETRIES
            if priority:
                self.txQueue.insert(0, packet)
            else:
                # add to txQueue
                self.txQueue    += [packet]

        return goOn

    # interface with radio

    def txDone(self, isACKed):
        assert isACKed in [True,False]

        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        assert slotOffset in self.getSchedule()
        assert d.CELLOPTION_TX in cell['cellOptions']
        assert self.waitingFor == d.WAITING_FOR_TX

        # log
        # assert slot in self.schedule.keys()

        slotoffsets = self.schedule.keys()
        otherC = []

        # import pdb
        #pdb.set_trace()

        for key, vals in self.schedule.items():
            # mytuple=()
            mytuple=(key, vals['channelOffset'], vals['neighbor'])
            otherC.append(mytuple)


        self.log(
            SimEngine.SimLog.LOG_TSCH_TXDONE,
            {
                
                'NbrOfCells':     len(self.schedule.keys()),# added Fadoua 
                'TSCH_schedule':  otherC,# added Fadoua 
                'selectedCell':   self.schedule[slotOffset], # added Fadoua 
                '_mote_id':       self.mote.id,
                'channel':        self.channel,
                'packet':         self.pktToSend,
                'isACKed':        isACKed,

            }
        )


        #*************************************************************************************************
        # print('the schedule of mote', self.mote.id, 'is cell:', slotOffset, self.getSchedule()[slotOffset])

        if self.pktToSend['mac']['dstMac'] == d.BROADCAST_ADDRESS:
            # I just sent a broadcast packet

            assert self.pktToSend['type'] in [d.PKT_TYPE_EB,d.PKT_TYPE_DIO]
            assert isACKed==False

            # EBs are never in txQueue, no need to remove.
            if self.pktToSend['type'] == d.PKT_TYPE_DIO:
                self.getTxQueue().remove(self.pktToSend)

        else:
            # I just sent a unicast packet...

            # TODO send txDone up; need a more general way
            if (
                    (isACKed is True)
                    and
                    (self.pktToSend['type'] == d.PKT_TYPE_SIXP)
                ):
                self.mote.sixp.recv_mac_ack(self.pktToSend)
            self.mote.rpl.indicate_tx(cell, self.pktToSend['mac']['dstMac'], isACKed)

            # update the backoff exponent
            self._update_backoff_state(
                isRetransmission = self._is_retransmission(self.pktToSend),
                isSharedLink     = d.CELLOPTION_SHARED in cell['cellOptions'],
                isTXSuccess      = isACKed
            )

            if isACKed:
                # ... which was ACKed

                # update schedule stats
                cell['numTxAck'] += 1

                # time correction
                if self.clock.source == self.pktToSend['mac']['dstMac']: # this must be for a keep alive pckt because it is the one which dstMac=self.clock.source: Fadoua
                    self.asnLastSync = asn # ACK-based sync
                    self.clock.sync()
                    self._reset_keep_alive_timer() # Fadoua commented this out to remove keep alive function

                # remove packet from queue
                self.getTxQueue().remove(self.pktToSend)

            else:
                # ... which was NOT ACKed

                # decrement 'retriesLeft' counter associated with that packet
                assert self.pktToSend['mac']['retriesLeft'] >= 0
                self.pktToSend['mac']['retriesLeft'] -= 1

                # drop packet if retried too many time
                if self.pktToSend['mac']['retriesLeft'] < 0:

                    # remove packet from queue
                    self.getTxQueue().remove(self.pktToSend)

                    # drop
                    self.mote.drop_packet(
                        packet  = self.pktToSend,
                        reason  = SimEngine.SimLog.DROPREASON_MAX_RETRIES,
                    )

        # end of radio activity, not waiting for anything
        self.waitingFor = None
        self.pktToSend  = None

    def rxDone(self, packet):

        # local variables
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength

        # copy the received packet to a new packet instance since the passed
        # "packet" should be kept as it is so that Connectivity can use it
        # after this rxDone() process.
        new_packet = copy.deepcopy(packet)
        packet = new_packet

        # make sure I'm in the right state
        if self.getIsSync():
            assert slotOffset in self.getSchedule()
            assert d.CELLOPTION_RX in self.getSchedule()[slotOffset]['cellOptions']
            assert self.waitingFor == d.WAITING_FOR_RX

        # not waiting for anything anymore
        self.waitingFor = None

        # abort if received nothing (idle listen)
        if packet==None:
            return False # isACKed

        # add the source mote to the neighbor list if it's not listed yet
        if packet['mac']['srcMac'] not in self.neighbor_table:
            self.neighbor_table.append(packet['mac']['srcMac'])

        # abort if I received a frame for someone else
        if packet['mac']['dstMac'] not in [d.BROADCAST_ADDRESS, self.mote.id]:
            return False # isACKed

        # if I get here, I received a frame at the link layer (either unicast for me, or broadcast)

        

        # #---- added Fadoua 
        # if packet['type'] == d.PKT_TYPE_DATA:
        #     self.settings.count= self.count_DATA_pkts_motes_without_dedicated_cell(packet)
        

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_RXDONE,
            {
                '_mote_id':        self.mote.id,
                'packet':          packet
                # 'count': self.settings.count
            }
        )

        # time correction using keep alive pckts: Fadoua
        if self.clock.source == packet['mac']['srcMac']:
            self.asnLastSync = asn # packet-based sync
            self.clock.sync()
            self._reset_keep_alive_timer() # Fadoua commented this out to remove keep alive function

        # update schedule stats
        if self.getIsSync():
            self.getSchedule()[slotOffset]['numRx'] += 1

        if   packet['mac']['dstMac'] == self.mote.id:
            # link-layer unicast to me

            # ACK frame
            isACKed = True

            # dispatch to the right upper layer
            if   packet['type'] == d.PKT_TYPE_SIXP:
                self.mote.sixp.recv_packet(packet)
            elif packet['type'] == d.PKT_TYPE_KEEP_ALIVE:
                # do nothing but send back an ACK
                pass
            elif 'net' in packet:
                self.mote.sixlowpan.recvPacket(packet)
            else:
                raise SystemError()

        elif packet['mac']['dstMac']==d.BROADCAST_ADDRESS:
            # link-layer broadcast

            # do NOT ACK frame (broadcast)
            isACKed = False

            # dispatch to the right upper layer
            if   packet['type'] == d.PKT_TYPE_EB:
                self._tsch_action_receiveEB(packet)
            elif 'net' in packet:
                assert packet['type']==d.PKT_TYPE_DIO
                self.mote.sixlowpan.recvPacket(packet)
            else:
                raise SystemError()

        else:
            raise SystemError()

        return isACKed

    def remove_frame_from_tx_queue(self, type, dstMac=None):
        i = 0
        while i<len(self.txQueue):
            if (
                    (self.txQueue[i]['type'] == type)
                    and
                    (
                        (dstMac is None)
                        or
                        (self.txQueue[i]['mac']['dstMac'] == dstMac)
                    )
                ):
                del self.txQueue[i]
            else:
                i += 1

    #======================== private ==========================================

    # listeningForEB

    def tsch_schedule_next_listeningForEB_cell(self):

        assert not self.getIsSync()

        # schedule at next ASN
        self.engine.scheduleAtAsn(
            asn              = self.engine.getAsn()+1,
            cb               = self._tsch_action_listeningForEB_cell,
            uniqueTag        = (self.mote.id, '_tsch_action_listeningForEB_cell'),
            intraSlotOrder   = d.INTRASLOTORDER_STARTSLOT,
        )

    def _tsch_action_listeningForEB_cell(self):
        """
        active slot starts, while mote is listening for EBs
        """

        assert not self.getIsSync()

        # choose random channel
        channel = random.randint(0, self.settings.phy_numChans-1)

        # start listening
        self.mote.radio.startRx(
            channel = channel,
        )

        # indicate that we're waiting for the RX operation to finish
        self.waitingFor = d.WAITING_FOR_RX

        # schedule next listeningForEB cell
        self.tsch_schedule_next_listeningForEB_cell()

    # active cell

    def tsch_schedule_next_active_cell(self):

        assert self.getIsSync()

        asn        = self.engine.getAsn()
        tsCurrent  = asn % self.settings.tsch_slotframeLength

        # find closest active slot in schedule

        if not self.schedule:
            # print('_tsch_action_active_cell is called to be removed in tsch_schedule_next_active_cell')
            self.engine.removeFutureEvent(uniqueTag=(self.mote.id, '_tsch_action_active_cell'))
            return

        tsDiffMin             = None

        for (slotOffset, cell) in self.schedule.items():
            if   slotOffset == tsCurrent:
                tsDiff        = self.settings.tsch_slotframeLength
            elif slotOffset > tsCurrent:
                tsDiff        = slotOffset-tsCurrent
            elif slotOffset < tsCurrent:
                tsDiff        = (slotOffset+self.settings.tsch_slotframeLength)-tsCurrent
            else:
                raise SystemError()

            if (not tsDiffMin) or (tsDiff < tsDiffMin):
                tsDiffMin     = tsDiff

        # schedule at that ASN
        self.engine.scheduleAtAsn(
            asn              = asn+tsDiffMin,
            cb               = self._tsch_action_active_cell,
            uniqueTag        = (self.mote.id, '_tsch_action_active_cell'),
            intraSlotOrder   = d.INTRASLOTORDER_STARTSLOT,
        )

    def _tsch_action_active_cell(self):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength

        # print ('------------------ self.mote.id', self.mote.id, 'slotOffset', slotOffset, 'self.schedule[slotoffset]', self.schedule[slotOffset])


        cell       = self.schedule[slotOffset]
        
        # except KeyError:
        #     import pdb
        #     pdb.set_trace()
        # make sure this is an active slot
        assert slotOffset in self.schedule

        # make sure we're not in the middle of a TX/RX operation
        assert self.waitingFor == None

        # make sure we are not busy sending a packet
        assert self.pktToSend == None

        # execute cell
        if cell['neighbor'] is None:
            # on a shared cell
            if sorted(cell['cellOptions']) == sorted([d.CELLOPTION_TX,d.CELLOPTION_RX,d.CELLOPTION_SHARED]):
                # on minimal cell
                # try to find a packet to neighbor to which I don't have any dedicated cell(s)...
                if not self.pktToSend:
                    for pkt in self.txQueue:
                        if  (
                                # DIOs and EBs always on minimal cell
                                (
                                    pkt['type'] in [d.PKT_TYPE_DIO,d.PKT_TYPE_EB]
                                )
                                or
                                # other frames on the minimal cell if no dedicated cells to the nextHop
                                (
                                    len(self.getTxCells(pkt['mac']['dstMac'])) == 0
                                    and
                                    len(self.getTxRxSharedCells(pkt['mac']['dstMac'])) == 0
                                )
                            ):
                            self.pktToSend = pkt
                            break

                # retransmission backoff algorithm
                if (
                        self.pktToSend
                        and
                        self._is_retransmission(self.pktToSend)
                    ):
                        if self.backoff_remaining_delay > 0:
                            # need to wait for retransmission
                            self.pktToSend = None
                            # decrement the remaining delay
                            self.backoff_remaining_delay -= 1
                        else:
                            # ready for retransmission
                            pass

                # ... if no such packet, probabilistically generate an EB
                if not self.pktToSend:
                    if self.mote.clear_to_send_EBs_DATA():
                        prob = self.settings.tsch_probBcast_ebProb/(1+len(self.neighbor_table))
                        if (
                                (random.random() < prob)
                                and
                                (self.iAmSendingEBs)
                            ):
                            self.pktToSend = self._create_EB()

                # send packet, or receive
                if self.pktToSend:
                    self._tsch_action_TX(self.pktToSend)
                    # print('src', self.pktToSend['mac']['srcMac'], 'dst', self.pktToSend['mac']['dstMac']) #*******************************************************************
                else:
                    self._tsch_action_RX()
            else:
                # We don't support shared cells which are not [TX=1, RX=1,
                # SHARED=1]
                raise NotImplementedError()
        else:
            # on a dedicated cell

            # find a possible pktToSend first
            _pktToSend = None
            for pkt in self.txQueue:
                if pkt['mac']['dstMac'] == cell['neighbor']:
                    _pktToSend = pkt
                    break
            # HACK: don't transmit a frame on a shared link if it has a
            # dedicated TX link to the destination and doesn't have a
            # dedicated RX link from the destination. In such a case, the
            # shared link is used as if it's a dedicated RX.
            if (
                    (d.CELLOPTION_SHARED in cell['cellOptions'])
                    and
                    (len(self.getTxCells(cell['neighbor'])) > 0)
                    and
                    (len(self.getRxCells(cell['neighbor'])) == 0)
                ):
                _pktToSend = None

            # retransmission backoff algorithm
            if (
                    (_pktToSend is not None)
                    and
                    (d.CELLOPTION_SHARED in cell['cellOptions'])
                    and
                    self._is_retransmission(_pktToSend)
                ):
                    if self.backoff_remaining_delay > 0:
                        # need to wait for retransmission
                        _pktToSend = None
                        # decrement the remaining delay
                        self.backoff_remaining_delay -= 1
                    else:
                        # ready for retransmission
                        pass

            if (
                    (_pktToSend is not None)
                    and
                    (d.CELLOPTION_TX in cell['cellOptions'])
                ):
                # we're going to transmit the packet
                self.pktToSend = _pktToSend
                self._tsch_action_TX(self.pktToSend)

            elif d.CELLOPTION_RX in cell['cellOptions']:
                # receive
                self._tsch_action_RX()
            else:
                # do nothing
                pass

            # notify SF
            if d.CELLOPTION_TX in cell['cellOptions']:
                self.mote.sf.indication_dedicated_tx_cell_elapsed(
                    cell    = cell,
                    used    = (self.pktToSend is not None),
                )

        # schedule next active cell
        self.tsch_schedule_next_active_cell()

    def _tsch_action_TX(self,pktToSend):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        # update cell stats
        cell['numTx'] += 1

        # Seciton 4.3 of draft-chang-6tisch-msf-01: "When NumTx reaches 256,
        # both NumTx and NumTxAck MUST be divided by 2.  That is, for example,
        # from NumTx=256 and NumTxAck=128, they become NumTx=128 and
        # NumTxAck=64."
        if cell['numTx'] == 256:
            cell['numTx']    /= 2
            cell['numTxAck'] /= 2

        # send packet to the radio
        self.mote.radio.startTx(
            channel          = cell['channelOffset'],
            packet           = pktToSend,
        )

        # indicate that we're waiting for the TX operation to finish
        self.waitingFor      = d.WAITING_FOR_TX
        self.channel         = cell['channelOffset']


        # if (pktToSend['type']== 'DATA'):
        #     print('asn:', asn, 'cell:', slotOffset, self.channel, 'Packet:', pktToSend['type'] ) #********************************************************

    def _tsch_action_RX(self):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        # start listening
        self.mote.radio.startRx(
            channel          = cell['channelOffset'],
        )

        # indicate that we're waiting for the RX operation to finish
        self.waitingFor      = d.WAITING_FOR_RX
        self.channel         = cell['channelOffset']

    # EBs

    def _create_EB(self):

        # create
        newEB = {
            'type':               d.PKT_TYPE_EB,
            'app': {
                'join_metric':    self.mote.rpl.getDagRank() - 1,
            },
            'mac': {
                'srcMac':         self.mote.id,            # from mote
                'dstMac':         d.BROADCAST_ADDRESS,     # broadcast
            },
        }

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_EB_TX,
            {
                "_mote_id":  self.mote.id,
                "packet":    newEB,
            }
        )

        return newEB

    def _tsch_action_receiveEB(self, packet):

        assert packet['type'] == d.PKT_TYPE_EB

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_EB_RX,
            {
                "_mote_id":  self.mote.id,
                "packet":    packet,
            }
        )

        # abort if I'm the root
        if self.mote.dagRoot:
            return

        if not self.getIsSync():
            # receiving EB while not sync'ed

            # I'm now sync'ed!
            self.clock.sync(packet['mac']['srcMac'])
            self.setIsSync(True) # mote

            # the mote that sent the EB is now by join proxy
            self.join_proxy = packet['mac']['srcMac']

            # add the minimal cell to the schedule (read from EB)
            self.add_minimal_cell() # mote

            # trigger join process
            self.mote.secjoin.startJoinProcess()

    # Retransmission backoff algorithm
    def _is_retransmission(self, packet):
        assert packet is not None
        return packet['mac']['retriesLeft'] < d.TSCH_MAXTXRETRIES

    def _decide_backoff_delay(self):
        # Section 6.2.5.3 of IEEE 802.15.4-2015: "The MAC sublayer shall delay
        # for a random number in the range 0 to (2**BE - 1) shared links (on
        # any slotframe) before attempting a retransmission on a shared link."
        self.backoff_remaining_delay = random.randint(
            0,
            pow(2, self.backoff_exponent) - 1
        )

    def _reset_backoff_state(self):
        old_be = self.backoff_exponent
        self.backoff_exponent = d.TSCH_MIN_BACKOFF_EXPONENT
        self.log(
            SimEngine.SimLog.LOG_TSCH_BACKOFF_EXPONENT_UPDATED,
            {
                '_mote_id': self.mote.id,
                'old_be'  : old_be,
                'new_be'  : self.backoff_exponent
            }
        )
        self._decide_backoff_delay()

    def _increase_backoff_state(self):
        old_be = self.backoff_exponent
        # In Figure 6-6 of IEEE 802.15.4, BE (backoff exponent) is updated as
        # "BE - min(BE 0 1, macMinBe)". However, it must be incorrect. The
        # right formula should be "BE = min(BE + 1, macMaxBe)", that we apply
        # here.
        self.backoff_exponent = min(
            self.backoff_exponent + 1,
            d.TSCH_MAX_BACKOFF_EXPONENT
        )
        self.log(
            SimEngine.SimLog.LOG_TSCH_BACKOFF_EXPONENT_UPDATED,
            {
                '_mote_id': self.mote.id,
                'old_be'  : old_be,
                'new_be'  : self.backoff_exponent
            }
        )
        self._decide_backoff_delay()

    def _update_backoff_state(
            self,
            isRetransmission,
            isSharedLink,
            isTXSuccess
        ):
        if isSharedLink:
            if isTXSuccess:
                # Section 6.2.5.3 of IEEE 802.15.4-2015: "A successful
                # transmission in a shared link resets the backoff window to
                # the minimum value."
                self._reset_backoff_state()
            else:
                if isRetransmission:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff window
                    # increases for each consecutive failed transmission in a
                    # shared link."
                    self._increase_backoff_state()
                else:
                    # First attempt to transmit the packet
                    #
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "A device upon
                    # encountering a transmission failure in a shared link
                    # shall initialize the BE to macMinBe."
                    self._reset_backoff_state()

        else:
            # dedicated link (which is different from a dedicated *cell*)
            if isTXSuccess:
                # successful transmission
                if len(self.getTxQueue()) == 0:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff
                    # window is reset to the minimum value if the transmission
                    # in a dedicated link is successful and the transmit queue
                    # is then empty."
                    self._reset_backoff_state()
                else:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff
                    # window does not change when a transmission is successful
                    # in a dedicated link and the transmission queue is still
                    # not empty afterwards."
                    pass
            else:
                # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff window
                # does not change when a transmission is a failure in a
                # dedicated link."
                pass

    # Synchronization / Keep-Alive
    def _send_keep_alive_message(self):
        
        # print('clock source of mote', self.mote.id)
        # assert self.clock.source is not None
        
        if not self.getIsSync():  # this is added to see if it resolves the problem of mote sending keepAlive message while it is desynched
            return

        else:

            packet = {
                'type': d.PKT_TYPE_KEEP_ALIVE,
                'mac': {
                    'srcMac': self.mote.id,
                    'dstMac': self.clock.source
                }
            }
            self.enqueue(packet)
        # the next keep-alive event will be scheduled on receiving an ACK

    def _start_keep_alive_timer(self):
        assert self.settings.tsch_keep_alive_interval >= 0
        if (
                (self.settings.tsch_keep_alive_interval == 0)
                or
                (self.mote.dagRoot is True)
            ):
            # do nothing
            pass
        else:
            # the clock drift of the child against the parent should be less
            # than macTsRxWait/2 so that they can communicate with each
            # other. Their clocks can be off by one clock interval at the
            # most. This means, the clock difference between the child and the
            # parent could be clock_interval just after synchronization. then,
            # the possible minimum guard time is ((macTsRxWait / 2) -
            # clock_interval). When macTsRxWait is 2,200 usec and
            # clock_interval is 30 usec, the minimum guard time is 1,070
            # usec. they will be desynchronized without keep-alive in 16
            # seconds as the paper titled "Adaptive Synchronization in
            # IEEE802.15.4e Networks" describes.
            #
            # the keep-alive interval should be configured in config.json with
            # "tsch_keep_alive_interval".
            self.engine.scheduleIn(
                delay          = self.settings.tsch_keep_alive_interval,
                cb             = self._send_keep_alive_message,
                uniqueTag      = self._get_keep_alive_event_tag(),
                intraSlotOrder = d.INTRASLOTORDER_STACKTASKS
            )

    def _stop_keep_alive_timer(self):
        self.engine.removeFutureEvent(
            uniqueTag = self._get_keep_alive_event_tag()
        )

    def _reset_keep_alive_timer(self):
        self._stop_keep_alive_timer()
        self._start_keep_alive_timer()

    def _get_keep_alive_event_tag(self):
        return '{0}-{1}'.format(self.mote.id, 'tsch.keep_alive_event')


class Clock(object):
    def __init__(self, mote):
        # singleton
        self.engine   = SimEngine.SimEngine.SimEngine()
        self.settings = SimEngine.SimSettings.SimSettings()

        # local variables
        self.mote = mote

        # instance variables which can be accessed directly from outside
        self.source = None

        # private variables
        self._clock_interval = 1.0 / self.settings.tsch_clock_frequency
        self._error_rate     = self._initialize_error_rate()

        self.desync()

    @staticmethod
    def get_clock_by_mote_id(mote_id):
        engine = SimEngine.SimEngine.SimEngine()
        mote = engine.get_mote_by_id(mote_id)
        return mote.tsch.clock

    def desync(self):
        self.source             = None
        self._clock_off_on_sync = 0
        self._accumulated_error = 0
        self._last_clock_access = None

    def sync(self, clock_source=None):
        if self.mote.dagRoot is True:
            # if you're the DAGRoot, you should have the perfect clock from the
            # point of view of the network.
            self._clock_off_on_sync = 0
        else:
            if clock_source is None:
                assert self.source is not None
            else:
                self.source = clock_source

            # the clock could be off by between 0 and 30 usec (clock interval)
            # from the clock source when 32.768 Hz oscillators are used on the
            # both sides. in addition, the clock source also off from a certain
            # amount of time from its source.
            off_from_source = random.random() * self._clock_interval
            source_clock = self.get_clock_by_mote_id(self.source)
            self._clock_off_on_sync = off_from_source + source_clock.get_drift()

        self._accumulated_error = 0
        self._last_clock_access = self.engine.getAsn()

    def get_drift(self):
        if self.mote.dagRoot is True:
            # if we're the DAGRoot, we are the clock source of the entire
            # network. our clock never drifts from itself. Its clock drift is
            # taken into accout by motes who use our clock as their reference
            # clock.
            error = 0
        else:
            assert self._last_clock_access <= self.engine.getAsn()
            slot_duration = self.engine.settings.tsch_slotDuration
            elapsed_slots = self.engine.getAsn() - self._last_clock_access
            elapsed_time  = elapsed_slots * slot_duration
            error = elapsed_time * self._error_rate

        # update the variables
        self._accumulated_error += error
        self._last_clock_access = self.engine.getAsn()

        # return the result
        return self._clock_off_on_sync + self._accumulated_error

    def _initialize_error_rate(self):
        # private variables:
        # the clock drifts by its error rate. for simplicity, we double the
        # error rate to express clock drift from the time source. That is,
        # our clock could drift by 30 ppm at the most and the clock of time
        # source also could drift as well ppm. Then, our clock could drift
        # by 60 ppm from the clock of the time source.
        #
        # we assume the error rate is constant over the simulation time.
        max_drift = (
            float(self.settings.tsch_clock_max_drift_ppm) / pow(10, 6)
        )
        return random.uniform(-1 * max_drift * 2, max_drift * 2)
