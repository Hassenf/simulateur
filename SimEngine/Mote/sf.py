# =========================== imports =========================================

import random
import sys
from abc import abstractmethod

import SimEngine
import MoteDefines as d

# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class SchedulingFunction(object):
    def __new__(cls, mote):
        settings    = SimEngine.SimSettings.SimSettings()
        class_name  = 'SchedulingFunction{0}'.format(settings.sf_class)
        return getattr(sys.modules[__name__], class_name)(mote)

class SchedulingFunctionBase(object):

    def __init__(self, mote):

        # store params
        self.mote            = mote

        # singletons (quicker access, instead of recreating every time)
        self.settings        = SimEngine.SimSettings.SimSettings()
        self.engine          = SimEngine.SimEngine.SimEngine()
        self.log             = SimEngine.SimLog.SimLog().log

    # ======================= public ==========================================

    # === admin

    @abstractmethod
    def start(self):
        """
        tells SF when should start working
        """
        raise NotImplementedError() # abstractmethod

    @abstractmethod
    def stop(self):
        '''
        tells SF when should stop working
        '''
        raise NotImplementedError() # abstractmethod

    # === indications from other layers

    @abstractmethod
    def indication_dedicated_tx_cell_elapsed(self,cell,used):
        """[from TSCH] just passed a dedicated TX cell. used=False means we didn't use it.

        """
        raise NotImplementedError() # abstractmethod

    @abstractmethod
    def indication_parent_change(self, old_parent, new_parent):
        """
        [from RPL] decided to change parents.
        """
        raise NotImplementedError() # abstractmethod

    @abstractmethod
    def detect_schedule_inconsistency(self, peerMac):
        raise NotImplementedError() # abstractmethod

    @abstractmethod
    def recv_request(self, packet):
        raise NotImplementedError() # abstractmethod


class SchedulingFunctionSFNone(SchedulingFunctionBase):

    def __init__(self, mote):
        super(SchedulingFunctionSFNone, self).__init__(mote)

    def start(self):
        pass # do nothing

    def stop(self):
        pass # do nothing

    def indication_dedicated_tx_cell_elapsed(self,cell,used):
        pass # do nothing

    def indication_parent_change(self, old_parent, new_parent):
        pass # do nothing

    def detect_schedule_inconsistency(self, peerMac):
        pass # do nothing

    def recv_request(self, packet):
        pass # do nothing


class SchedulingFunctionMSF(SchedulingFunctionBase):

    DEFAULT_CELL_LIST_LEN = 5
    TXRX_CELL_OPT = [d.CELLOPTION_TX, d.CELLOPTION_RX, d.CELLOPTION_SHARED]
    TX_CELL_OPT   = [d.CELLOPTION_TX]
    RX_CELL_OPT   = [d.CELLOPTION_RX]

    def __init__(self, mote):
        # initialize parent class
        super(SchedulingFunctionMSF, self).__init__(mote)

        # (additional) local variables
        self.num_cells_passed = 0       # number of dedicated cells passed
        self.num_cells_used   = 0       # number of dedicated cells used
        self.cell_utilization = 0
        self.locked_slots     = set([]) # slots in on-going ADD transactions
        
        self.count_adding_requests = [0 for i in range(self.settings.exec_numMotes)] # added Fadoua, to count the add requests after we exceed the max of cells per pref parent

    # ======================= public ==========================================

    # === admin

    def start(self):
        if self.mote.dagRoot:
            # do nothing
            pass
        else:
            # self.dedicated_cell_request_to_preferredParent() # added Fadoua
            self._housekeeping_collision()

            self._refresh_scheduling_tables()
            
    def stop(self):
        # FIXME: need something before stopping the operation such as freeing
        # all the allocated cells
        if self.mote.dagRoot:
            # do nothing
            pass
        else:
            self.engine.removeFutureEvent('_housekeeping_collision')

    # === indications from other layers

    def indication_dedicated_tx_cell_elapsed(self, cell, used):
        assert cell['neighbor'] is not None

        preferred_parent = self.mote.rpl.getPreferredParent()
        if cell['neighbor'] == preferred_parent:

            # HACK: we don't transmit a frame on a shared link if it
            # has a dedicated TX link to the destination and doesn't
            # have a dedicated RX link from the destination (see
            # tsch.py). Because of that, we exclude the shared link
            # (the very first dedicated cell to the perferred parent)
            # from TX cells for this housekeeping when it has at least
            # one TX dedicate link.
            if (
                    (len(self.mote.tsch.getTxCells(cell['neighbor'])) > 0)
                    and
                    (d.CELLOPTION_SHARED in cell['cellOptions'])
                ):
                # ignore this TX/(RX)/SHARED cell for this housekeeping round
                pass
            else:
                # increment cell passed counter
                self.num_cells_passed += 1

                # increment cell used counter
                if used:
                    self.num_cells_used += 1

            # adapt number of cells if necessary
            if d.MSF_MAX_NUMCELLS <= self.num_cells_passed:
                self._adapt_to_traffic(preferred_parent)
                self._reset_cell_counters()

    def indication_parent_change(self, old_parent, new_parent):
        assert old_parent != new_parent

        # allocate the same number of cells to the new parent as it has for the
        # old parent; note that there could be three types of cells:
        # (TX=1,RX=1,SHARED=1), (TX=1), and (RX=1)
        if old_parent is None:
            num_tx_cells = 0
            num_rx_cells = 0
        else:
            num_tx_cells = len(self.mote.tsch.getTxCells(old_parent))
            num_rx_cells = len(self.mote.tsch.getRxCells(old_parent))
        
        #*************************************************************************
        # use identity function to find the slotOffset, add a random value and make sue I don't have these slotoffsets in my schedules
        # use another function to find channelOffset

        
        parent = self.engine.get_mote_by_id(new_parent)
        slot = self.mote.id
        lockedSlots = self.locked_slots
        
        ParentlockedSlots = parent.sf.locked_slots
        
        while (slot in self.mote.tsch.schedule.keys()) or (slot in parent.tsch.schedule.keys()) or (slot in lockedSlots) or (slot in ParentlockedSlots): 
            rand = random.randint(0, 100-self.mote.id)
            slot = rand+self.mote.id # I need create some randomness here and test if the selectd slotOffset already exists in the schedule keys
        
        selected_slot = slot
        # print('selected slot is', selected_slot, 'between',self.mote.id , new_parent )
        channel = self.mote.id%16

    
        # No need to test the length of the scheduling tables here because when I change preferred parent
        # some cells would be deleted and I will be able to add a first dedicated cell to the new parent
        self.mote.tsch.addCell(
                    slotOffset         = slot,
                    channelOffset      = channel,
                    neighbor           = new_parent,
                    cellOptions        = self.TXRX_CELL_OPT
                )      
        
        parent.tsch.addCell(
                    slotOffset         = slot,
                    channelOffset      = channel,
                    neighbor           = self.mote.id,
                    cellOptions        = self.TXRX_CELL_OPT
                )

        self.log(
                    SimEngine.SimLog.LOG_MSF_DEDICATED_CELL_TO_PREFERREDPARENT,
                    {
                        '_mote_id'    : self.mote.id,
                        'preferredParent_id'    : new_parent,
                        'tx_add_first_dedicated_cell' : True
                    }
                )


        #*************************************************************************
        # clear all the cells allocated for the old parent
        if old_parent is not None:
            
            # added Fadoua: 
            # at this level I need test if there is pending application 
            # DATA packets in the txQueue that emerged in the past but failed to be 
            # transmitted (maxreties >0 ). If it is the case then supress all dedicated 
            # cells to the previous preferred parent but one. The remaining cell will be 
            # the guard cell. It will be used to transmit pending DATA packets and it 
            # will be supressed later on from the schedule.
            # start by scanning the content of the txQueue and detect pending DATA packets
            # to the previous preferred parent

            i = 0
            txQueue = self.mote.tsch.getTxQueue()
            pending_DATA_pkts = False
            test = False

            while (i<len(txQueue) and pending_DATA_pkts == False):
                if (
                    (txQueue[i]['type'] == 'DATA') and (txQueue[i]['mac']['dstMac'] == old_parent) and  (txQueue[i]['mac']['retriesLeft'] >0)
                    
                ):
                    pending_DATA_pkts = True
                
                else:
                    i += 1
                    pending_DATA_pkts = False

                test= pending_DATA_pkts
   

            if (test== True): # leave one guard cell to the old preferred parent

            
                # adde Fadoua:
                # if the test is True, then there are applicaton DATA packets queued for the old preferred parent
                # in which case, I will need leave a guardcell if scenario = "withGuardCell" or delete all cells to 
                # ld preferred parent and redirect the buffered packets to the new parent if scenario = "packetRedirection"
                # therefore a second test is needed here before going a step further


                if (self.settings.scenario == "withGuardCell"):

                    # if the length of the cell_list is one, then no need to send a clear request if we still have pending data
                    cell_list = self.mote.tsch.getDedicatedCells(old_parent)
                    if (len(cell_list) <= 1):
                        pass
                    else:

                        # import pdb
                        # pdb.set_trace()

                        self.mote.sixp.send_request(
                            code     = 'clear cells to old parent and leave one',
                            dstMac   = old_parent,
                            command  = d.SIXP_CMD_CLEAR,
                            callback = lambda event, packet: self._clear_cells_old_parent(old_parent)
                        )
                    

                        # self._clear_cells_old_parent(old_parent)
                    # print('un delete request est envoye de mote', self.mote.id, 'vers le neighbor', old_parent)
                        
                elif (self.settings.scenario == "packetRedirection"):

                    # First, redirect the pending data packets to the new parent 
                    # extra tests are needed inside the function itself to see if the old pref parent is 
                    # the sink probably it is better to keep the packet as is and to keep a guard cell to
                    # send it to the root, then refresh the scheduling tables
                    # if the final destination of the packet is the old preferred parent, then leave a guard cell
                    # if the packet is a transiting one ( its final destination is not the old pref parent), then 
                    # update the destMac and leave no cells to old pref parent in the schedule

                    # Redirecting packets should happen at level 3 (IP level). The packet that is created with 
                    # selected scrMac and dstMac is in the buffer until it is its turn to be transmitted
                    # once it tries to make it up to the subsequent layer (from mac level to ip level), and since
                    # no cell is there to listen for that transmission, RPL takes over to override the dstIP of the packet
                    # Next, the packet will be routed through the new route 

                    # implementation wise, we can do this by overriding the dstMac of the packet (not real) 

                    self.packetRedirection(old_parent, new_parent)


                    # Second, delete all dedicated cells to the old parent

                    self.mote.sixp.send_request( # clear out all dedicated cells to the old preferred parent
                        code     = 'clear all cells to old parent',
                        dstMac   = old_parent,
                        command  = d.SIXP_CMD_CLEAR,
                        callback = lambda event, packet: self._clear_cells(old_parent)
                    )





            else:
                self.mote.sixp.send_request( # clear out all dedicated cells to the old preferred parent
                    code     = 'clear all cells to old parent',
                    dstMac   = old_parent,
                    command  = d.SIXP_CMD_CLEAR,
                    callback = lambda event, packet: self._clear_cells(old_parent)
                )

    

    # **************************************************************************
    # added Fadoua: This function is to double-check the pending application packets in the 
    # mote's tx buffer. Whnever a mote switches its preferred parent a test is performed.

    def packetRedirection(self, old_parent, new_parent):


        # Oldparent = self.engine.get_mote_by_id(old_parent)
        # Newparent = self.engine.get_mote_by_id(new_parent)

        txQueue = self.mote.tsch.getTxQueue()

        for i in range(len(txQueue)):
            
            if ( (txQueue[i]['type'] == 'DATA') and (txQueue[i]['mac']['dstMac'] == old_parent) and  (txQueue[i]['mac']['retriesLeft'] >0) ):
                
                txQueue[i]['mac']['dstMac'] = new_parent # set the detMac to the new parent


                self.log(
                    SimEngine.SimLog.LOG_MSF_REDIRECT_CELLS_TO_NEW_PARENT,
                    {
                        '_mote_id'    : self.mote.id,
                        'old_parent'  : old_parent,
                        'new_parent'  : new_parent
                    }
                )






    # **********************************  OBSOLETE  **************************************
    # added Fadoua: This is to double-check if a mote has requested a dedicated cell to its preferredParent
    # double-check the schedule and the on-going transactions
    def dedicated_cell_request_to_preferredParent(self):
        
        # for quick access; get preferred parent
        preferred_parent = self.mote.rpl.getPreferredParent()
        dedicated_cells = self.mote.tsch.getDedicatedCells(preferred_parent)
        tx_add_cell_request_queued = False
        event = None

        if len(dedicated_cells) == 0: # if we don't have a dedicated cell yet
            
            src_txQueue = self.mote.tsch.getTxQueue()
            # dst_rxQueue = preferred_parent.tsch.getTxQueue

            for i in range(len(src_txQueue)):
                if (
                    (src_txQueue[i]['type'] == d.PKT_TYPE_SIXP)
                    and
                    (src_txQueue[i]['app']['code'] == 'ADD')
                    and
                    (src_txQueue[i]['mac']['dstMac'] == preferred_parent)
                ):
                    tx_add_cell_request_queued=True
                    event=d.SIXP_RC_SUCCESS
                else:
                    tx_add_cell_request_queued=False
                    event=d.SIXP_CALLBACK_EVENT_FAILURE

            # prepare _callback which is passed to SixP.send_request()
            def callback(event):
                if event == d.SIXP_CALLBACK_EVENT_FAILURE:
                    self._request_adding_cells(
                    neighbor_id    = preferred_parent,
                    num_txrx_cells = 1,
                    code           = 'DEDICATED_CELL_TO_PREFF_PARENT'
                    )


            if(tx_add_cell_request_queued == True):
                pass
            else:
            # added to log: Fadoua
                self.log(
                    SimEngine.SimLog.LOG_MSF_DEDICATED_CELL_TO_PREFERREDPARENT,
                    {
                        '_mote_id'    : self.mote.id,
                        'preferredParent_id'    : preferred_parent,
                        'tx_add_cell_request_queued' : tx_add_cell_request_queued
                    }
                )
                
                callback(event)
            # schedule next verfication leave this for later -- make sure it works fine at least for one time
        self.engine.scheduleAtAsn(
            asn=self.engine.asn + d.MSF_DEDICATED_CELLS_ALLOCATION_PERIOD,
            cb=self.dedicated_cell_request_to_preferredParent,
            uniqueTag=('SimEngine', 'dedicated_cell_request_to_preferredParent'),
            intraSlotOrder=d.INTRASLOTORDER_STACKTASKS,
        )

 
    # **************************************************************************
  





    def detect_schedule_inconsistency(self, peerMac):
        # send a CLEAR request to the peer
        self.mote.sixp.send_request(
            code     = 'inconsistency',
            dstMac   = peerMac,
            command  = d.SIXP_CMD_CLEAR,
            callback = lambda event, packet: self._clear_cells(peerMac)
        )

    def recv_request(self, packet):
        if   packet['app']['code'] == d.SIXP_CMD_ADD:
            self._receive_add_request(packet)
        elif packet['app']['code'] == d.SIXP_CMD_DELETE:
            self._receive_delete_request(packet)
        elif packet['app']['code'] == d.SIXP_CMD_CLEAR:
            self._receive_clear_request(packet)
        elif packet['app']['code'] == d.SIXP_CMD_RELOCATE:
            self._receive_relocate_request(packet)
        else:
            # not implemented or not supported
            # ignore this request
            pass

    # ======================= private ==========================================

    def _reset_cell_counters(self):
        self.num_cells_passed = 0
        self.num_cells_used   = 0

    def _adapt_to_traffic(self, neighbor_id):
        """
        Check the cells counters and trigger 6P commands if cells need to be
        added or removed.

        :param int neighbor_id:
        :return:
        """
        
        cell_utilization = self.num_cells_used / float(self.num_cells_passed)
        if cell_utilization != self.cell_utilization:
            self.log(
                SimEngine.SimLog.LOG_MSF_CELL_UTILIZATION,
                {
                    '_mote_id'    : self.mote.id,
                    'neighbor_id' : neighbor_id,
                    'value'       : '{0}% -> {1}%'.format(
                        int(self.cell_utilization * 100),
                        int(cell_utilization * 100)
                    )
                }
            )
            self.cell_utilization = cell_utilization
        

        




        


        if d.MSF_LIM_NUMCELLSUSED_HIGH < cell_utilization:
            # add one TX cell
           
            # added Fadoua
            parent = self.engine.motes[neighbor_id] 
            list_cell_to_pref_parent = self.mote.tsch.getDedicatedCells(neighbor_id)
            list_cell_from_pref_parent = parent.tsch.getDedicatedCells(self.mote.id)


            # added Fadoua: This part is added to adapt to traffic as part of the excessive add cells requests
            # if the count_adding_request to a neighbor exceeds a threshold knowing that we already allocated 
            # the maximum number of dedicated cells to that parent, we switch the preferred parent of the mote
            # to see if we can contriute to a better traffic balance within the network

            
            # print('(self.count_adding_requests', self.count_adding_requests)
            # print('(self.self.count_adding_requests[neighbor_id] of neighbor',self.count_adding_requests[neighbor_id], neighbor_id, 'from mote', self.mote.id )
            # print('MSF_MAX_ADD_CELLS_REQUEST_PER_PARENT', d.MSF_MAX_ADD_CELLS_REQUEST_PER_PARENT)



            if (self.count_adding_requests[neighbor_id] >= d.MSF_MAX_ADD_CELLS_REQUEST_PER_PARENT):

                # switch to a new parent
                print('count_adding_requests from mote', self.mote.id, 'to neighbor', neighbor_id)
                print('EXTRA update of pref parent of mote', self.mote.id, 'current pp',  self.mote.rpl.getPreferredParent()
)
                self.mote.rpl.update_preferred_parent('EXTRA')
                # reset the counter
                self.count_adding_requests[neighbor_id] = 0

            else:

                if ((len(list_cell_to_pref_parent) >= d.MSF_MAX_DEDICATED_CELLS_PER_PARENT ) or (len(list_cell_from_pref_parent) >= d.MSF_MAX_DEDICATED_CELLS_PER_PARENT)): # the schedule is full, I reached the max_usable_size of the table
                    # we don't add more cells here
                    self.log(
                        SimEngine.SimLog.LOG_MSF_ERROR_SCHEDULE_FULL,
                        {
                            '_mote_id'    : self.mote.id,
                            'neighbor'    : neighbor_id, 
                            'count_adding_requests' : self.count_adding_requests[neighbor_id]

                        }
                    )

                    self.count_adding_requests[neighbor_id] += 1  # Fadoua: this variable is added to count the adding requests and to switch the pref parent if the count exceeds a threshold 

                    return

                else:

                    self._request_adding_cells(
                        neighbor_id    = neighbor_id,
                        num_tx_cells   = 1,
                        code           =  'ADAPT_TO_TRAFFIC'
                    )
                
                    # added Fadoua
                    self.log(
                            SimEngine.SimLog.LOG_MSF_MORE_DEDICATED_CELL,
                            {
                                '_mote_id'    : self.mote.id,
                                'neighbor_id'    : neighbor_id,
                                'num_tx_cells' : 1, 
                                'reason': 'adapt to traffic',

                            }
                        )

            





        elif cell_utilization < d.MSF_LIM_NUMCELLSUSED_LOW:
            # delete one *TX* cell
            if len(self.mote.tsch.getTxCells(neighbor_id)) > 0:
                
                # print('this delete cell is called inside adapt to traffic function between ', self.mote.id, neighbor_id)
                self._request_deleting_cells(
                    neighbor_id  = neighbor_id,
                    num_cells    = 1,
                    cell_options = self.TX_CELL_OPT
                )

                 # added Fadoua
            self.log(
                    SimEngine.SimLog.LOG_MSF_LESS_DEDICATED_CELL,
                    {
                        '_mote_id'    : self.mote.id,
                        'neighbor_id'    : neighbor_id,
                        'num_tx_cells' : 1, 
                        'reason': 'adapt to traffic'
                    }
                )




    def _housekeeping_collision(self):
        """
        Identify cells where schedule collisions occur.
        draft-chang-6tisch-msf-01:
            The key for detecting a schedule collision is that, if a node has
            several cells to the same preferred parent, all cells should exhibit
            the same PDR.  A cell which exhibits a PDR significantly lower than
            the others indicates than there are collisions on that cell.
        :return:
        """

        # for quick access; get preferred parent
        preferred_parent = self.mote.rpl.getPreferredParent()

        # collect TX cells which has enough numTX
        tx_cell_list = self.mote.tsch.getTxCells(preferred_parent)
        tx_cell_list = {
            slotOffset: cell for slotOffset, cell in tx_cell_list.items() if (
                d.MSF_MIN_NUM_TX < cell['numTx']
            )
        }

        # collect PDRs of the TX cells
        def pdr(cell):
            assert cell['numTx'] > 0
            return cell['numTxAck'] / float(cell['numTx'])
        pdr_list = {
            slotOffset: pdr(cell) for slotOffset, cell in tx_cell_list.items()
        }

        if len(pdr_list) > 0:
            # pick up TX cells whose PDRs are less than the higest PDR by
            # MSF_MIN_NUM_TX
            highest_pdr = max(pdr_list.values())
            relocation_cell_list = [
                {
                    'slotOffset'   : slotOffset,
                    'channelOffset': tx_cell_list[slotOffset]['channelOffset']
                } for slotOffset, pdr in pdr_list.items() if (
                    d.MSF_RELOCATE_PDRTHRES < (highest_pdr - pdr)
                )
            ]
            if len(relocation_cell_list) > 0:
                self._request_relocating_cells(
                    neighbor_id          = preferred_parent,
                    cell_options         = self.TX_CELL_OPT,
                    num_relocating_cells = len(relocation_cell_list),
                    cell_list            = relocation_cell_list
                )
        else:
            # we don't have any TX cell whose PDR is available; do nothing
            pass




        # schedule next housekeeping
        self.engine.scheduleAtAsn(
            asn=self.engine.asn + d.MSF_HOUSEKEEPINGCOLLISION_PERIOD,
            cb=self._housekeeping_collision,
            uniqueTag=('SimEngine', '_housekeeping_collision'),
            intraSlotOrder=d.INTRASLOTORDER_STACKTASKS,
        )


    def _refresh_scheduling_tables(self):   

        # #***********************************************************************************
        # # Fadoua: I needs test again if I have an on-going transmission, leave one guard cell
        # # This is done to clean up the schedule of motes that kept guard cells with their 
        # # previous preferred parents

        list_old_parents= self.mote.rpl.get_list_of_old_parents()
        if all(x is None for x in list_old_parents):
            pass
        else:
            
            list_sans_none = [p for p in list_old_parents if p is not None ]
            for  parent in list_sans_none:
                
                list_cell_to_old_parent = self.mote.tsch.getDedicatedCells(parent)
                
                # I am adding this test to double-check if  delete transaction started at a peer
                # and did not finish atthe other peer for any reason. If it is the case, then I need 
                # postpone the refresh_scheduling_table() 
                # This can be done without risk as the deletion of cells in a refreshing process
                # happens at the same time for both peers
                oldparent = self.engine.motes[parent]
                list_cell_old_parent_to_mote = oldparent.tsch.getDedicatedCells(self.mote.id)

                # if (len(list_cell_to_old_parent) != len(list_cell_old_parent_to_mote)):
                #     #postpone the refreshing process to later
                #     RANDOM_WAIT = random.randint(1, ((d.MSF_REFRESH_SCHEDULING_TABLES_PERIOD/2) - 1))
                #     # schedule next housekeeping
                #     self.engine.scheduleAtAsn(
                #         asn=self.engine.asn +  RANDOM_WAIT,
                #         cb=self._refresh_scheduling_tables,
                #         uniqueTag=('SimEngine', '_refresh_scheduling_tables'),
                #         intraSlotOrder=d.INTRASLOTORDER_STACKTASKS,
                #     )
             
                
                #     # log
                #     self.log(
                #         SimEngine.SimLog.LOG_MSF_REFRESHIN_SCHED_TABLES,
                #         {
                #             '_mote_id':        self.mote.id,
                #             'old_parent' :     parent,
                #             'tab_size_mote_to_parent' : len(list_cell_to_old_parent),
                #             'tab_size_parent_to_mote' : len(list_cell_old_parent_to_mote),
                #             'waiting_time':    RANDOM_WAIT
                #             # 'count': self.settings.count
                #         }
                #     )

                # else:

                if (len(list_cell_to_old_parent)> 0 ):


                    # log
                    self.log(
                        SimEngine.SimLog.LOG_MSF_REFRESHIN_SCHED_TABLES,
                        {
                            '_mote_id':        self.mote.id,
                            'old_parent' :     parent,
                            'tab_size_mote_to_parent' : len(list_cell_to_old_parent),
                            'tab_size_parent_to_mote' : len(list_cell_old_parent_to_mote),
                            # 'waiting_time':    RANDOM_WAIT
                            # 'count': self.settings.count
                        }
                    )


                    i = 0
                    txQueue = self.mote.tsch.getTxQueue()
                    pending_DATA_pkts = False
                    code_test = False

                    while (i<len(txQueue)):

                        if ((txQueue[i]['type'] == 'DATA') and (txQueue[i]['mac']['dstMac'] == parent) and  (txQueue[i]['mac']['retriesLeft'] >= 0)):
                            pending_DATA_pkts = True

                        else:          
                            pending_DATA_pkts = False
                        i += 1
        
                        code_test = pending_DATA_pkts

                    if (code_test == True):
                        pass 
                        # do nothing at this level
                    else:

                        for selected_slotOffset in list_cell_to_old_parent.keys():

                            # I need unlock cells at this level


                            # self._unlock_cells(candidate_cells)
                            # _unlock_slots (self.locked_slots)

                            if selected_slotOffset in self.locked_slots:
                                self.locked_slots.remove(selected_slotOffset)





                            # del self.mote.tsch.schedule[selected_slotOffset]
                            cell= self.mote.tsch.schedule[selected_slotOffset]
                            old_parent_mote= self.engine.motes[parent] # find the correponding mote in the motes table using the mac
                       
                            REASON = 'Refresh schedule delete old cell to old pref parent'
                            # add the test for the cell options, as a cell in the schedules can be shared TX/RX 
                            # or simply a TX at the sending mote and an RX at the receiving mote
                            # we need do this test as before deleting the cell, we double check its cell options   
                            CellOptions=cell['cellOptions']
                            # print('------------mote', self.mote.id, ' CellOptions ==', CellOptions , 'for cell', cell)
                        
                            if (CellOptions == self.TXRX_CELL_OPT): # if a shared cell is in both schedules, 
                            
                                # print('====== delete cell from mote', self.mote.id, 'to neighbor', old_parent_mote.id)
                            
                                self.mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = old_parent_mote.id,
                                    cellOptions   = CellOptions
                                )

                                # print('====== delete cell from mote', old_parent_mote.id, 'to neighbor', self.mote.id)

                                old_parent_mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = self.mote.id,
                                    cellOptions   = CellOptions
                                )
                        
                            elif (CellOptions == self.TX_CELL_OPT):
                                CellOptions_reverse = self.RX_CELL_OPT
                            
                            
                                # print('====== delete cell from mote', self.mote.id, 'to neighbor', old_parent_mote.id)

                                self.mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = old_parent_mote.id,
                                    cellOptions   = CellOptions
                                )

                                # print('====== delete cell from mote', old_parent_mote.id, 'to neighbor', self.mote.id)
                                old_parent_mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = self.mote.id,
                                    cellOptions   = CellOptions_reverse
                                )

                            elif (CellOptions == self.RX_CELL_OPT):
                                CellOptions_reverse = self.TX_CELL_OPT
                            
                                # print('====== delete cell from mote', self.mote.id, 'to neighbor', old_parent_mote.id)

                                self.mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = old_parent_mote.id,
                                    cellOptions   = CellOptions
                                )

                                # print('====== delete cell from mote', old_parent_mote.id, 'to neighbor', self.mote.id)
                                old_parent_mote.tsch.deleteCell(
                                    reason        = REASON,
                                    slotOffset    = selected_slotOffset,
                                    channelOffset = cell['channelOffset'],
                                    neighbor      = self.mote.id,
                                    cellOptions   = CellOptions_reverse
                                )
                                                  
                else:
                    pass

            # else:
            #     pass

            # schedule next housekeeping
        self.engine.scheduleAtAsn(
            asn=self.engine.asn + d.MSF_REFRESH_SCHEDULING_TABLES_PERIOD,
            cb=self._refresh_scheduling_tables,
            uniqueTag=('SimEngine', '_refresh_scheduling_tables'),
            intraSlotOrder=d.INTRASLOTORDER_STACKTASKS,
        )
        # #***********************************************************************************

    # def _refresh_root_scheduling_table(self): 






    #     self.engine.scheduleAtAsn(
    #         asn=self.engine.asn + d.MSF_REFRESH_SCHEDULING_TABLES_PERIOD,
    #         cb=self._refresh_scheduling_tables,
    #         uniqueTag=('SimEngine', '_refresh_scheduling_tables'),
    #         intraSlotOrder=d.INTRASLOTORDER_STACKTASKS,
    #     )



    # cell manipulation helpers
    def _lock_cells(self, cell_list):
        for cell in cell_list:
            self.locked_slots.add(cell['slotOffset'])

    def _unlock_cells(self, cell_list):
        for cell in cell_list:
            self.locked_slots.remove(cell['slotOffset'])

    def _add_cells(self, neighbor_id, cell_list, cell_options):
        try:
            for cell in cell_list:
                self.mote.tsch.addCell(
                    slotOffset         = cell['slotOffset'],
                    channelOffset      = cell['channelOffset'],
                    neighbor           = neighbor_id,
                    cellOptions        = cell_options
                )
        except Exception:
            # We may fail in adding cells since they could be allocated for
            # another peer. We need to have a locking or reservation mechanism
            # to avoid such a situation.
            raise

    
    def _unlock_slots(self, slot): # added Fadoua
       
        self.locked_slots.remove(slot)




    def _delete_cells(self, neighbor_id, cell_list, cell_options):
            
        # print('inside the very function _delete_cells', self.mote.id, neighbor_id, 'at asn:', self.engine.asn)
        # print('cell list of mote', self.mote.id, 'is:', cell_list)

        REASON = 'normal cell delete'
        
        for cell in cell_list:
            self.mote.tsch.deleteCell(
                reason        = REASON, 
                slotOffset    = cell['slotOffset'],
                channelOffset = cell['channelOffset'],
                neighbor      = neighbor_id,
                cellOptions   = cell_options
            )

    #added Fadoua: if we change to a new parent remove cells and leave one cell for future use
    def _clear_cells_old_parent(self, neighbor_id): 
        cells = self.mote.tsch.getDedicatedCells(neighbor_id)
        all_mote_slots   = set(self.mote.tsch.getDedicatedCells(neighbor_id).keys())
        neighbor_mote = self.engine.motes[neighbor_id]

        # print('all cells from', self.mote.id, 'to neighbor', neighbor_id, all_mote_slots)
        # print('_clear_cells_old_parent', self.mote.id, neighbor_id, 'at asn:', self.engine.asn)
        
        #added Fadoua
        Nbr_cells=len(cells)

        # we have only one dedicated cell to the preferred parent; do nothing
        if(Nbr_cells <= 1):
            pass
            # Do nothing: the unique cell is left as a guard cell

        else:
            selected_keys=random.sample(cells, Nbr_cells-1)
            set_cells = {key: cells[key] for key in selected_keys}
      
            
            # print('the set of cells ', set_cells.items())
            for slotOffset, cell in set_cells.items():
                assert neighbor_id == cell['neighbor']
                
                REASON = 'clear cells to old parent and leave one guard'
                # print('=== delete numero', slotOffset)

                # if (cell['cellOptions'] == self.TXRX_CELL_OPT):
                #     celloptions_reverse = self.TXRX_CELL_OPT
                # elif (cell['cellOptions'] == self.TX_CELL_OPT):
                #     celloptions_reverse = self.RX_CELL_OPT
                # elif (cell['cellOptions'] == self.RX_CELL_OPT):
                #     celloptions_reverse = self.TX_CELL_OPT


                self.mote.tsch.deleteCell(
                    reason        = REASON,
                    slotOffset    = slotOffset,
                    channelOffset = cell['channelOffset'],
                    neighbor      = cell['neighbor'],
                    cellOptions   = cell['cellOptions']
                )

                # neighbor_mote.tsch.deleteCell(
                #     reason        = REASON,
                #     slotOffset    = slotOffset,
                #     channelOffset = cell['channelOffset'],
                #     neighbor      = self.mote.id,
                #     cellOptions   = celloptions_reverse
                # )




            # Fadoua: I need lock the remaining cell to be able to delete it later
            # once the transmission of the pending DATA packet to the previous 
            # preferred parent is concluded successfully

                 
            # print('cells to supress', selected_keys)

            lockedSlot = list(all_mote_slots - set(selected_keys))
            # print('lockedSlot ', lockedSlot)

            # print('*** cells ', cells.items() )
            # print('')
            
            lockedCell = [ # Fadoua: create the cell list for that neighbor that are in the schedule
                {
                'slotOffset'   :slot,
                'channelOffset': cell['channelOffset']
                } for slot, cell in cells.items() if (slot in lockedSlot)
            
            ]
            
            # print('mote', self.mote.id, 'locked cell', lockedCell, 'towards neighbor', neighbor_id)
      
            self._lock_cells(lockedCell)


    def _clear_cells(self, neighbor_id):
        cells = self.mote.tsch.getDedicatedCells(neighbor_id)

        # print('_clear_cells', self.mote.id, neighbor_id, 'at asn:', self.engine.asn)

        for slotOffset, cell in cells.items():
            assert neighbor_id == cell['neighbor']
            
            REASON = 'clear cells'
            self.mote.tsch.deleteCell(
                reason        = REASON,
                slotOffset    = slotOffset,
                channelOffset = cell['channelOffset'],
                neighbor      = cell['neighbor'],
                cellOptions   = cell['cellOptions']
            )

    def _relocate_cells(
            self,
            neighbor_id,
            src_cell_list,
            dst_cell_list,
            cell_options
        ):
        assert len(src_cell_list) == len(dst_cell_list)

        # relocation
        # print('--- inside the realocation function before adding and deleting cells !! == at asn:', self.engine.asn, 'mote', self.mote.id, 'neighbor', neighbor_id)
        
        self._add_cells(neighbor_id, dst_cell_list, cell_options)
        self._delete_cells(neighbor_id, src_cell_list, cell_options)

    def _create_available_cell_list(self, cell_list_len):
        slots_in_slotframe    = set(range(0, self.settings.tsch_slotframeLength))
        slots_in_use          = set(self.mote.tsch.getSchedule().keys())
        available_slots       = list(
            slots_in_slotframe - slots_in_use - self.locked_slots
        )

        if len(available_slots) <= cell_list_len:
            # we don't have enough available cells; no cell is selected
            selected_slots = []
        else:
            selected_slots = random.sample(available_slots, cell_list_len)

        cell_list = []
        for slot_offset in selected_slots:
            channel_offset = random.randint(0, self.settings.phy_numChans - 1)
            cell_list.append(
                {
                    'slotOffset'   : slot_offset,
                    'channelOffset': channel_offset
                }
            )
        self._lock_cells(cell_list)
        return cell_list

    def _create_occupied_cell_list(
            self,
            neighbor_id,
            cell_options,
            cell_list_len
        ):
        # Fadoua: at this level retieve all occupied cells to that neighbor that figure in the schedule
        if   cell_options == self.TX_CELL_OPT:
            occupied_cells = self.mote.tsch.getTxCells(neighbor_id)
        elif cell_options == self.RX_CELL_OPT:
            occupied_cells = self.mote.tsch.getRxCells(neighbor_id)
        elif cell_options == self.TXRX_CELL_OPT:
            occupied_cells = self.mote.tsch.getTxRxSharedCells(neighbor_id)

        cell_list = [ # Fadoua: create the cell list for that neighbor that are in the schedule
            {
                'slotOffset'   : slotOffset,
                'channelOffset': cell['channelOffset']
            } for slotOffset, cell in occupied_cells.items()
        ]

        

        # #***********************************************************************************
        # # Fadoua: I needs test again if I have an on-going transmission, leave one guard cell
        # i = 0
        # txQueue = self.mote.tsch.getTxQueue()
        # pending_DATA_pkts = False
        # code_test = False
        # list_old_parents= self.mote.rpl.get_list_of_old_parents()

        # # self.log(
        # #         SimEngine.SimLog.LOG_MSF_CREATE_OCCUPIED_CELL_LIST,
        # #         {
        # #             '_mote_id':        self.mote.id,
        # #             'neighbor_id':     neighbor_id,
        # #             'reason':  '--',
        # #             'occupied_cell_list': cell_list, 
        # #             'list_old_parents' : list_old_parents,
        # #             'txQueue' : txQueue
        # #         }
        # # )
        

        # while (i<len(txQueue) and pending_DATA_pkts == False):
        #     if ((txQueue[i]['type'] == 'DATA') and (txQueue[i]['mac']['dstMac'] in list_old_parents) and  (txQueue[i]['mac']['retriesLeft'] >0)):
        #         pending_DATA_pkts = True
        #     else:
        #         i += 1
        #         pending_DATA_pkts = False
        #     code_test = pending_DATA_pkts
   
        
        # if (code_test == True): # Fadoua: it means that I still have pending transmission that is not yet granted a medium 

        #     if (cell_list_len <= len(occupied_cells)) and (cell_list_len > 1) : # Fadoua: if the node has only one cell to the neighbor do not supress it
        #         cell_list = random.sample(cell_list, cell_list_len-1) # Fadoua: force it to select cellList-1 to garantee that we always have a cell reserved for that neighbor (don't delete them all)
            
        #     # self.log(
        #     #     SimEngine.SimLog.LOG_MSF_CREATE_OCCUPIED_CELL_LIST,
        #     #     {
        #     #         '_mote_id':        self.mote.id,
        #     #         'neighbor_id':     neighbor_id,
        #     #         'reason':  'pending data packet',
        #     #         'occupied_cell_list': cell_list, 
        #     #         'list_old_parents' : list_old_parents,
        #     #         'txQueue' : txQueue
        #     #     }
        #     # )
        

        # else: # this is the default way, where we supress all dedicated cells

        #     if cell_list_len <= len(occupied_cells):# the original code
        #         cell_list = random.sample(cell_list, cell_list_len) # the original code

        #     # self.log(
        #     #     SimEngine.SimLog.LOG_MSF_CREATE_OCCUPIED_CELL_LIST,
        #     #     {
        #     #         '_mote_id':        self.mote.id,
        #     #         'neighbor_id':     neighbor_id,
        #     #         'reason':  'nothing pending -- supress all',
        #     #         'occupied_cell_list': cell_list, 
        #     #         'list_old_parents' : list_old_parents,
        #     #         'txQueue' : txQueue
        #     #     }
        #     # )

        # #***********************************************************************************

        
        if cell_list_len <= len(occupied_cells):# the original code
            cell_list = random.sample(cell_list, cell_list_len) # the original code






        return cell_list

    def _are_cells_allocated(
            self,
            peerMac,
            cell_list,
            cell_options
        ):

        # collect allocated cells
        assert cell_options in [self.TX_CELL_OPT, self.RX_CELL_OPT]
        if   cell_options == self.TX_CELL_OPT:
            allocated_cells = self.mote.tsch.getTxCells(peerMac)
        elif cell_options == self.RX_CELL_OPT:
            allocated_cells = self.mote.tsch.getRxCells(peerMac)

        # test all the cells in the cell list against the allocated cells
        ret_val = True
        for cell in cell_list:
            slotOffset    = cell['slotOffset']
            channelOffset = cell['channelOffset']
            if (
                    (slotOffset not in allocated_cells.keys())
                    or
                    (channelOffset != allocated_cells[slotOffset]['channelOffset'])
                ):
                ret_val = False
                break

        return ret_val

    # ADD command related stuff
    def _request_adding_cells(
            self,
            neighbor_id,
            num_txrx_cells = 0,
            num_tx_cells   = 0,
            num_rx_cells   = 0,
            code           = None #added Fadoua
        ):


        # determine num_cells and cell_options; update num_{txrx,tx,rx}_cells
        if   num_txrx_cells > 0:
            assert num_txrx_cells == 1
            cell_options   = self.TXRX_CELL_OPT
            num_cells      = num_txrx_cells
            num_txrx_cells = 0
        elif num_tx_cells > 0:
            cell_options   = self.TX_CELL_OPT
            if num_tx_cells < self.DEFAULT_CELL_LIST_LEN:
                num_cells    = num_tx_cells
                num_tx_cells = 0
            else:
                num_cells    = self.DEFAULT_CELL_LIST_LEN
                num_tx_cells = num_tx_cells - self.DEFAULT_CELL_LIST_LEN
        elif num_rx_cells > 0:
            cell_options = self.RX_CELL_OPT
            num_cells    = num_rx_cells
            if num_rx_cells < self.DEFAULT_CELL_LIST_LEN:
                num_cells    = num_rx_cells
                num_rx_cells = 0
            else:
                num_cells    = self.DEFAULT_CELL_LIST_LEN
                num_rx_cells = num_rx_cells - self.DEFAULT_CELL_LIST_LEN
        else:
            # nothing to add
            return

        # prepare cell_list
        cell_list = self._create_available_cell_list(self.DEFAULT_CELL_LIST_LEN)

        if len(cell_list) == 0:
            # we don't have available cells right now
            self.log(
                SimEngine.SimLog.LOG_MSF_ERROR_SCHEDULE_FULL,
                {
                    '_mote_id'    : self.mote.id,
                    'neighbor'    : neighbor_id,
                    'count_adding_requests' : self.count_adding_requests[neighbor_id]

                }
            )
            # print('cannot add more cells between mote', self.mote.id, 'and neighbor', neighbor_id)
            return
        # added Fadoua: this part is to see if the function of adding dedicated cells between mote and its preferredParent is executing fine
        else:
            # printout the cell list proposed here
            self.log(
                SimEngine.SimLog.LOG_MSF_CELL_LIST_PROP,
                {
                    '_mote_id'    : self.mote.id,
                    'parent_id': neighbor_id,
                    'cellList': cell_list,
                    'code'    : code
                }
            )



        # prepare _callback which is passed to SixP.send_request()
        callback = self._create_add_request_callback(
            neighbor_id,
            num_cells,
            cell_options,
            cell_list,
            num_txrx_cells,
            num_tx_cells,
            num_rx_cells
        )

        # send a request
        self.mote.sixp.send_request(
            code     = 'request to add cells',
            dstMac      = neighbor_id,
            command     = d.SIXP_CMD_ADD,
            cellOptions = cell_options,
            numCells    = num_cells,
            cellList    = cell_list,
            callback    = callback
        )

    def _receive_add_request(self, request):

        # for quick access
        proposed_cells = request['app']['cellList']
        peerMac         = request['mac']['srcMac']

        # find available cells in the received CellList
        slots_in_slotframe = set(range(0, self.settings.tsch_slotframeLength))
        slots_in_use       = set(self.mote.tsch.getSchedule().keys())
        slots_in_cell_list = set(
            map(lambda c: c['slotOffset'], proposed_cells)
        )
        available_slots    = list(
            slots_in_cell_list.intersection(
                slots_in_slotframe - slots_in_use - self.locked_slots)
        )

        # prepare cell_list
        candidate_cells = [
            c for c in proposed_cells if c['slotOffset'] in available_slots
        ]
        if len(candidate_cells) < request['app']['numCells']:
            cell_list = candidate_cells
        else:
            cell_list = random.sample(
                candidate_cells,
                request['app']['numCells']
            )

        # prepare callback
        if len(available_slots) > 0:
            code = d.SIXP_RC_SUCCESS

            self._lock_cells(candidate_cells)
            def callback(event, packet):
                if event == d.SIXP_CALLBACK_EVENT_MAC_ACK_RECEPTION:
                    # prepare cell options for this responder
                    if   request['app']['cellOptions'] == self.TXRX_CELL_OPT:
                        cell_options = self.TXRX_CELL_OPT
                    elif request['app']['cellOptions'] == self.TX_CELL_OPT:
                        # invert direction
                        cell_options = self.RX_CELL_OPT
                    elif request['app']['cellOptions'] == self.RX_CELL_OPT:
                        # invert direction
                        cell_options = self.TX_CELL_OPT
                    else:
                        # Unsupported cell options for MSF
                        raise Exception()

                    self._add_cells(
                        neighbor_id  = peerMac,
                        cell_list    = cell_list,
                        cell_options = cell_options
                )
                
                self._unlock_cells(candidate_cells)
        else:
            code      = d.SIXP_RC_ERR
            cell_list = None
            callback  = None

        # send a response
        self.mote.sixp.send_response(
            dstMac      = peerMac,
            return_code = code,
            cellList    = cell_list,
            callback    = callback
        )

    def _create_add_request_callback(
            self,
            neighbor_id,
            num_cells,
            cell_options,
            cell_list,
            num_txrx_cells,
            num_tx_cells,
            num_rx_cells
        ):
        def callback(event, packet):
            if event == d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION:
                assert packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE
                if packet['app']['code'] == d.SIXP_RC_SUCCESS:
                    # add cells on success of the transaction
                    self._add_cells(
                        neighbor_id  = neighbor_id,
                        cell_list    = packet['app']['cellList'],
                        cell_options = cell_options
                    )

                    # The received CellList could be smaller than the requested
                    # NumCells; adjust num_{txrx,tx,rx}_cells
                    _num_txrx_cells = num_txrx_cells
                    _num_tx_cells   = num_tx_cells
                    _num_rx_cells   = num_rx_cells
                    remaining_cells = num_cells - len(packet['app']['cellList'])
                    if remaining_cells > 0:
                        if   cell_options == self.TXRX_CELL_OPT:
                            # One (TX=1,RX=1,SHARED=1) cell is requested;
                            # RC_SUCCESS shouldn't be returned with an empty cell
                            # list
                            raise Exception()
                        elif cell_options == self.TX_CELL_OPT:
                            _num_tx_cells -= remaining_cells
                        elif cell_options == self.RX_CELL_OPT:
                            _num_rx_cells -= remaining_cells
                        else:
                            # never comes here
                            raise Exception()

                    code= 'SIXP_CALLBACK_EVENT_PACKET_RECEPTION' # added Fadoua
                    # start another transaction
                    self._request_adding_cells(
                        neighbor_id    = neighbor_id,
                        num_txrx_cells = _num_txrx_cells,
                        num_tx_cells   = _num_tx_cells,
                        num_rx_cells   = _num_rx_cells, 
                        code           = code  
                    )
                else:
                    # TODO: request doesn't succeed; how should we do?
                    pass
            elif event == d.SIXP_CALLBACK_EVENT_TIMEOUT:
                # If this transaction is for the very first cell allocation to
                # the preferred parent, let's retry it. Otherwise, let
                # adaptation to traffic trigger another transaction if
                # necessary.
                code= 'SIXP_CALLBACK_EVENT_TIMEOUT' # added Fadoua
                if cell_options == self.TXRX_CELL_OPT:
                    self._request_adding_cells(
                        neighbor_id    = neighbor_id,
                        num_txrx_cells = 1, 
                        code           = code
                    )
                else:
                    # do nothing as mentioned above
                    pass
            else:
                # ignore other events
                pass

            # unlock the slots used in this transaction
            self._unlock_cells(cell_list)

        return callback

    # DELETE command related stuff
    def _request_deleting_cells(
            self,
            neighbor_id,
            num_cells,
            cell_options
        ):

        # prepare cell_list to send
        cell_list = self._create_occupied_cell_list( # Fadoua: basically here we return list-1 element to leave guard cell
            neighbor_id   = neighbor_id,
            cell_options  = cell_options,
            cell_list_len = self.DEFAULT_CELL_LIST_LEN
        )
        assert len(cell_list) > 0

        # prepare callback
        # print('calleddddddd from _request_deleting_cells !!!!!! aaaaaaaaaaaaaaaaaaaa')
        callback = self._create_delete_request_callback(
            neighbor_id,
            cell_options
        )

        # send a DELETE request
        self.mote.sixp.send_request(
            code     = 'crequest to delete cells',
            dstMac      = neighbor_id,
            command     = d.SIXP_CMD_DELETE,
            cellOptions = cell_options,
            numCells    = num_cells,   # Fadoua this is the original code
            #numCells    = 1,
            cellList    = cell_list,
            callback    = callback
        )

    def _receive_delete_request(self, request):
        # for quick access
        num_cells           = request['app']['numCells']
        cell_options        = request['app']['cellOptions']
        candidate_cell_list = request['app']['cellList']
        peerMac             = request['mac']['srcMac']

        # confirm all the cells in the cell list are allocated for the peer
        # with the specified cell options
        #
        # invert the direction in cell_options
        assert cell_options in [self.TX_CELL_OPT, self.RX_CELL_OPT]
        if   cell_options == self.TX_CELL_OPT:
            our_cell_options = self.RX_CELL_OPT
        elif cell_options == self.RX_CELL_OPT:
            our_cell_options   = self.TX_CELL_OPT

        if (
                (
                    self._are_cells_allocated(
                        peerMac      = peerMac,
                        cell_list    = candidate_cell_list,
                        cell_options = our_cell_options
                    ) is True
                )
                and
                (num_cells <= len(candidate_cell_list))
            ):
            code = d.SIXP_RC_SUCCESS
            cell_list = random.sample(candidate_cell_list, num_cells)

            def callback(event, packet):
                if event == d.SIXP_CALLBACK_EVENT_MAC_ACK_RECEPTION:

                    # print('inside callback of _receive_delete_request from ', self.mote.id, 'vers le neighbor', peerMac)
                    
                    self._delete_cells(
                        neighbor_id  = peerMac,
                        cell_list    = cell_list,
                        cell_options = our_cell_options
                )
        else:
            code      = d.SIXP_RC_ERR
            cell_list = None
            callback  = None

        # send the response
        self.mote.sixp.send_response(
            dstMac      = peerMac,
            return_code = code,
            cellList    = cell_list,
            callback    = callback
        )

    def _create_delete_request_callback(
            self,
            neighbor_id,
            cell_options
        ):
        def callback(event, packet):
            if (
                    (event == d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION)
                    and
                    (packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE)
                ):
                if packet['app']['code'] == d.SIXP_RC_SUCCESS:
                    
                    # print('inside callback of _create_delete_request_callback a partir de', self.mote.id, 'vers le neoud', neighbor_id)
                    
                    self._delete_cells(
                        neighbor_id  = neighbor_id,
                        cell_list    = packet['app']['cellList'],
                        cell_options = cell_options
                    )
                else:
                    # TODO: request doesn't succeed; how should we do?
                    pass
            elif event == d.SIXP_CALLBACK_EVENT_TIMEOUT:
                # TODO: request doesn't succeed; how should we do?
                pass
            else:
                # ignore other events
                pass

        return callback

    # RELOCATE command related stuff
    def _request_relocating_cells(
            self,
            neighbor_id,
            cell_options,
            num_relocating_cells,
            cell_list
        ):

        # determine num_cells and relocation_cell_list;
        # update num_relocating_cells and cell_list
        if self.DEFAULT_CELL_LIST_LEN < num_relocating_cells:
            num_cells             = self.DEFAULT_CELL_LIST_LEN
            relocation_cell_list  = cell_list[:self.DEFAULT_CELL_LIST_LEN]
            num_relocating_cells -= self.DEFAULT_CELL_LIST_LEN
            cell_list             = cell_list[self.DEFAULT_CELL_LIST_LEN:]
        else:
            num_cells             = num_relocating_cells
            relocation_cell_list  = cell_list
            num_relocating_cells  = 0
            cell_list             = []

        # prepare candidate_cell_list
        candidate_cell_list = self._create_available_cell_list(
            self.DEFAULT_CELL_LIST_LEN
        )

        if len(candidate_cell_list) == 0:
            # no available cell to move the cells to
            self.log(
                SimEngine.SimLog.LOG_MSF_ERROR_SCHEDULE_FULL,
                {
                    '_mote_id'    : self.mote.id, 
                    'neighbor'   : neighbor_id,
                    'count_adding_requests' : self.count_adding_requests[neighbor_id]
                }
            )
            return

        # prepare callback
        def callback(event, packet):
            if event == d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION:
                assert packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE
                if packet['app']['code'] == d.SIXP_RC_SUCCESS:
                    # perform relocations
                    num_relocations = len(packet['app']['cellList'])
                    self._relocate_cells(
                        neighbor_id   = neighbor_id,
                        src_cell_list = relocation_cell_list[:num_cells],
                        dst_cell_list = packet['app']['cellList'],
                        cell_options  = cell_options
                    )

                    # adjust num_relocating_cells and cell_list
                    _num_relocating_cells = (
                        num_relocating_cells + num_cells - num_relocations
                    )
                    _cell_list = (
                        cell_list + relocation_cell_list[num_relocations:]
                    )

                    # start another transaction
                    self._request_relocating_cells(
                        neighbor_id          = neighbor_id,
                        cell_options         = cell_options,
                        num_relocating_cells = _num_relocating_cells,
                        cell_list            = _cell_list
                    )
            # unlock the slots used in this transaction
            self._unlock_cells(candidate_cell_list)

        # send a request
        self.mote.sixp.send_request(
            code     = 'request to relocate cells',
            dstMac             = neighbor_id,
            command            = d.SIXP_CMD_RELOCATE,
            cellOptions        = cell_options,
            numCells           = num_cells,
            relocationCellList = relocation_cell_list,
            candidateCellList  = candidate_cell_list,
            callback           = callback
        )

    def _receive_relocate_request(self, request):
        # for quick access
        num_cells        = request['app']['numCells']
        cell_options     = request['app']['cellOptions']
        relocating_cells = request['app']['relocationCellList']
        candidate_cells  = request['app']['candidateCellList']
        peerMac          = request['mac']['srcMac']

        # confirm all the cells in the cell list are allocated for the peer
        # with the specified cell options
        #
        # invert the direction in cell_options
        assert cell_options in [self.TX_CELL_OPT, self.RX_CELL_OPT]
        if   cell_options == self.TX_CELL_OPT:
            our_cell_options = self.RX_CELL_OPT
        elif cell_options == self.RX_CELL_OPT:
            our_cell_options   = self.TX_CELL_OPT

        if (
                (
                    self._are_cells_allocated(
                        peerMac      = peerMac,
                        cell_list    = relocating_cells,
                        cell_options = our_cell_options
                    ) is True
                )
                and
                (num_cells <= len(candidate_cells))
            ):
            # find available cells in the received candidate cell list
            slots_in_slotframe = set(range(0, self.settings.tsch_slotframeLength))
            slots_in_use       = set(self.mote.tsch.getSchedule().keys())
            candidate_slots    = set(
                map(lambda c: c['slotOffset'], candidate_cells)
            )
            available_slots    = list(
                candidate_slots.intersection(slots_in_slotframe - slots_in_use)
            )

            # FIXME: handle the case when available_slots is empty

            # prepare response
            code           = d.SIXP_RC_SUCCESS
            cell_list      = []
            selected_slots = random.sample(available_slots, num_cells)
            for cell in candidate_cells:
                if cell['slotOffset'] in selected_slots:
                    cell_list.append(cell)

            self._lock_cells(cell_list)
            # prepare callback
            def callback(event, packet):
                if event == d.SIXP_CALLBACK_EVENT_MAC_ACK_RECEPTION:
                    num_relocations = len(cell_list)
                    self._relocate_cells(
                        neighbor_id   = peerMac,
                        src_cell_list = relocating_cells[:num_relocations],
                        dst_cell_list = cell_list,
                        cell_options  = our_cell_options
                    )
                self._unlock_cells(cell_list)

        else:
            code      = d.SIXP_RC_ERR
            cell_list = None
            callback  = None

        # send a response
        self.mote.sixp.send_response(
            dstMac      = peerMac,
            return_code = code,
            cellList    = cell_list,
            callback    = callback
        )


    # CLEAR command related stuff
    def _receive_clear_request(self, request):

        peerMac = request['mac']['srcMac']

        def callback(event, packet):
            # remove all the cells no matter what happens
            self._clear_cells(peerMac)

        # create CLEAR response
        self.mote.sixp.send_response(
            dstMac      = peerMac,
            return_code = d.SIXP_RC_SUCCESS,
            callback    = callback
        )
