import io
import json
import pprint
#import base64
with open('output_80motes.dat','r') as fd:
    for line in fd:
        #line = line.encode().decode()
        line = line.strip()
        #s = eval(line)
        #print(s)
        #data =json.dumps(line)
        data=json.loads(line)
        #print(line)
        #print(data['app_pkPeriodVar'])
        
        

        #--------- for collided packets --------------
        
        if 'interfering_transmissions' in data: #only want "_type": "interfering_transmissions"
            #print(data['_type'])
            for dt in (data['interfering_transmissions']):
                cycle=int(data['_asn']/101) # find the cycle number
                if 'type' in dt:
                    print("{},{},{},{}".format(data['_run_id'],cycle,data['_mote_id'],dt['type']))



        # #--------- for dropped packets --------------
       
        # if 'packet_dropped' in data: #only want "_type": "interfering_transmissions"
        #     #print(data['_type'])
        #     for dt in (data['packet_dropped']):
        #         cycle=int(data['_asn']/101) # find the cycle number
        #         if 'type' in dt:
        #             print("{},{},{},{}".format(data['_run_id'],cycle,data['_mote_id'],dt['type']))
