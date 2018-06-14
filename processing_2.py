import io
import json
import pprint
import base64
with open('exec_numMotes_10.dat','r') as fd:
    for line in fd:
        #line = line.encode().decode()
        line = line.strip()
        #s = eval(line)
        #print(s)
        #data =json.dumps(line)
        data=json.loads(line)
        #print(line)
        #print(data['app_pkPeriodVar'])
        
       
        # if ('_asn' in data) and ('interfering_transmissions' in data):
        #     cycle=int(data['_asn']/101) # find the cycle number
        #     collide={}
        
        #     if cycle not in collide:
        #         collide[cycle]=0       
            

            
        #     #raise()
        #     for dt in (data['interfering_transmissions']): #only want "_type": "interfering_transmissions"
                
        #         #print('heloooooooooooooooooooooo')
            
        #         #for dt in (data['interfering_transmissions']):
        #         #if 'type' in dt:
        #         collide[cycle]+=1 
        #     print(cycle,collide)
        #     #print("{},{},{}".format(data['_run_id'],data['_asn'],count)) 

        # if '_asn' in data:    
        #     asn=int(data['_asn'])
        collision=0
        
        if 'interfering_transmissions' in data: #only want "_type": "interfering_transmissions"
                #print(data['_type'])
            asn=int(data['_asn'])/101
            collision+=1


                # for dt in (data['interfering_transmissions']):
                #     cycle=int(data['_asn']/101) # find the cycle number
                #     if 'type' in dt:
            print("{},{}".format(asn,collision))
