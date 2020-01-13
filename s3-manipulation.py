import array
import boto3
import os
import datetime
from datetime import timedelta
import re

arr = ['myapp-test1','myapp-test2']
s3col = boto3.resource('s3')

def lambda_handler(event, context):
    last_week = ((datetime.datetime.now() - datetime.timedelta(days=7)).date()).strftime('%Y-%m-%d')
    date_obj = datetime.datetime.strptime(last_week, '%Y-%m-%d')
    s3 = boto3.client('s3')
    for i in arr:
        response = s3.list_objects(Bucket=i,Prefix= 'TranLogs/', Delimiter='/')
        forms = [x['Prefix'] for x in response['CommonPrefixes']]
        data = []
        for f in forms:
            #find the files in the Active folder
            key_path_source = f + 'Active' + '/'
            data.append(key_path_source)
            sub_folder = 'week-of'+last_week
            key = f + sub_folder + '/'
            # for x in newfolders:
            #     resp = s3.put_object(Bucket=i, Key=x)
                
            
        
        for  d in data:
            response1 = s3.list_objects(Bucket=i,Prefix=d, Delimiter='/')
            data1 = []
            for resp in response1['Contents']:
                obj_to_analyze= resp['Key']
                data1.append(obj_to_analyze)
            
            
            #break the file names to find the dates they were created on
            for ota in data1:
                m = re.search(r'\d[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]', ota)
                if m:
                    #print(m)
                    date = datetime.datetime.strptime(m.group(), '%Y%m%d%H%M%S').date()
                    recent_week_start = date_obj - timedelta(days=1)
                    recent_week_end = recent_week_start + timedelta(days=-6)
 
                    if date  < (recent_week_end).date():
                        #calculate what week folder it should be in by canculating the next sunday from the date of the file
                        while date.weekday() != 6:
                            date += datetime.timedelta(days=1)
   
                        #build the key o search
                        k1,k2,k3,k4 = ota.split('/')
                        my_key = k4
                        my_path = k1+'/'+k2
                        #print(my_path)

                        copy_source = {
                            'Bucket': i,
                            'Key': ota
                             }
                        
                        copy_dest = my_path+'/' +'week-of'+ date.strftime('%Y-%m-%d') +'/'+ my_key
                        #print(copy_dest)
                        s3.copy_object(CopySource=copy_source, Bucket=i, Key=copy_dest)
                        s3.delete_object(Bucket=i,Key=ota)
                    elif date >= recent_week_end.date() and date <= recent_week_start.date():
                        while date.weekday() != 6:
                            date += datetime.timedelta(days=1)
                        #build the key o search
                        k1,k2,k3,k4 = ota.split('/')
                        my_key = k4
                        my_path = k1+'/'+k2
                        #print(my_path)

                        copy_source = {
                            'Bucket': i,
                            'Key': ota
                             }
                        copy_dest = my_path+'/' +'week-of'+ date.strftime('%Y-%m-%d') +'/'+ my_key
                        #print(copy_dest)
                        s3.copy_object(CopySource=copy_source, Bucket=i, Key=copy_dest)
                        s3.delete_object(Bucket=i,Key=ota)
                    else:
                        continue
                        
                        

                                
                                

                             
                            
                            
                            
                            
                        
                        
                        
                    

                
                
                
            
            
            
        
        
        
            
            