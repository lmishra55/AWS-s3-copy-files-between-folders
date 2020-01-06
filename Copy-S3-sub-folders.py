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
    #dt_now = datetime.datetime.now().strptime('%Y-%m-%d')
    #print(dt_now)
    #date1 = int(last_week)
    #print(date)
    s3 = boto3.client('s3')
    for i in arr:
        response = s3.list_objects(Bucket=i,Prefix= 'TranLogs/', Delimiter='/')
        forms = [x['Prefix'] for x in response['CommonPrefixes']]
        #print(forms)
        data = []
        for f in forms:
            #find the files in the Active folder
            key_path_source = f + 'Active' + '/'
            data.append(key_path_source)
            sub_folder = 'week-of'+last_week
            key = f + sub_folder + '/'
            resp = s3.put_object(Bucket=i, Key=key)
            continue
        #print(data)
        for  d in data:
            response1 = s3.list_objects(Bucket=i,Prefix=d, Delimiter='/')
            data1 = []
            for resp in response1['Contents']:
                obj_to_analyze= resp['Key']
                data1.append(obj_to_analyze)
            #print(data1)
            
            #break the file names to find the dates they were created on
            for ota in data1:
                m = re.search(r'\d[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]', ota)
                if m:
                    #print(m)
                    date = datetime.datetime.strptime(m.group(), '%Y%m%d%H%M%S').date()
                    recent_week_start = date_obj - timedelta(days=6)
                    recent_week_end = recent_week_start + timedelta(days=7)
                    #print (start_of_week)
                    #print(end_of_week)
                    #print(date)
                    if date  < (recent_week_start).date():
                        #calculate what week folder it should be in by checking the last week from the date of file
                        start_of_week = date - timedelta(days=6)
                        end_of_week = start_of_week + timedelta(days=7)
                        #print(end_of_week)
                        #build the key o search
                        k1,k2,k3,k4 = ota.split('/')
                        my_key = k4
                        my_path = k1+'/'+k2
                        #print(my_path)

                        copy_source = {
                            'Bucket': i,
                            'Key': ota
                             }
                        
                        copy_dest = my_path+'/' +'week-of'+ end_of_week.strftime('%Y-%m-%d') +'/'+ my_key
                        #print(copy_dest)
                        s3.copy_object(CopySource=copy_source, Bucket=i, Key=copy_dest)
                        #s3.delete_object(Bucket=i,Key=ota)
                    elif date >= recent_week_start.date() and date <= recent_week_end.date():
                        #build the key o search
                        k1,k2,k3,k4 = ota.split('/')
                        my_key = k4
                        my_path = k1+'/'+k2
                        #print(my_path)

                        copy_source = {
                            'Bucket': i,
                            'Key': ota
                             }
                        copy_dest = my_path+'/' +'week-of'+ recent_week_end.strftime('%Y-%m-%d') +'/'+ my_key
                        #print(copy_dest)
                        s3.copy_object(CopySource=copy_source, Bucket=i, Key=copy_dest)
                        #s3.delete_object(Bucket=i,Key=ota)
                    else:
                        continue
                        
                        

                                
                                

                             
                            
                            
                            
                            
                        
                        
                        
                    

                
                
                
            
            
            
        
        
        
            
            