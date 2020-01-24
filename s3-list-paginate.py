import array
import boto3
import os
import datetime
from datetime import timedelta
import re

arr = ['bos-dbclus5']
s3col = boto3.resource('s3')

def my_function():
    last_week = ((datetime.datetime.now() - datetime.timedelta(days=7)).date()).strftime('%Y-%m-%d')
    date_obj = datetime.datetime.strptime(last_week, '%Y-%m-%d')
    #print(date_obj)
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
        newfolders= []
        for f in forms:
            #find the files in the Active folder
            key_path_source = f + 'Active' + '/'
            data.append(key_path_source)
            #sub_folder = 'week-of'+last_week
            #key = f + sub_folder + '/'
            #newfolders.append(key)
            # for x in newfolders:
            #     resp = s3.put_object(Bucket=i, Key=x)
                
            
        #print(data)
        for  d in data:
            paginator = s3.get_paginator('list_objects')
            operation_parameters = {'Bucket': i,
                        'Prefix': d,
                        'Delimiter':'/'
            }
            page_iterator = paginator.paginate(**operation_parameters)
            #response1 = s3.list_objects(Bucket=i,Prefix=d, Delimiter='/')
            data1 = []
            #glacier = [] #a list to have the files in glacier
            #for resp in response1['Contents']:
            for page in page_iterator:
                for p in page['Contents']:
                    if p['StorageClass'] == 'STANDARD':
                        obj_to_analyze= p['Key']
                        data1.append(obj_to_analyze)
                #else:
                    #glacier.append(resp['Key'])
            #break the file names to find the dates they were created on
            for ota in data1:
                m = re.search(r'\d[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]', ota)
                if m:
                    #print(m)
                    date = datetime.datetime.strptime(m.group(), '%Y%m%d').date()
                    recent_week_start = date_obj - timedelta(days=1)
                    recent_week_end = recent_week_start + timedelta(days=-6)
                    
                    #print(recent_week_start,recent_week_end) 
                    
                    # (start_of_week)
                    #print(end_of_week)
                    #print(date)
                    if date  < (recent_week_end).date():
                        #calculate what week folder it should be in by checking the last week from the date of file
                        while date.weekday() != 6:
                            date += datetime.timedelta(days=1)
                        #start_of_week = date - timedelta(days=6)
                        #end_of_week = start_of_week + timedelta(days=7)
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
                        
                        copy_dest = my_path+'/' +'week-of'+ date.strftime('%Y-%m-%d') +'/'+ my_key
                        #print(copy_dest)
                        s3.copy_object(CopySource=copy_source, Bucket=i, Key=copy_dest)
                        #s3.delete_object(Bucket=i,Key=ota)
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
                        #s3.delete_object(Bucket=i,Key=ota)
                    else:
                        continue
                        
                        
            
                
                
                

                                
                                

                             
                            
                            
                            
                            
                        
                        
                        
                    

                
                
                
            
            
            
        
        
        
            
            