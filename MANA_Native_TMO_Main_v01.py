#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import os, fnmatch
import datetime
from datetime import datetime, timedelta
import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from threading import Thread
import threading
import json
import shutil
from datetime import datetime, timedelta
import spUploadDownloadAPI
import numpy as np
import sys
import json
import requests
from zipfile import ZipFile
import re
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import gc
import pyodbc
from handleISFactions import createInstantWO, isfActionStaus, updateDashboard, closeAndUploadDashboard,updateOutputURL
from ApiCall import new_api


# In[2]:


pd.options.mode.chained_assignment = None  # default='warn'

home_folder = '/home/isfuser1/autoBots/auto_TMO_Native_Deviated_KPIs'
os.chdir(home_folder)


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


# In[4]:


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


# In[5]:


def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


# In[6]:


def getEPPreport_forTemplates(EPPNodeList, StartDate, EndDate, granularity, tech, directory, EPPUserEmail,rptname,customer,level):
    try:
        api_key = 'tamEhFAJhwj0ng5TM1Whzg'
        EPPurl1 = "https://manadopapi.naops.exu.ericsson.se/EPPMiddleware/api/GRGEmailReport/{}/ericsson/{}?apikey={}".format(customer,tech,api_key)
     
        queryStr = {
            "customer": customer,
            "Vendor": "Ericsson",
            "tech": tech,
            "apikey": api_key,
            "UserId": EPPUserEmail,
            "EmailAddress": EPPUserEmail,
            "AppId": "External_User",
            "StartDate": StartDate,
            "EndDate": EndDate,
            "Timeaggr": granularity,
            "ReportName": rptname,
            "NEs": EPPNodeList,
            "Level": level,
            "Mode": "mw",
            "Format": "CSV",
            "ReportInterval": "Download"
            
        }
     
        headers = {'Content-Type': 'application/json'}
        # Check for SQL ID 3 times
        count = 1
        SQLId = ""
        while True:
            if count > 3:
                print("3 attempts failed to get sql id from epp for cluster {} template {}".format(directory, rptname))
                break
            try:
                response1 = requests.post(EPPurl1, headers=headers, verify=False, data=json.dumps(queryStr,default=myconverter))
                print(response1.text)
                if (response1.status_code == 200):
                    SQLId = response1.text.split(" ")[6]
                    print("Report Generation Initiated. SQL ID: " + str(SQLId))
                    break
                else:
                    print(directory, ": Retry: TaskID Retrieve Failed Attempt No -", count)
                    count += 1
                    continue
            except Exception as e:
                print(directory, ": Retry: Exception while fetching the token", e)
                print(datetime.now(), ": Attempt No -", count)
                count += 1
                continue


        if SQLId != "":
            print_count = 0
            waitTime = 0
            downLoadRetryCount = 0
            while (waitTime < 90 * 60):
                try:
                    EPPurl2 = "https://manadopapi.naops.exu.ericsson.se/EPPMiddleware/api/ScheduledReportStatusRetrieve/{}/ericsson/{}?apikey={}".format(customer,tech,api_key)
                        
                    queryStr ={
                                "apikey":api_key,
                                "SQLId":SQLId,
                                "UserId":EPPUserEmail,
                                "Customer":customer,
                                "Vendor":"Ericsson",
                                "Tech":tech
                                }
             
                    Download_response = requests.post(EPPurl2, headers=headers, verify=False, data=json.dumps(queryStr,default=myconverter), timeout=600)
                    #print(Download_response.status_code)
                    #print(Download_response.text)
             
                    if "Completed" in Download_response.text:
                        flag = "Completed"
                        print(tech+" Report generation completed for "+directory)
                        EPPurl2 = "https://manadopapi.naops.exu.ericsson.se/EPPMiddleware/api/ScheduledReportStatusRetrieve/{}/Ericsson/{}?apikey={}".format(customer,tech,api_key)
                        queryStr = {
                            "apikey":api_key,
                            "SQLId":SQLId,
                            "UserId":EPPUserEmail,
                            "Customer":customer,
                            "Vendor":"Ericsson",
                            "Tech":tech,
                            "IsDownloadReport":"true"
                        }
                       
                        Download_response = requests.post(EPPurl2, headers=headers, verify=False, data=json.dumps(queryStr,default=myconverter))
                        
                        with open(os.path.join(directory, f"{rptname}_{granularity}.zip"), 'wb') as file:
                                    
                                    file.write(Download_response.content)
                                    FullFileName = f"{rptname}_{granularity}.zip"
                                    file.close()
                                    print(f'{FullFileName} downloaded in folder -{directory}')
                                    return
                    else:
                        if print_count == 0:
                            print("Report ID - " + SQLId + " in progress for cluster - "+directory +" (Tech = "+tech.upper()+")")
                        elif print_count == 2:
                            print_count = -1
                        print_count += 1
                        waitTime = waitTime + 60
                        time.sleep(60)
                        continue
                except Exception as e:
                    print(directory, ": Retry: Exception while fetching the data from EPP: ", rptname, e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    waitTime = waitTime + 30
                    time.sleep(30)
                    if downLoadRetryCount > 2:
                        print(directory, ": Unable to download the report even after 3 attempts")
                        return
                    else:
                        downLoadRetryCount += 1
                        print(directory, ": Download failed and retrying for download", "Attempt No: ",
                                downLoadRetryCount)
                        continue

            if (waitTime >= 90 * 60):
                print(directory, ": Report not received from EPP within 90 Mins")
                return
                
        else:
            print(directory, ": EPP is down")
            return
            #continue
            
    except Exception as e:
        print(directory, ": Exception while fetching the token from EPP: ", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    return


# In[7]:


def getEPPreport_forKPIs(EPPNodeList, kpiList, start_date, end_date, cluster_name, tech, eppCustomerName):
    #reportDate = (datetime.now() - timedelta(1)).strftime('%Y/%m/%d')
    #print(datetime.now(), ":", clustername, ": Processing:", "4G", " : ", "pmCounters", " : ", reportDate)
######### LTE Report Fetch ###########
    try:
        # Read node/site list from 5G_Sitelist
        #for i in range(0,len(payld_list)):
        #SITELIST = EPPNodeList
        #KPI = kpiList
        StartDate = start_date
        EndDate = end_date
        dateRange = "{}-{}".format(StartDate, EndDate)
        #eppCustomerName = statusOfRequest["EppCustName"]
        eppApiKey = 'tamEhFAJhwj0ng5TM1Whzg'
        symbol = ("/")
        #path = statusOfRequest["HomeDir"]
        cluster_name = cluster_name + "_" + tech
        reportName = cluster_name + '_cell_level' + symbol + "pmCounterReport.csv"
        print("Fetching the report:{}".format(reportName))
        api_getData = "https://manadopapi.naops.exu.ericsson.se/EPPMiddleware/api/CMPMFMAsync/{}/ericsson/{}?apikey={}".format(eppCustomerName, tech, eppApiKey)
        api_fetchData = "https://manadopapi.naops.exu.ericsson.se/EPPMiddleware/api/CMPMFMAsyncResults/{}/ericsson/{}?apikey={}".format(eppCustomerName, tech, eppApiKey)
        reqPayload = {
            "Customer": eppCustomerName,
            "Vendor": "ericsson",
            "Tech": tech,
            "TemporalResolution": "Hourly",
            "SpatialResolution": "cell",
            "ObjectidType": "cell",
            "Objectids": EPPNodeList,
            "Kpis": kpiList,
            "Daterange": dateRange,
            "TimeZoneFilter": "local",
            "TimeZoneOutput": "local",
            "MaxNumberOfRows": "2000000",
            "AppId": "RunApi2",
            "IgnoreMissingCounters": "False",
            "ConvertDistFrom1to0Based": "True"
            #"ConvertDistFrom1to0Based": "False"
        }
        reqHeader = {'Content-Type': 'application/json'}

        reqToken = ""
        #global semaObj
        cnt = 1
        while True:
            if cnt > 3:
                print(cluster_name, ": Unable to fetch the data after 3 attempts for ", "pmCounters", StartDate)
                break
            try:
                #semaObj.acquire()
                response = requests.request("POST", api_getData, json=reqPayload, headers=reqHeader, timeout=600, verify=False)
                time.sleep(1)
                print(cluster_name, ": pmCounters", StartDate, response.text)
                #semaObj.release()
                if (response.status_code == 200):
                    reqToken = response.json()
                    break
                else:
                    print(cluster_name, ": Retry: TaskID Retrieve Failed Attempt No -", cnt)
                    cnt += 1
                    continue
            except Exception as e:
                print(cluster_name, ": Retry: Exception while fetching the token", e)
                print(datetime.now(), ": Attempt No -", cnt)
                cnt += 1
                continue

        # Wait till the processing is complete
        getDataPayload = {
            "key": reqToken
        }
        if reqToken != "":
            print_count = 0
            waitTime = 0
            downLoadRetryCount = 0
            while (waitTime < 90 * 60):  # Wait for 90 Mins
                try:
                    response = requests.request("POST", api_fetchData, json=getDataPayload, headers=reqHeader, timeout=3600, verify=False)
                    if ("in progress" in response.text):
                        if print_count == 0:
                            print(cluster_name,": pmCounters", response.text)
                        elif print_count == 2:
                            print_count = -1
                        print_count += 1    
                        waitTime = waitTime + 60
                        time.sleep(60)
                        continue
                    elif ("An error has occurred" in response.text):
                        print(cluster_name, ": Error while processing the request, retrying...")
                        
                    else:
                        respContent = response.json()
                        dataHeaders = []
                        for dd in respContent.pop(0):
                            dataHeaders.append(dd) # Added
                        df = pd.DataFrame(respContent, columns=dataHeaders)
                        print(datetime.now(), ": Writing to ", reportName)
                        df.to_csv(reportName, index=False)
                        return
                        
                        

                except Exception as e:
                    print(cluster_name, ": Retry: Exception while fetching the data from EPP: ", reportName, e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    waitTime = waitTime + 60
                    time.sleep(60)
                    if downLoadRetryCount > 2:
                        print(cluster_name, ": Unable to download the report even after 3 attempts")
                        return
                        
                    else:
                        downLoadRetryCount += 1
                        print(cluster_name, ": Download failed and retrying for download", "Attempt No: ",
                                downLoadRetryCount)
                        continue

            if (waitTime >= 90 * 60):
                print(cluster_name, ": Report not received from EPP within 90 Mins")
                return
            
        else:
            print(cluster_name, ": EPP is down")
            return
            #continue

    except Exception as e:
        print(cluster_name, ": Exception while fetching the token from EPP: ", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    return


# In[8]:


def getDeviantKPIs(folderName, filePath, customer, email_list, cluster, level, launch_date, band, df_cluster):# Native api call for cluster level
    try:
        url = 'https://native.ran.rc.us.am.ericsson.se/api/clusterKPIUpload'
    
        api_key = 'lHxRq4hYYySovXs-wJy3jA'
        fileName = filePath.split("/")[-1]
        files=[
          ('file',(fileName,open(filePath,'rb'),'text/csv'))
        ]
        headers = {'x-api-key': api_key}
        payload = {'cu': customer.lower(),
                   'email_list': email_list,
                   'cluster_name': cluster,
                   'file_type': level,
                   'launch_date': launch_date}

        transaction_id = ""
        count = 1
        while True:
            if count > 3:
                print(cluster, ": Unable to fetch the data after 3 attempts from Native")
                break
            try:
                response = requests.request("POST", url, headers=headers, data=payload, files=files, verify = False)
                
                transaction_response = json.loads(response.content)
                if response.status_code == 200:
                    transaction_id = transaction_response['transaction_id']
                    print("Native request initiated with transacation id {} for cluster {}".format(transaction_id, cluster))
                    break
                else:
                    print(transaction_response['message'])
                    count += 1
                    time.sleep(60)
                    continue

            except Exception as e:
                print(cluster, ": Retry: Exception while fetching the token", e)
                print(datetime.now(), ": Attempt No -", count)
                count += 1
                continue

        if level == 'cluster':
            endTime = 30
        else:
            endTime = 10

        if transaction_id != "":

            print_count = 0
            waitTime = 0
            downLoadRetryCount = 0

            
            params = {'transaction_id':transaction_id}
            headers= {'x-api-key':api_key,'Content-Type':'application/json'}

            while (waitTime < 60 * endTime):  # Wait for 60 Mins
                try:
                    DeviatedKPIs = requests.get('https://native.ran.rc.us.am.ericsson.se/api/getDeviatedKPIs',params=params,verify=False,headers=headers)
                    if DeviatedKPIs.status_code == 200:
                        if level == 'cluster':
                            data = json.loads(DeviatedKPIs.content)
                            df_native_output = pd.json_normalize(data['result']['deviancy_data']).T.reset_index()
                            df_native_output.columns = ['KPI Name','deviancy_data']
                            df_kpi = df_native_output.loc[df_native_output['deviancy_data'].isin(['Slight Degradation found','Degradation found'])]
                            outputfilename = folderName + "/cluster_level_deviated_kpi_list_{}.csv".format(band)
                            df_kpi.to_csv(outputfilename, index = False)
                            
                            print("Deviated KPI list generated for cluster: {}".format(cluster))
                        elif level == 'node':
                            # create df for cell and output
                            df_data_list = []
                            for i in range(0,len(df_cluster)):
                                kpi_name = df_cluster['KPI Name'][i]
                                for node in df_cluster['node_list'][i].split(","):
                                    try:
                                        data = json.loads(DeviatedKPIs.text)['result']['deviancy_data'][kpi_name][node]
                                        df_data = pd.json_normalize(data)
                                        ld = 'Launch_Date: {}'.format(df_cluster['launch_date'][i].strftime(date_format_ref))
                                        df_data.columns = [ld, 'Delta', 'Post', 'Pre']
                                        df_data['KPI_Name'] = kpi_name
                                        df_data['Node'] = node
                                        df_data['Cluster'] = cluster
                                        df_data = df_data[['Cluster','Node','KPI_Name',ld,'Pre','Post','Delta']]
                                        df_data_list.append(df_data)
                                    except:
                                        pass
                                    try:
                                        df_data_all = pd.concat(df_data_list, ignore_index=True)
                                    except:
                                        data = {'Message': ['No Deviated KPIs found in the input data']}
                                        df_data_all = pd.DataFrame(data)
                            outputfilename = folderName + "/cell_level_deviated_kpi_details_{}.csv".format(band)
                            try:
                                df_data_all.to_csv(outputfilename, index = False)
                                print("Cell level report with deviated KPI generated for cluster: {}".format(cluster))
                            except:
                                print("Please check native for cell report fof cluster: {}".format(cluster))
                        return 
                    elif DeviatedKPIs.status_code == 202:
                        if print_count == 0:
                            print ("Data processing for cluster: {}".format(cluster))
                        elif print_count == 2:
                            print_count = -1
                        print_count += 1
                        waitTime = waitTime + 60
                        time.sleep(60)
                        continue
                    else:
                        print(json.loads(response.content)['message'])
                        return

                except Exception as e:
                    print(cluster, ": Retry: Exception while fetching the data from Native: ", level, e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    waitTime = waitTime + 30
                    time.sleep(30)
                    if downLoadRetryCount > 2:
                        print(cluster, ": Unable to download the report even after 3 attempts")
                        return
                    else:
                        downLoadRetryCount += 1
                        print(cluster, ": Download failed and retrying for download", "Attempt No: ",
                                downLoadRetryCount)
                        continue
                        
            if (waitTime >= 60 * endTime):
                print(cluster, ": Report not received from Native within {} Mins".format(endTime))
                return

        else:
            print(cluster, ": Native is down")
            return
            #continue

    
    except Exception as e:
        print(cluster, ": Exception while fetching the token from Native: ", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    return


# In[9]:


def df_details_print(df_job_details):
    d1 = datetime.now().strftime("%d-%m-%H_%M_%p")
    try:
        df_job_details.drop(['file_name','folder_name'],axis = 1, inplace = True)
    except:
        pass
    for i in range (0,len(df_job_details)):
        try:
            filename = "TMO_Job_Details_Cluster_Thread_"+df_job_details.loc[i,'Cluster Name Band']+"_"+d1+".xlsx"
            filename_all_jobs = "All_Job_Initiated_TMO_"+df_job_details.loc[i,'Cluster Name Band']+"_"+d1+".xlsx"
            df_job_details.loc[[i]].to_excel(filename, index=False)
            df_job_details.loc[[i]].to_excel(filename_all_jobs, index=False)
            print('Job details printed for cluster: {}'.format(df_job_details.loc[i,'Cluster Name Band']))
        except:
            filename = "TMO_Job_Details_Cluster_Thread_"+df_job_details.loc[i,'Cluster Name']+"_"+d1+".xlsx"
            filename_all_jobs = "All_Job_Initiated_TMO_"+df_job_details.loc[i,'Cluster Name']+"_"+d1+".xlsx"
            df_job_details.loc[[i]].to_excel(filename, index=False)
            df_job_details.loc[[i]].to_excel(filename_all_jobs, index=False)
            print('Job details printed for cluster: {}'.format(df_job_details.loc[i,'Cluster Name']))
            
    return


# In[10]:


def band_label(label, tech):
    if tech == 'nr':
        if label in ['I','2100']:
            return 'N66'
        elif label in ['K','600']:
            return 'N71'
        elif label in ['J','1900 PCS']:
            return 'N25'
        elif label in ['A','TD 2600+']:
            return 'N41'
        elif label in ['N','39 GHz']:
            return 'mmW_39'
        elif label in ['M','28 GHz']:
            return 'mmW_28'
        else:
            return np.nan
    elif tech == 'lte':
        if label in ['D','700 a']:
            return 'LTE_FDD_700'
        elif label in ['C','850+']:
            return 'LTE_FDD_850'
        elif label in ['L','AWS-1']:
            return 'LTE_FDD_AWS-1'
        elif label in ['F','AWS-3']:
            return 'LTE_FDD_AWS-3'
        elif label in ['E', '600']:
            return 'LTE_FDD_600'
        elif label in ['B', '1900 PCS']:
            return 'LTE_FDD_1900'
        elif label in ['T', 'TD 2500']:
            return 'LTE_TDD_2500'
        else:
            return np.nan
    else:
        return np.nan


def del_folders(folders_to_deleted):
    if len(folders_to_deleted) != 0:
        for folder in folders_to_deleted:
            try:
                print("Deleting Folder:", folder)
                shutil.rmtree(folder)
            except:
                pass
    return


def Native_Data_Process_Flow(df_input):

    folders_to_deleted = []

    df_input_wo_all = df_input.copy(deep = True)
    for i in range(0, len(df_input_wo_all)):
        startWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))

    native_mail_box = 'anurag.dey@ericsson.com'


    date_format_start = '%m/%d/%Y 12:00 AM'
    date_format_end = '%m/%d/%Y 11:59 PM'
    date_format_ref = '%m/%d/%Y'
        
    today = datetime.now()
    
    df_input['file_name'] = np.nan
    df_input['start_date'] = np.nan
    df_input['end_date'] = np.nan
    df_input['EPP_Cluster_Level_StartTime'] = 'Not Completed'
    df_input['EPP_Cluster_Level_EndTime'] = 'Not Completed'
    
    threadList = []
    
    i = 0
    while i<len(df_input):
        # get inputs from the input sheet
        EPPNodeList = df_input['List of Nodes'][i]
        owner = df_input['Owner'][i]
        email = df_input['E-Mail'][i]
        ref_date = df_input['Launch Date'][i]
        start_date = ref_date - timedelta(days = int(df_input['Number of pre days'][i]))
        start_date = start_date.strftime(date_format_start)
        end_date = today.strftime(date_format_end)
        ref_date = ref_date.strftime(date_format_ref)
        tech = df_input['Technology'][i]
        epp_report = df_input['EPP Template Name'][i]
        cluster_name = df_input['Cluster Name'][i]
        customer = df_input['Customer'][i].lower()
        granularity = "Hourly"
        level = "band level"
    
        #Create folder with cluster name and level
        folder_name = cluster_name
        file_name = folder_name + "/{}.zip".format(epp_report + "_" + granularity)
        try:
            shutil.rmtree(folder_name)
        except:
            pass
        os.makedirs(folder_name, exist_ok=True)
        folders_to_deleted.append(folder_name)
        start = time.time()
        start_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))
        df_input.loc[i,'EPP_Cluster_Level_StartTime'] = 'Initiated: {}'.format(start_as_str)
        thread = threading.Thread(target=getEPPreport_forTemplates, args=[EPPNodeList, start_date, end_date, granularity, tech, folder_name,email,epp_report,customer,level])
        thread.daemon = True
        thread.start()
        threadList.append(thread)
        df_input.loc[i,'file_name'] = file_name
        df_input.loc[i,'folder_name'] = folder_name
        df_input.loc[i,'start_date'] = start_date
        df_input.loc[i,'end_date'] = end_date
            
        i+=1
    for each in threadList:
            each.join()
    
    
    for file_name in df_input['file_name'].to_list():
        try:
            folder = "/".join(file_name.split("/")[0:-1])
            with ZipFile(file_name, 'r') as zip_files:
                print('Extracting {}'.format(file_name.split('/')[-1]))
                zip_files.extractall(path = folder)
                print('Done!')
        except:
            pass

    for filename in df_input['file_name'].to_list():
        remove_last_hour(filename)
            
    

    df_input_bandwise_list = []
    for i in range(0,len(df_input)):
        try:
            file_name = find('*.csv', df_input['folder_name'][i])
            df_input.loc[i,'file_name'] = file_name[0].replace("\\","/")
            end = time.time()
            end_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))
            df_input.loc[i, 'EPP_Cluster_Level_EndTime'] = 'Completed: {}'.format(end_as_str)
            df_cluster_kpis_all_bands = pd.read_csv(df_input['file_name'][i])
            #df_cluster_kpis_all_bands ['ObjectId'] = df_cluster_kpis_all_bands ['ObjectId'].apply(band_label)
            df_cluster_kpis_all_bands ['ObjectId'] = df_cluster_kpis_all_bands.apply(lambda x: band_label(x["ObjectId"], df_input['Technology'][i]), axis=1)
            df_cluster_kpis_all_bands = df_cluster_kpis_all_bands.dropna(subset = ['ObjectId'])
            #list_of_bands_in_data = list(df_cluster_kpis_all_bands['ObjectId'].unique())
            #list_of_bands.remove('unknown')
            if df_input.loc[i,'Technology'] == 'lte':
                list_of_bands = ['LTE_FDD_700', 'LTE_FDD_850','LTE_FDD_AWS-1','LTE_FDD_AWS-3']
            elif df_input.loc[i,'Technology'] == 'nr':
                list_of_bands = ['N66','N71','N41','N25','mmW_39','mmW_28']
            
            for band in list_of_bands:
                if len(df_cluster_kpis_all_bands[df_cluster_kpis_all_bands['ObjectId'] == band]) != 0:
                    filename_band = "/".join(df_input['file_name'][i].split(".")[0:-1]) + "_"+ band + ".csv"
                    df_cluster_kpis_all_bands[df_cluster_kpis_all_bands['ObjectId'] == band].to_csv(filename_band, index = False)
                    df_input_current_band = df_input.iloc[[i]]
                    df_input_current_band['Cluster Name Band'] = df_input_current_band['Cluster Name'] + "_" + band
                    df_input_current_band['Band'] = band
                    df_input_current_band['file_name'] = filename_band
                    df_input_bandwise_list.append(df_input_current_band)
    
        except:
            pass
        
        i+=1
    
    
    try:
        df_input_bandwise = pd.concat(df_input_bandwise_list, ignore_index=True)
    except:
        df_job_details = df_input.copy(deep = True)
        df_job_details['Band'] = np.nan
        df_job_details['Cluster Name Band'] = np.nan
        df_job_details['Native_Cluster_Level_StartTime'] = 'Not Completed'
        df_job_details['Native_Cluster_Level_EndTime'] = 'Not Completed'
        df_job_details['EPP_KPI_Template'] = 'Not Completed'
        df_job_details['EPP_Cell_Level_StartTime'] = 'Not Completed'
        df_job_details['EPP_Cell_Level_EndTime'] = 'Not Completed'
        df_job_details['Native_Cell_Level'] = 'Not Completed'
        df_job_details['Native_Cell_Level_StartTime'] = 'Not Completed'
        df_details_print(df_job_details)
        for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            del_folders(folders_to_deleted)
        return
    
    
    df_job_details = pd.merge(df_input[['Cluster Name','List of Nodes', 'Customer', 'Owner', 'E-Mail', 'Launch Date', 'Number of pre days', 'Technology', 'EPP Template Name','start_date', 'end_date']],df_input_bandwise, how = 'outer', on =['Cluster Name','List of Nodes', 'Customer', 'Owner', 'E-Mail', 'Launch Date', 'Number of pre days', 'Technology', 'EPP Template Name','start_date', 'end_date'])
    
    
    
    df_job_details['Native_Cluster_Level_StartTime'] = 'Not Completed'
    df_job_details['Native_Cluster_Level_EndTime'] = 'Not Completed'
    df_job_details['EPP_KPI_Template'] = 'Not Completed'
    df_job_details['EPP_Cell_Level_StartTime'] = 'Not Completed'
    df_job_details['EPP_Cell_Level_EndTime'] = 'Not Completed' 
    df_job_details['Native_Cell_Level'] = 'Not Completed'
    df_job_details['Native_Cell_Level_StartTime'] = 'Not Completed'
    
    
    #df_job_details.loc[~df_job_details['Band'].isna(),'EPP_Cluster_Level'] = 'Completed'
    
    
    if len(df_job_details[df_job_details['EPP_Cluster_Level_EndTime'] != 'Not Completed']) == 0:
        df_details_print(df_job_details)
        for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            del_folders(folders_to_deleted)
        return
    
    batch_size = 10
    
    count_of_clusters = 0
    for x in batch(range(0,len(df_input_bandwise)), batch_size):
        batch_start_time = time.time()
        threadList = []
        for i in x:
            start = time.time()
            start_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))
            df_job_details.loc[df_job_details['Cluster Name Band'] == df_input_bandwise['Cluster Name Band'][i], 'Native_Cluster_Level_StartTime'] =  'Initiated: {}'.format(start_as_str)
            thread = threading.Thread(target=getDeviantKPIs, args=(df_input_bandwise['folder_name'][i], df_input_bandwise['file_name'][i],df_input_bandwise['Customer'][i],df_input_bandwise['E-Mail'][i] + ',' + native_mail_box,df_input_bandwise['Cluster Name Band'][i],'cluster',df_input_bandwise['Launch Date'][i].strftime('%Y-%m-%d'), df_input_bandwise['Band'][i], ""))
            thread.daemon = True
            threadList.append(thread)
            thread.start()
            count_of_clusters += 1
        
        count_of_clusters = count_of_clusters + batch_size
        if count_of_clusters < len(df_input_bandwise):
            count = 3
            while time.time() - batch_start_time < 5 * 60:
                if count == 3:
                    print("Waiting for 5 mins to before the next native batch")
                    count = -1
                time.sleep(30)
                count += 1
    for each in threadList:
        each.join()
        
    df_kpis = []
    i = 0
    while i<len(df_input_bandwise):
        try:
            filename = df_input_bandwise['folder_name'][i] + "/cluster_level_deviated_kpi_list_{}.csv".format(df_input_bandwise['Band'][i])
            df_kpi = pd.read_csv(filename)
            end = time.time()
            end_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))
            df_job_details.loc[df_job_details['Cluster Name Band'] == df_input_bandwise['Cluster Name Band'][i], 'Native_Cluster_Level_EndTime'] = 'Completed: {}'.format(end_as_str)
            if len(df_kpi) != 0:
                df_kpi['cluster'] = df_input_bandwise['Cluster Name'][i]
                df_kpi['report_name'] = df_input_bandwise['EPP Template Name'][i]
                df_kpi['tech'] = df_input_bandwise['Technology'][i]
                df_kpi['customer'] = df_input_bandwise['Customer'][i]
                df_kpi['node_list'] = df_input_bandwise['List of Nodes'][i]
                df_kpi['start_date'] = df_input_bandwise['start_date'][i]
                df_kpi['end_date'] = df_input_bandwise['end_date'][i]
                df_kpi['launch_date'] = df_input_bandwise['Launch Date'][i]
                df_kpi['email'] = df_input_bandwise['E-Mail'][i]
                df_kpi['cluster band'] = df_input_bandwise['Cluster Name Band'][i]
                df_kpi['band'] = df_input_bandwise['Band'][i]
                df_kpis.append(df_kpi)
            else:
                df_job_details.loc[df_job_details['Cluster Name Band'] == df_input_bandwise['Cluster Name Band'][i], 'Native_Cluster_Level_EndTime'] = 'Completed: {}; No Deviated KPIs found'.format(end_as_str)
        except:
            pass
        
        
        i+=1
    
    if len(df_kpis) == 0:
        df_details_print(df_job_details)
        for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            #del_folders(folders_to_deleted)
        return
    
    df_deviant_kpis_all_cluster = pd.concat(df_kpis, ignore_index=True)
    

    report_list = list(set(df_deviant_kpis_all_cluster['report_name'].to_list()))
    

    
    

    kpi_counters = []
    for report_name in report_list:
        try:
            df_report = pd.read_excel(report_name+'.xls',sheet_name = 'Table', header = 2)
            df_report['report_name'] = report_name
            df_job_details.loc[df_job_details['EPP Template Name'] == report_name, 'EPP_KPI_Template'] = 'Completed'
            kpi_counters.append(df_report)
        except:
            print('KPI counters for report {} not available in the template folder'.format(report_name+'.xls'))
            df_job_details['EPP_KPI_Template'] = 'Report not found'
    
    if len(kpi_counters) == 0:
        df_details_print(df_job_details)
        for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            del_folders(folders_to_deleted)
        return

    
    df_kpi_counters = pd.concat(kpi_counters, ignore_index=True)
    
    df_deviant_kpis_all_cluster = pd.merge(df_deviant_kpis_all_cluster, df_kpi_counters[['report_name','KPI Name','KPI Formula']], how = 'inner', on = ['report_name','KPI Name'])

    df_payload_epp_cell_level = df_deviant_kpis_all_cluster[['cluster','tech','customer','node_list','start_date','end_date','launch_date','email']].drop_duplicates().reset_index().drop('index',axis = 1)

    df_payload_epp_cell_level['file_name'] = np.nan
    
    KPI_LTE = []
    for i in range(0,len(df_payload_epp_cell_level)):
        df_temp = df_deviant_kpis_all_cluster.loc[(df_deviant_kpis_all_cluster['cluster'] == df_payload_epp_cell_level['cluster'][i]) & (df_deviant_kpis_all_cluster['tech'] == df_payload_epp_cell_level['tech'][i])]
        data_LTE = dict(zip(df_temp['KPI Name'],df_temp['KPI Formula']))
        globals()['kpi_LTE_'+str(i)] = []
        for row in data_LTE:
            kpi = {
                "name": row.strip(),
                "formula": data_LTE[row].strip()
            }
            globals()['kpi_LTE_'+str(i)].append(kpi)
        KPI_LTE.append('kpi_LTE_'+str(i))
    
        folder_name = df_payload_epp_cell_level['cluster'][i] + "_" + df_payload_epp_cell_level['tech'][i] + "_cell_level"
        file_name = folder_name + "/" + "pmCounterReport.csv"
        df_payload_epp_cell_level.loc[i,'file_name'] = file_name
        folders_to_deleted.append(folder_name)
        try:
            shutil.rmtree(folder_name)
        except:
            pass
        os.makedirs(folder_name, exist_ok=True)
    
    
    df_payload_epp_cell_level['start_date'] = pd.to_datetime(df_payload_epp_cell_level['start_date'], format= date_format_start)
    df_payload_epp_cell_level['end_date'] = pd.to_datetime(df_payload_epp_cell_level['end_date'], format=date_format_end)
    df_payload_epp_cell_level['start_date'] = df_payload_epp_cell_level['start_date'].dt.strftime('%Y/%m/%d 00:00')
    df_payload_epp_cell_level['end_date'] = df_payload_epp_cell_level['end_date'].dt.strftime('%Y/%m/%d 23:59')
    
    for attempt in range(1, 3):
        
            try:
                if (attempt < 3):
                    threadList = []
                    for i in range(0,len(df_payload_epp_cell_level)):
                        kpiList = globals()['kpi_LTE_'+str(i)]
                        print(f'--- Thread Starting for Cluster {df_payload_epp_cell_level["cluster"][i]}')
                        start = time.time()
                        start_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))
                        df_job_details.loc[df_job_details['Cluster Name'] == df_payload_epp_cell_level["cluster"][i],'EPP_Cell_Level_StartTime'] =  'Initiated: {}'.format(start_as_str)
                        thread = threading.Thread(target=getEPPreport_forKPIs, args=(df_payload_epp_cell_level["node_list"][i], kpiList, df_payload_epp_cell_level["start_date"][i], df_payload_epp_cell_level["end_date"][i], df_payload_epp_cell_level["cluster"][i], df_payload_epp_cell_level["tech"][i], df_payload_epp_cell_level["customer"][i]))
                        thread.daemon = True
                        threadList.append(thread)
                        thread.start()    
                    for each in threadList:
                        each.join()
                else:
                    print(f'Attempt {attempt} finally worked.')
            except Exception as error:
                print(f'Attempt {attempt} hit the exception.')
                print(error)
                print(Exception)
                if attempt==1:
                    print("Waiting for 5 Mins")
                    time.sleep(300) # try after 0.5 Mins
                    continue
            else:
                break
    else:
        print(f'Exit after final attempt: {attempt}')
    
    i=0
    native_payload_list = []
    for i in range(0,len(df_payload_epp_cell_level)):
        try:
            df_band_all_bands = pd.read_csv(df_payload_epp_cell_level['file_name'][i])
            df_band_all_bands['band'] = df_band_all_bands['ObjectId'].str.split(".").str[1].str[0]
            #df_band_all_bands['band'] = df_band_all_bands['band'].apply(band_label)
            df_band_all_bands ['band'] = df_band_all_bands.apply(lambda x: band_label(x["band"], df_payload_epp_cell_level['tech'][i]), axis=1)
            df_band_all_bands = df_band_all_bands.dropna(subset = ['band'])
            for band in list(df_band_all_bands['band'].unique()):
                df_band_current = df_band_all_bands[df_band_all_bands['band'] == band]
                fileNameBand = df_input_bandwise.loc[((df_input_bandwise['Cluster Name'] == df_payload_epp_cell_level['cluster'][i]) & (df_input_bandwise['Band'] == band)),'folder_name'].item() + '/cluster_level_deviated_kpi_list_{}.csv'.format(band)
                try:
                    kpi_list = pd.read_csv(fileNameBand)['KPI Name'].to_list()
                    kpi_list_band =['ObjectId','RecordDate','DateInDays','DateInHours','DateInTicks']
                    for kpi in kpi_list:
                        if kpi in list(df_band_all_bands):
                            kpi_list_band.append(kpi)
                    fileName_band_cell_level = "/".join(df_payload_epp_cell_level['file_name'][i].split(".")[0:-1]) + "_{}.csv".format(band)
                    df_band_current[kpi_list_band].to_csv(fileName_band_cell_level, index = False)
                    df_native_cell_native_payload_band = df_payload_epp_cell_level[['cluster','tech','customer','launch_date','email']].iloc[[i]]
                    df_native_cell_native_payload_band.loc[:,'band'] = band
                    df_native_cell_native_payload_band.loc[:,'file_name'] = fileName_band_cell_level
                    df_native_cell_native_payload_band['cluster band'] = df_native_cell_native_payload_band['cluster'] + "_" + df_native_cell_native_payload_band['band']
                    cluster_band = df_native_cell_native_payload_band['cluster'] + "_" + df_native_cell_native_payload_band['band']
                    end = time.time()
                    end_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))
                    df_job_details.loc[df_job_details['Cluster Name Band'] == cluster_band.to_list()[0], 'EPP_Cell_Level_EndTime'] = 'Completed: {}'.format(end_as_str)
                    native_payload_list.append(df_native_cell_native_payload_band)
                    df_deviant_kpis_all_cluster.loc[df_deviant_kpis_all_cluster['cluster band'] == df_native_cell_native_payload_band['cluster band'].item(), 'node_list'] = ",".join(list(df_band_current['ObjectId'].unique()))
                except:
                    pass
        except:
            pass
    
    if len(df_job_details[df_job_details['EPP_Cell_Level_EndTime'] != 'Not Completed']) == 0:
        df_details_print(df_job_details)
        for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            del_folders(folders_to_deleted)
        return
        
    df_native_cell_native_payload = pd.concat(native_payload_list, ignore_index=True)

    cluster_list_deviated_kpis = df_job_details.loc[~df_job_details['Native_Cluster_Level_EndTime'].str.contains('No Deviated KPIs'),'Cluster Name Band'].to_list()
    df_native_cell_native_payload = df_native_cell_native_payload.loc[df_native_cell_native_payload['cluster band'].isin(cluster_list_deviated_kpis)]
    df_native_cell_native_payload = df_native_cell_native_payload.reset_index().drop('index',axis=1)

    for filename in df_native_cell_native_payload['file_name'].to_list():
        remove_last_hour(filename)
    
    count_of_clusters = 0
    
    for x in batch(range(0,len(df_native_cell_native_payload)), batch_size):
        batch_start_time = time.time()
        threadList = []
        for i in x:
            df_cluster = df_deviant_kpis_all_cluster.loc[df_deviant_kpis_all_cluster['cluster band'] == df_native_cell_native_payload['cluster band'][i]]
            df_cluster = df_cluster.reset_index().drop('index',axis = 1)
            start = time.time()
            start_as_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))
            df_job_details.loc[df_job_details['Cluster Name Band'] == df_native_cell_native_payload['cluster band'][i], 'Native_Cell_Level_StartTime'] = 'Initiated :{}'.format(start_as_str)
            df_job_details.loc[df_job_details['Cluster Name Band'] == df_native_cell_native_payload['cluster band'][i], 'Native_Cell_Level'] = 'Waiting for response'
            folderName = "/".join(df_native_cell_native_payload['file_name'][i].split("/")[0:-1])
            thread = threading.Thread(target=getDeviantKPIs, args=(folderName,df_native_cell_native_payload['file_name'][i],df_native_cell_native_payload['customer'][i],df_native_cell_native_payload['email'][i] + ',' + native_mail_box,df_native_cell_native_payload['cluster band'][i],'node',df_native_cell_native_payload['launch_date'][i].strftime('%Y-%m-%d'),df_native_cell_native_payload['band'][i], df_cluster))
            thread.daemon = True
            threadList.append(thread)
            thread.start()    
        
        count_of_clusters = count_of_clusters + batch_size
        if count_of_clusters < len(df_input_bandwise):
            count = 3
            while time.time() - batch_start_time < 5 * 60:
                if count == 3:
                    print("Waiting for 5 mins to before the next native batch")
                    count = -1
                time.sleep(30)
                count += 1
    for each in threadList:
        each.join()
    
    
    
    #folders_to_deleted = list(df_input['folder_name'].unique()) + list(df_payload_epp_cell_level['file_name'].str.split("/").str[0].unique())
    
            
    df_details_print(df_job_details)
    for i in range(0, len(df_input_wo_all)):
            stopWOID(df_input_wo_all.loc[[i]].reset_index().drop(['index'],axis = 1))
            del_folders(folders_to_deleted)
    return


# In[12]:


def getWOID(cluster_name, site_list):
    
    
    DrSignum = "esuspam"
    deliverablePlanName =  "TMO_Native_Deviated_KPIs"
    Projectid = '216'
    WoName = 'NativeDeviatedKPIS'
    woRequest = {
        "ProjectID": Projectid,
        "WoPriority": "Low",
        "WoName": WoName,
        "DrSignum": DrSignum,
        "sitelist": site_list,
        "comments": "Cluster: {}".format(cluster_name),
        "deliverablePlanName": deliverablePlanName
    }
        
    woRequest  = createInstantWO(woRequest)
    wo_id = woRequest.get("WoId","")
    return wo_id


# In[13]:


def closeWOID(wo_id):
    task_name = "Data Collection and Validation"
    DrSignum = "esuspam"
    isfActionStaus(wo_id, task_name, "close", failure=False,signum=DrSignum)
    print(f'---- Work Order {wo_id} Closed')
    print(f'---- Completed at {datetime.now()}')


# In[14]:


def startWOID(df):
    for i in range(0, len(df)):
        wo_id = str(df.loc[i,'WorkOrder_id'])
        deliverablePlanName =  "TMO_Native_Deviated_KPIs"
        task_name = 'Data Collection and Validation'
        DrSignum = 'esuspam'
        statusOfRequest = {
                    "Node_count":int(len(df.loc[i, 'List of Nodes'].split(','))),
                    "ProjectID":'216',
                    "WoName":"NativeDeviatedKPIS",
                    "Source":"EPP",
                    "DrSignum":"esuspam",
                    "DataDownload":"No",
                    "CustomerUnit":"TMO",
                    "WoId": wo_id,
                    "DataType": "PM",
                    "DataDownloadedTo": "USA",
                    "AutoBOT": "Native_Fetch_AutoBOT",
                    "Team":"NPI Team",
                    "Tool":"",
                    "SourceCountry": "USA",
                    "StoredCountry": "USA",
                    "StoredLocation": "Sharepoint/MANA DM Server",
                    "Remarks": ""
                }
        
        print(f'----- WOID {wo_id} Generated for Deliverable/WF  {deliverablePlanName}')

        #mana_api = new_api(statusOfRequest)

        dashboard_pyld = {
            "WoId" : wo_id,
            "StatusCode" : "",
            "Market" : str(df.loc[i,'Cluster Name']),
            "Region" :str(df.loc[i,'Cluster Name']),
            "NodeList" : "Network_Level",
            "link" : "",
            }
        dashboard_pyld["StatusCode"] = "Native Deviated KPI Fetch for TMO: Initiated"
        
        updateDashboard(dashboard_pyld)
        isfActionStaus(wo_id, task_name, "start", failure=False, signum=DrSignum)
        print(f'-- Task {task_name} for wo {wo_id} Started in ISF')
    return


# In[15]:


def stopWOID(df):
    for i in range(0, len(df)):
        wo_id = str(df.loc[i,'WorkOrder_id'])
        deliverablePlanName =  "TMO_Native_Deviated_KPIs"
        task_name = 'Data Collection and Validation'
        DrSignum = 'esuspam'
        dashboard_pyld = {
                "WoId" : wo_id,
                "StatusCode" : "",
                "Market" : str(df.loc[i,'Cluster Name']),
                "Region" :str(df.loc[i,'Cluster Name']),
                "NodeList" : "Network_Level",
                "link" : "",
                }
        
        dashboard_pyld["StatusCode"] = "Native Deviated KPI Fetch for TMO: Completed"
        updateDashboard(dashboard_pyld)
    
        isfActionStaus(wo_id, task_name, "stop", failure=False, signum=DrSignum)
        print(f'-- Task {task_name} for wo {wo_id} Stopped in ISF')
    return


# In[16]:


def remove_last_hour(filename):
    try:
        df_data = pd.read_csv(filename)
        df_data['RecordDate'] = pd.to_datetime(df_data['RecordDate'])
        df_data = df_data.loc[df_data['RecordDate'] != df_data['RecordDate'].max()]
        df_data['RecordDate'] = df_data['RecordDate'].dt.strftime('%Y/%m/%d %H:%M')
        df_data.to_csv(filename, index = False)
    except:
        pass
    return


# In[17]:


def getJOBs(input_filename):

    try:
        input_file = spUploadDownloadAPI.downloadFiles("", "216", "TransactionalFolders", "//Native_Data",True, 0, input_filename)
        df_input = pd.read_excel (input_file,sheet_name = 'NPI_WO')
        formulas = spUploadDownloadAPI.downloadFiles("", "216", "TransactionalFolders", "//Native_Data//KPI_Templates")
    except:
        print("Input file {} not found".format(input_filename))
        raise Exception

    try:
        df_workorders = pd.read_excel ('Native_workorder_details.xlsx')
    except:
        print('Work order details file Native_workorder_details.xlsx not found')
        raise Exception

    df_input = pd.merge(df_input, df_workorders[['Cluster Name','WorkOrder_id']], how = 'left', on = 'Cluster Name')
    df_input['WorkOrder_StartDate'] = pd.to_datetime(df_input['WorkOrder_StartDate'], format='%Y-%m-%d')
    df_input['WorkOrder_EndDate'] = pd.to_datetime(df_input['WorkOrder_EndDate'], format='%Y-%m-%d')

    date_today = pd.Timestamp.today().strftime('%Y-%m-%d')

    df_input_active_new = df_input[(df_input['WorkOrder_EndDate'] >= date_today) & (df_input['WorkOrder_StartDate'] <= date_today) & (df_input['WorkOrder_id'].isna())]
    df_input_active_old = df_input[(df_input['WorkOrder_EndDate'] >= date_today) & ~(df_input['WorkOrder_id'].isna())]

    df_input_active_new = df_input_active_new.reset_index().drop('index', axis =1)
    df_input_active_old = df_input_active_old.reset_index().drop('index', axis =1)
    
    #Get Work order Ids for new jobs
    if len(df_input_active_new) != 0:
        print('Getting new Work Order IDs')
        for i in range(0, len(df_input_active_new)):
            df_input_active_new.loc[i,'WorkOrder_id'] = getWOID(df_input_active_new.loc[i, 'Cluster Name'],df_input_active_new.loc[i, 'List of Nodes'])

    df_input_active_new = df_input_active_new.reset_index().drop('index', axis =1)
    df_input_active_old = df_input_active_old.reset_index().drop('index', axis =1)

    df_input_active = pd.concat([df_input_active_new,df_input_active_old], ignore_index = True)

    return df_input_active
    


# In[18]:


def launchJOB(file_name):
    date_today = pd.Timestamp.today().strftime('%Y-%m-%d')
    try:
        df_input = getJOBs(file_name)
    except:
        raise Exception

    
    count_of_clusters = 0
    batch_size = 10

    threadList_main = []
    for x in batch(range(0,len(df_input)), batch_size):
        
        batch_start_time = time.time()
        for i in x:
            df = df_input.loc[[i]].reset_index().drop(['index'],axis = 1)
            thread_main = threading.Thread(target=Native_Data_Process_Flow, args=(df, ))
            thread_main.daemon = True
            threadList_main.append(thread_main)
            thread_main.start()
        
        count_of_clusters = count_of_clusters + batch_size
        if count_of_clusters < len(df_input):
            count = 3
            while time.time() - batch_start_time < 5 * 60:
                if count == 3:
                    print("Waiting for 5 mins to before the next batch of clusters are processed")
                    count = -1
                time.sleep(30)
                count += 1
    for each_main in threadList_main:
            each_main.join()

    file_names = find('All_Job_Initiated_TMO_*.xlsx', os.getcwd())
    all_jobs_list = []
    for file in file_names:
        df_temp = pd.read_excel(file)
        all_jobs_list.append(df_temp)
        os.remove(file)
    try:
        df_job_details_all_clusters = pd.concat(all_jobs_list, ignore_index=True)
        d1 = datetime.now().strftime("%d-%m-%H_%M_%p")
        filename_all_jobs = "All_Native_Jobs_Initiated_"+ d1 + ".xlsx"
        df_job_details_all_clusters.to_excel(filename_all_jobs, index = False)
        print("All Native jobs launched. Details can be found in file: {}".format(filename_all_jobs))
    except:
        print('Error in excecution of main process. Please check logs')

    df_input_close_work_order = df_input[(df_input['WorkOrder_EndDate'] <= date_today) & ~(df_input['WorkOrder_id'].isna())]
    df_input_close_work_order = df_input_close_work_order.reset_index().drop('index', axis =1)

    if len(df_input_close_work_order) != 0:
        print('Closing  Work Orders Ending today', date_today)
        for i in range(0, len(df_input_close_work_order)):
            closeWOID(df_input_close_work_order.loc[i, 'WorkOrder_id'])

    workorders_closed = list(df_input_close_work_order['Cluster Name'].unique())
    df_input.loc[~df_input['Cluster Name'].isin(workorders_closed)].to_excel("Native_workorder_details.xlsx", index=False)

    return
    


# In[19]:


if __name__ == "__main__":
    
    ####################################################################   
    # Start of Main Body
    ####################################################################

    timeToStart = 6
    timeToStart_minutes = 30
    #timeToStart = datetime.now().hour
    #AllowedTimeToStart = [7,11]
    isAlreadyRun = False
    #def get_timeToStart(currentHour, AllowedTimeToStart):
    #    for runtime in AllowedTimeToStart:
    #        if runtime >= currentHour:
    #            return runtime
    #    return None  # If no number is found

    while (True):
        currentTime = datetime.now()
        #timeToStart = get_timeToStart(currentTime.hour, AllowedTimeToStart)
        if currentTime.hour != timeToStart and isAlreadyRun == False:
            if currentTime.hour < timeToStart:
                deltaInMinutes = (timeToStart - currentTime.hour) * 60 - currentTime.minute
            else:
                deltaInMinutes = timeToStart * 60 + (24 - currentTime.hour) * 60 - currentTime.minute

            print(datetime.now().replace(microsecond=0),
                    ": Scheduled start time is {}, sleeping for {} minutes".format(timeToStart, deltaInMinutes))
            time.sleep(deltaInMinutes * 60)
            print(datetime.now().replace(microsecond=0), ": Woke up for execution", datetime.now())

        elif currentTime.hour == timeToStart and currentTime.minute == timeToStart_minutes and isAlreadyRun == False:
            print(datetime.now().replace(microsecond=0), ": Execution time")
            isAlreadyRun = True
            try:
                launchJOB('NpiClustersWO_TMO_Inputs.xlsx')
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print(datetime.now(), " : Exception while running doprocess function : ", e)

            collected = gc.collect()
            currentTime = datetime.now()
            if currentTime.hour != timeToStart:
                break

        elif currentTime.hour == timeToStart and isAlreadyRun == True:
            print(datetime.now().replace(microsecond=0), ": Going to sleep for 60 minutes")
            time.sleep(65 * 60)
            isAlreadyRun = False
            break
        else:
            #print(datetime.now().replace(microsecond=0), ": Else")
            isAlreadyRun = False     






