#!/usr/bin/env python
# coding: utf-8

# In[1]:



#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#####---------Developer @ Juhin Saha Developed on 11.02.2022-------Co-developer @Rishav Kumar Agrahari on 07-10-2022

import pandas as pd
import numpy as np
import openpyxl
import xlsxwriter
import xlrd
import collections
import sys
import time
import os
from collections import Counter

def doProcess(INPATH=".", OUTPATH="."):
    
    try:

        INPATH="C:\\Users\\ezagrri\\CWC\\New folder\\"
        OUTPATH=INPATH

        df_output=pd.DataFrame()

        PPLUS_file=INPATH + "Inception_PPLUS_Export.xlsx"

        df_PPLUS = pd.read_excel(PPLUS_file, sheet_name='Daten')


        Input_file=INPATH + "Input.xlsx"
        df_input = pd.read_excel(Input_file, sheet_name='IP')

        #print(df_input)

        df_input=df_input[["Input"]]

        df_input=df_input[2:]
        #print(df_input)

        Tkt_list=df_input['Input'].to_list()
        #print(Tkt_list)

        Length=len(Tkt_list)

        file_loc = INPATH + "TTWOS_Export.xlsx"
        df = pd.read_excel(file_loc, sheet_name='Sheet1')

        df=df[["Ticket Number", "Loc Identifier +","Status","PPluS MN-ID","Assignee Name +"]]
        #print(df)
        #print(df.columns.tolist())

        df_filtered=df[df['Ticket Number'].isin(Tkt_list)]

        df_filtered[['PPluS MN-ID']] = df_filtered[['PPluS MN-ID']].fillna('12345')
        df_filtered[['PPluS MN-ID']] = df_filtered[['PPluS MN-ID']].astype('int')

        df_filtered = df_filtered.reset_index(drop=True)

        #print(df_filtered)

        df_filtered = df_filtered.reset_index(drop=True)

        #print(df_filtered)

        LocId=df_filtered['Loc Identifier +'].to_list()
        #print(LocId)

        Assignee_list=["amit.shrivastava","apurbakumar.chattopadhyay","chayan.dhara","somasis.maji1","dilipkumar.nayak","debasis.pattanaik","amitkumar.poddar","mithun.neogi","sachin.sood1","suchisuvra.banerjee","mobarakhossen.sekh","prasun.chakraborty","aritra.majumdar","sagar.bahekar1","subhramkesari.parida"]

        ##########----------Filtering based on single ticket number in Input list------------* need iteration for more tickets

        for i in range(Length):
            try:
                rslt_df = df_filtered.loc[df_filtered['Ticket Number'] == Tkt_list[i]]

                #print(rslt_df)

                site=str(rslt_df.iloc[0,1])
                #print(site)
                last_letter = site[4]
                last_letter = last_letter.upper()
                rest = site[0:4]
                site=str(site)

                #print(last_letter,rest)
                PPluS_MN_ID=rslt_df.iloc[0,3]
                print(PPluS_MN_ID)


                #####----------"Ticket missing infra"-----------

                if PPluS_MN_ID == 12345:
                    print("NOK: - Ticket missing infra")
                    Error_3="TTWOS PPluS MN-ID missing"
                else:
                    print("OK: - Ticket is having Infra present")
                    Error_3=""

                ####-----------"Ticket missing ownership (as in tracker)"----------

                owner=rslt_df.iloc[0,4]

                #print(owner)

                if owner in Assignee_list:
                    print("\nTicket has valid owner")
                    Error_1=""

                else:
                    print("\nNot a valid owner")
                    Error_1="Not a valid owner " + str(owner)

                ##########------Status (In process or other)----------

                status = rslt_df.iloc[0,2]

                if ((status == "In Process") and (PPluS_MN_ID != 12345)):
                    print("\nTicket is In Process state")
                    Error_2=""
                elif ((PPluS_MN_ID == 12345) and (status == "In Process")):
                    print("\nTicket missing infra with In Process state")
                    Error_2="TTWOS Ticket missing infra with State:- In Process"
                elif ((PPluS_MN_ID == 12345) and (status != "In Process")):
                    print("\nTicket missing infra but not In Process state")
                    Error_2="TTWOS Status NOK: " + str(status)
                else:
                    print("\nTicket is not In Process state")
                    Error_2="TTWOS Status NOK: " + str(status)
                    print(Error_2)

                ###########---------working with PPLUS_Export-----

                df_first = df_PPLUS.loc[df_PPLUS['[1] Standort-Code'] == rest]
                df_second = df_first.loc[df_first['[1] Niederlassung'] == last_letter]

                df_second=df_second[["[1] Niederlassung", "[1] Standort-Code","[1] Maßnahmen_ID","[1] Bemerkung","[1] ZV-Kategorie","[1] Angelegt von","[1] Maßnahmenname","[2] NE-Typ-Detail","[2] Bemerkung","[2] ZV-Projekt","[2] Angelegt von","[2] FUN_SGC_Partner","[2] Zieltermin_Soll","[2] Activity Type","[2] WBS-Typ","[1] Start Rang","[1] Antragsformular an Akquise übergeben Rang","[1] Akquisitionsstart Rang","[1] Status","[2] Maßnahmentyp","[2] ZV-Kategorie","[2] StoB_Art","[2] Status","[2] Site_Sharing_Adapter"]]

                df_second = df_second.reset_index(drop=True)
                df_second[['[1] Maßnahmen_ID']] = df_second[['[1] Maßnahmen_ID']].astype('int')
                df_second[['[1] Bemerkung']] = df_second[['[1] Bemerkung']].astype('str')
                #df_second[['[2] Bemerkung']] = df_second[['[2] Bemerkung']].astype('str')
                df_second[['[1] ZV-Kategorie']] = df_second[['[1] ZV-Kategorie']].astype('str')
                df_second[['[2] NE-Typ-Detail']] = df_second[['[2] NE-Typ-Detail']].astype('str')
                df_second[['[2] Maßnahmentyp']] = df_second[['[2] Maßnahmentyp']].astype('str')
                df_second[['[2] Activity Type']] = df_second[['[2] Activity Type']].astype('str')
                df_second[['[2] WBS-Typ']] = df_second[['[2] WBS-Typ']].astype('str')
                df_second[['[2] ZV-Kategorie']] = df_second[['[2] ZV-Kategorie']].astype('str')
                df_second[['[2] StoB_Art']] = df_second[['[2] StoB_Art']].astype('str')
                df_second[['[2] Status']] = df_second[['[2] Status']].astype('str')
                df_second[['[2] Site_Sharing_Adapter']] = df_second[['[2] Site_Sharing_Adapter']].astype('str')
                #df_second['[2] Zieltermin_Soll'] = pd.to_datetime(df_second['[2] Zieltermin_Soll'], format='%d.%m.%Y')
                #df_second['[1] Start Rang'] = pd.to_datetime(df_second['[1] Start Rang'], format='%d.%m.%Y')
                #df_second['[1] Antragsformular an Akquise übergeben Rang'] = pd.to_datetime(df_second['[1] Antragsformular an Akquise übergeben Rang'], format='%d.%m.%Y')
                #df_second['[1] Akquisitionsstart Rang'] = pd.to_datetime(df_second['[1] Akquisitionsstart Rang'], format='%d.%m.%Y')


                search="Inception"
                df_third=df_second[df_second["[1] Bemerkung"].str.startswith(search)==True]
                df_Open=df_second[~df_second["[1] Bemerkung"].str.startswith(search)==True]

                df_Open = df_Open.reset_index(drop=True)
                df_third = df_third.reset_index(drop=True)

                #print(df_third)
                #print(df_Open)

                ###########----------"Infra missing Inception comment"---------------

                missing_Inception=len(df_third)

                if missing_Inception == 0:
                    print("NOK:- Ticket Infra missing Inception comment")
                    Error_5="NOK:- Ticket Infra missing Inception comment"
                else:
                    print("OK:- Ticket having Infra Inception comment")
                    Error_5=""

                filter_df_count=len(df_third)

                ###########-------"Check if site has multiple Infras with Inception"-------

                df_Infracheck=df_PPLUS.loc[df_PPLUS['[1] Maßnahmen_ID'] == PPluS_MN_ID]
                Infracheck_list_total=df_Infracheck['[1] Maßnahmen_ID'].to_list()
                Infracheck_list_total=list(set(Infracheck_list_total))
                length_Infracheck_list_total=len(Infracheck_list_total)

                Infracheck_list_filtered=df_third['[1] Maßnahmen_ID'].to_list()
                Infracheck_list_filtered=list(set(Infracheck_list_filtered))
                length_Infracheck_list_filtered=len(Infracheck_list_filtered)

                Count_InfraID=len(df_Infracheck)

                if Count_InfraID==filter_df_count:
                    print("\nunique InfraID present")
                    Error_7=""
                elif Count_InfraID==0:
                    print("\nTicket missing infra")
                    Error_7="NOK - Ticket missing infra"
                elif length_Infracheck_list_filtered != length_Infracheck_list_total:
                    print("\nmultiple InfraIDs")
                    Error_7="Multiple Infra with Inception"

                ###########-----------"Ticket with wrong infra"------------
                wrong_infra=[]
                InfraID_list=df_third['[1] Maßnahmen_ID'].to_list()

                InfraID_list=list(set(InfraID_list))
                print(InfraID_list)


                if InfraID_list[0]==PPluS_MN_ID:
                    print("\nTicket with correct infra")
                    Error_4=""
                elif PPluS_MN_ID == 12345:
                    print("\nTicket missing infra")
                    Error_4="Ticket missing infra in TTWOS Export"
                    wrong_infra.append(Error_4)
                elif (len(InfraID_list))>1:
                    for t in range(len(InfraID_list)):
                        if InfraID_list[t]==PPluS_MN_ID:
                            continue
                        else:
                            df_wrong_infra=df_third.loc[df_third['[1] Maßnahmen_ID'] == int(InfraID_list[t])]
                            band_wrong_infra_list= df_wrong_infra['[2] NE-Typ-Detail'].to_list()
                            band_wrong_infra_list=list(set(band_wrong_infra_list))
                            Error_4="wrong infra ID: " + ' & '.join([str(ele) for ele in band_wrong_infra_list])
                            wrong_infra.append(Error_4)
                            print("\nTicket with wrong infra")

                combined4=' '.join([str(x) for x in wrong_infra])
                Error_4=combined4
                print(Error_4)
                ##

                ############--------"Infra type is wrong (Neubau)"-----------
                Infra_type_wrong=[]
                Infra_type="Neubau Infrastruktur"

                Infra_type_pre_filter=df_third['[1] ZV-Kategorie'].to_list()
                Infra_type_pre_filter=list(set(Infra_type_pre_filter))

                df_Infra_type=df_third.loc[df_third['[1] ZV-Kategorie'] == Infra_type]

                InfraType_list=df_Infra_type['[1] ZV-Kategorie'].to_list()
                InfraType_list=list(set(InfraType_list))



                print(InfraType_list)

                length_InfraType_list=len(InfraType_list)

                if length_InfraType_list == 1:
                    if str(InfraType_list[0]) == Infra_type:
                        print("\nCorrect Infra Type present: - Neubau Infrastruktur")
                        Error_6=""
                    else:
                        print("\n Infra type is wrong")
                        df_Infra_type1=df_third.loc[df_third['[1] ZV-Kategorie'] == str(InfraType_list[0])]
                        band_wrong_infra_type_list= df_Infra_type1['[2] NE-Typ-Detail'].to_list()
                        Infra_type_wrong.append("Infra Type Wrong:" + str(band_wrong_infra_type_list[0]))

                elif length_InfraType_list == 0:
                    Error_6="All Infra type entry is wrong: " + str(Infra_type_pre_filter[0])
                    Infra_type_wrong.append(Error_6)
                    print("All Infra type entry is wrong")

                elif length_InfraType_list>1:
                    for s in range(length_InfraType_list):
                        if str(InfraType_list[s])==Infra_type:
                            continue
                        else:
                            df_wrong_type=df_third.loc[df_third['[1] ZV-Kategorie'] == str(InfraType_list[s])]
                            band_wrong_infra_type= df_wrong_type['[2] NE-Typ-Detail'].to_list()
                            band_wrong_infra_type=list(set(band_wrong_infra_type))
                            Error_6="Infra Type Wrong:" + ' & '.join([str(ele) for ele in band_wrong_infra_type])
                            Infra_type_wrong.append(Error_6)
                            print("\nTicket with wrong infra type")

                combined6=' '.join([str(x1) for x1 in Infra_type_wrong])
                Error_6=combined6
                print(Error_6)

                ##

                ################--------------"Measures under infra missing Ticket comment or ticket number is wrong"--------

                to_add=[]

                df10=df_third
                df10[['[2] Bemerkung']] = df10[['[2] Bemerkung']].astype('str')

                TktPPLUS_list=df10['[2] Bemerkung'].to_list()
                #length_TktPPLUS_list_pre=len(TktPPLUS_list)

                TktPPLUS_list=list(set(TktPPLUS_list))

                print(TktPPLUS_list)

                length_TktPPLUS_list_post=len(TktPPLUS_list)
                check_for_nan = df_third['[2] Bemerkung'].isnull().values.any()



                if length_TktPPLUS_list_post==1:
                    if TktPPLUS_list[0]==str(Tkt_list[i]):
                        print("ticket number is correct")
                        Error_10 =""
                        #to_add.append(Error_10)
                    elif TktPPLUS_list[0]!=str(Tkt_list[i]) and TktPPLUS_list[0]!='nan':
                        df_tkt_1=df10.loc[df10['[2] Bemerkung'] == TktPPLUS_list[0]]
                        band_tkt_1=df_tkt_1['[2] NE-Typ-Detail'].to_list()
                        print("ticket number is wrong")
                        Error_10 ="Ticket Number wrong: " + str(band_tkt_1[0])
                        to_add.append(Error_10)
                    elif TktPPLUS_list[0]!=str(Tkt_list[i]) and TktPPLUS_list[0]=='nan':
                        df_tkt_1=df10.loc[df10['[2] Bemerkung'] == TktPPLUS_list[0]]
                        band_tkt_1=df_tkt_1['[2] NE-Typ-Detail'].to_list()
                        print("ticket number is wrong")
                        Error_10 ="Ticket Number Missing: " + str(band_tkt_1[0])
                        to_add.append(Error_10)
                elif length_TktPPLUS_list_post>1:
                    for p in range(length_TktPPLUS_list_post):
                        if TktPPLUS_list[p]==str(Tkt_list[i]):
                            print("ticket number is correct")
                            Error_10 =""
                            continue
                        elif TktPPLUS_list[p]!=str(Tkt_list[i]) and TktPPLUS_list[p]!= 'nan':
                            print("ticket number is wrong")
                            df_tkt=df10.loc[df10['[2] Bemerkung'] == TktPPLUS_list[p]]
                            Tktband_list=df_tkt['[2] NE-Typ-Detail'].to_list()
                            band10=' - '.join([str(u1) for u1 in Tktband_list])
                            Error_10 =" Ticket Number wrong: " + band10
                            to_add.append(Error_10)
                        elif TktPPLUS_list[p]!=str(Tkt_list[i]) and TktPPLUS_list[p]== 'nan':
                            print("ticket number is wrong")
                            df_tkt=df10.loc[df10['[2] Bemerkung'] == TktPPLUS_list[p]]
                            Tktband_list=df_tkt['[2] NE-Typ-Detail'].to_list()
                            band10=' - '.join([str(u1) for u1 in Tktband_list])
                            Error_10 =" Ticket Number Missing: " + band10
                            to_add.append(Error_10) 

                combined_10=' And '.join([str(x) for x in to_add])
                Error_10=combined_10
                print(Error_10)

                ################--------------"Check if there are multiple Fun NEU for the same layer"--------    

                layer_list=df_third['[2] NE-Typ-Detail'].to_list()
                layer_list_duplicate=layer_list
                layer_list_duplicate=[item for item, count in collections.Counter(layer_list).items() if count > 1]
                layer_list=list(set(layer_list))

                length_layer_list=len(layer_list)

                if length_layer_list == missing_Inception:
                    print("Unique Fun NEU for the layer")
                    Error_9=""
                else:
                    str_multiple=' - '.join([str(m) for m in layer_list_duplicate])
                    print("multiple Fun NEU for the same layer")
                    Error_9="Multiple Fun NEU: " + str_multiple
                ##

                ######------"Check if same infra has multiple projects from same BO user"-----------


                multiple_user=df_third['[2] Angelegt von'].to_list()
                multiple_user=list(set(multiple_user))

                length_multiple_user=len(multiple_user)

                Erorr_list=[]
                Erorr_list1=[]
                Erorr_list2=[]

                for j in range(length_multiple_user):
                    df_fourth = df_third.loc[df_third['[2] Angelegt von'] == multiple_user[j]]
                    multi_proj=df_fourth['[2] ZV-Projekt'].to_list()
                    multi_proj=list(set(multi_proj))

                    length_multi_proj=len(multi_proj)

                    if length_multi_proj==1:
                        print("same infra has unique project from same BO user")
                        User_proj=""
                        #Erorr_list.append(User_proj)
                        #Error_11="OK - same infra has unique project from same BO user"
                        continue

                    elif length_multi_proj>1:
                        print("same infra has multiple projects from same BO user")
                        for a in range(length_multi_proj):
                            df_inter=df_fourth.loc[df_fourth['[2] ZV-Projekt'] == str(multi_proj[a])]
                            Bands_list=df_inter['[2] NE-Typ-Detail'].to_list()
                            length_Bands_list=len(Bands_list)
                            for m in range(length_Bands_list):
                                Erorr_list1.append(Bands_list[m])

                        #cnt = Counter(Erorr_list1)
                        Erorr_list=[k for k, v in collections.Counter(Erorr_list1).items() if v > 1]
                        band_error='-'.join([str(elements) for elements in Erorr_list])
                        Erorr_list2.append(" Multiple Projects:" + band_error)

                combined11=' And '.join([str(s) for s in Erorr_list2])
                Error_11=combined11
                print(Error_11)
                ##

                ########------------"Check for attribute errors - FUN_SGC_Partner"---------
                FUN_SGC_wrong=[]

                df11=df_third
                df11[['[2] FUN_SGC_Partner']] = df11[['[2] FUN_SGC_Partner']].astype('str')

                FUN_SGC_Partner="Inception Eri"

                FUN_SGC_list=df11['[2] FUN_SGC_Partner'].to_list()

                FUN_SGC_list=list(set(FUN_SGC_list))

                print(FUN_SGC_list)

                length_FUN_SGC_list=len(FUN_SGC_list)

                check_for_nan_FUN_SGC = df_third['[2] FUN_SGC_Partner'].isnull().values.any()

                if length_FUN_SGC_list==1:
                    if FUN_SGC_list[0] == FUN_SGC_Partner:
                        print("\nCorrect FUN_SGC_Partner: - Inception Eri")
                        Error_12=""
                        #FUN_SGC_list.append(Error_12)
                    elif FUN_SGC_list[0]!=FUN_SGC_Partner and FUN_SGC_list[0]!='nan':
                        df_fun_1=df11.loc[df11['[2] FUN_SGC_Partner'] == FUN_SGC_list[0]]
                        band_fun_1=df_fun_1['[2] NE-Typ-Detail'].to_list()
                        print("Fun SGC is wrong")
                        Error_12 ="FUN_SGC_Partner wrong: " + str(band_fun_1[0])
                        FUN_SGC_wrong.append(Error_12)
                    elif FUN_SGC_list[0]!=FUN_SGC_Partner and FUN_SGC_list[0]=='nan':
                        df_fun_1=df11.loc[df11['[2] FUN_SGC_Partner'] == FUN_SGC_list[0]]
                        band_fun_1=df_fun_1['[2] NE-Typ-Detail'].to_list()
                        print("Fun_SGC is Missing")
                        Error_12 ="FUN_SGC_Partner Missing: " + str(band_fun_1[0])
                        FUN_SGC_wrong.append(Error_12)


                elif length_FUN_SGC_list>1:
                    for p1 in range(length_FUN_SGC_list):
                        if FUN_SGC_list[p1]==FUN_SGC_Partner:
                            print("\nCorrect FUN_SGC_Partner")
                            Error_12 =""
                            continue
                        elif FUN_SGC_list[p1]!=FUN_SGC_Partner and FUN_SGC_list[p1]!= 'nan':
                            print("FUN_SGC_Partner is wrong")
                            df_fun=df11.loc[df11['[2] FUN_SGC_Partner'] == FUN_SGC_list[p1]]
                            funband_list=df_fun['[2] NE-Typ-Detail'].to_list()
                            band12=' - '.join([str(v2) for v2 in funband_list])
                            Error_12 =" FUN_SGC_Partner wrong: " + band12
                            FUN_SGC_wrong.append(Error_12)
                        elif FUN_SGC_list[p1]!=FUN_SGC_Partner and FUN_SGC_list[p1]== 'nan':
                            print("Fun_SGC is Missing")
                            df_fun=df11.loc[df11['[2] FUN_SGC_Partner'] == FUN_SGC_list[p1]]
                            funband_list=df_fun['[2] NE-Typ-Detail'].to_list()
                            band12=' - '.join([str(v2) for v2 in funband_list])
                            Error_12 =" FUN_SGC_Partner Missing: " + band12
                            FUN_SGC_wrong.append(Error_12) 

                combined_12=' And '.join([str(x2) for x2 in FUN_SGC_wrong])
                Error_12=combined_12
                print(Error_12)


                ########------------"Check for attribute errors - Zieltermin_Soll"---------
                attribute_error=[]
                attribute_error1=[]

                df13=df_third
                df13[['[2] Zieltermin_Soll']] = df13[['[2] Zieltermin_Soll']].astype('str')

                att_list=df13['[2] Zieltermin_Soll'].to_list()
                #most_common=mode(att_list)

                att_list=list(set(att_list))

                print(att_list)

                length_att_list=len(att_list)
                check_for_nan_att=False

                for o in range(length_att_list):
                    if att_list[o]=='nan':
                        check_for_nan_att=True

                if length_att_list==1:
                    if check_for_nan_att==False:
                        print("\nNot an attribute errors - Zieltermin_Soll")
                        Error_13 =""
                        #to_add.append(Error_10)
                    else:
                        print("\nattribute errors - Zieltermin_Soll, total entry is empty in column Zieltermin_Soll")
                        Error_13=" NOK :- empty Zieltermin_Soll column"
                        attribute_error.append(Error_13)
                elif length_att_list==2:
                    att_list_not_nan = [att_list for att_list in att_list if str(att_list) != 'nan']
                    length_att_list_not_nan=len(att_list_not_nan)
                    if length_att_list_not_nan==2:
                        print("Zieltermin_Soll different")
                        att_list_diff1='-'.join([str(e2) for e2 in att_list_not_nan])
                        attribute_error.append("Zieltermin_Soll different:" + att_list_diff1)

                    elif length_att_list_not_nan==1:
                        print("Zieltermin_Soll missing")
                        for c1 in range(length_att_list):
                            if att_list[c1]=='nan':
                                print("Zieltermin_Soll missing")
                                df_soll=df13.loc[df13['[2] Zieltermin_Soll'] == att_list[c1]]
                                band_soll=df_soll['[2] NE-Typ-Detail'].to_list()
                                band13=' - '.join([str(d1) for d1 in band_soll])
                                Error_13 ="Zieltermin_Soll missing: " + band13
                                attribute_error.append(Error_13)

                    #diff_soll='-'.join([str(e1) for e1 in attribute_error1])
                    #attribute_error.append("Zieltermin_Soll different:" + diff_soll)

                elif length_att_list>2:
                        for c1 in range(length_att_list):
                            if att_list[c1]=='nan':
                                print("Zieltermin_Soll missing")
                                df_soll=df13.loc[df13['[2] Zieltermin_Soll'] == att_list[c1]]
                                band_soll=df_soll['[2] NE-Typ-Detail'].to_list()
                                band13=' - '.join([str(d1) for d1 in band_soll])
                                Error_13 ="Zieltermin_Soll missing: " + band13
                                attribute_error.append(Error_13)
                            else:
                                print("Zieltermin_Soll different")
                                #att_list = [att_list for att_list in att_list if str(att_list) != 'nan']
                                #att_list_diff=' - '.join([str(e1) for e1 in att_list])
                                att_list_diff=str(att_list[c1])
                                attribute_error1.append(att_list_diff)

                        diff_soll='-'.join([str(e1) for e1 in attribute_error1])
                        attribute_error.append("Zieltermin_Soll different:" + diff_soll)

                combined_13=' And '.join([str(x5) for x5 in attribute_error])
                Error_13=combined_13
                print(Error_13)
                ##

                ########------------"Bands Present"---------

                Band_present=df_third['[2] NE-Typ-Detail'].to_list()
                print(Band_present)
                Error_17=','.join([str(e) for e in Band_present])
                print(Error_17)

                ####-----------"Number of MS advanced"--------------

                milestone=[]

                df16=df_third
                df16[['[1] Start Rang']] = df16[['[1] Start Rang']].astype('str')
                df16[['[1] Antragsformular an Akquise übergeben Rang']] = df16[['[1] Antragsformular an Akquise übergeben Rang']].astype('str')
                df16[['[1] Akquisitionsstart Rang']] = df16[['[1] Akquisitionsstart Rang']].astype('str')

                len1=len(df16)
                ms100=df16['[1] Start Rang'].to_list()
                ms150=df16['[1] Antragsformular an Akquise übergeben Rang'].to_list()
                ms200=df16['[1] Akquisitionsstart Rang'].to_list()

                len_ms100=len(ms100)
                len_ms150=len(ms150)
                len_ms200=len(ms200)

                list_after_nan_removal_ms100=[ms100 for ms100 in ms100 if str(ms100) != 'NaT']
                list_after_nan_removal_ms150=[ms150 for ms150 in ms150 if str(ms150) != 'NaT']
                list_after_nan_removal_ms200=[ms200 for ms200 in ms200 if str(ms200) != 'NaT']

                #print(list_after_nan_removal_ms100,list_after_nan_removal_ms150,list_after_nan_removal_ms200)

                unique_len_ms100=len(list(set(list_after_nan_removal_ms100)))
                unique_len_ms150=len(list(set(list_after_nan_removal_ms150)))
                unique_len_ms200=len(list(set(list_after_nan_removal_ms200)))

                print(unique_len_ms100,unique_len_ms150,unique_len_ms200)


                if ((len1==len_ms100) and (unique_len_ms150==0) and (unique_len_ms200==0)):
                    milestone.append("P(100)")
                elif ((len1==len_ms100) and (len1==len_ms150) and (unique_len_ms200==0)):
                    milestone.append("P(100) & R(150)")
                elif ((len1==len_ms100) and (len1==len_ms150) and (len1==len_ms200)):
                    milestone.append("P(100) & R(150) & T(200)")

                Error_16= ''.join([str(le) for le in milestone])
                print(Error_16)

                ###-----------"Open measures"------------

                df_Open=df_Open[["[1] Maßnahmen_ID", "[1] Maßnahmenname","[1] Angelegt von","[2] NE-Typ-Detail"]]

                df_Open[['[1] Maßnahmen_ID']] = df_Open[['[1] Maßnahmen_ID']].astype('str')
                df_Open[['[1] Maßnahmenname']] = df_Open[['[1] Maßnahmenname']].astype('str')
                df_Open[['[1] Angelegt von']] = df_Open[['[1] Angelegt von']].astype('str')
                df_Open[['[2] NE-Typ-Detail']] = df_Open[['[2] NE-Typ-Detail']].astype('str')


                df_Open['Open_measures'] = "(" + df_Open['[1] Maßnahmen_ID'].map(str) + " - " + df_Open['[1] Maßnahmenname'].map(str) + " - " + df_Open['[1] Angelegt von'].map(str) + " - " + df_Open['[2] NE-Typ-Detail'].map(str) + ")"

                Open_measures_list=df_Open['Open_measures'].to_list()
                Error_18 = ' & '.join([str(l) for l in Open_measures_list])

                print(Error_18)

                ######---------------"There is empty infra from Marius S. and we didn't use it"----------


                df_empty = df_Open.loc[df_Open['[1] Angelegt von'] == "Schymczyk,Marius"]

                df_non_G2L=df_empty[~df_empty["[1] Maßnahmenname"].str.contains("G2L")==True]

                df_non_DSS=df_non_G2L[~df_non_G2L["[1] Maßnahmenname"].str.contains("DSS")==True]

                df_non_Sunset=df_non_DSS[~df_non_DSS["[1] Maßnahmenname"].str.contains("Sunset")==True]

                empty_infra_length=len(df_non_Sunset)

                if empty_infra_length==0:
                    print("There is no empty infra from Marius S. that we didn't use it")
                    Error_8=""
                else:
                    print("There is empty infra from Marius S. and we didn't use it")
                    df_non_Sunset['empty_infra'] = df_non_Sunset['[1] Maßnahmen_ID'].map(str) + " - " + df_non_Sunset['[2] NE-Typ-Detail'].map(str)
                    empty_infra_list=df_non_Sunset['empty_infra'].to_list()
                    Error_8 = "Empty infra from Marius S: " + ' & '.join([str(b) for b in empty_infra_list])

                print(Error_8)


                ####--------------" Check for missing WBS or wrong prefix - Error_14"--------------

                WBS_wrong=[]

                df21=df_third
                df21[['[2] Activity Type']] = df21[['[2] Activity Type']].astype('str')

                WBS_Typ="F1A"

                WBS_Typ_list=df21['[2] Activity Type'].to_list()

                WBS_Typ_list=list(set(WBS_Typ_list))

                print(WBS_Typ_list)

                length_WBS_Typ_list=len(WBS_Typ_list)

                check_for_nan_WBS_Typ = df21['[2] Activity Type'].isnull().values.any()

                if length_WBS_Typ_list==1:
                    if (str(WBS_Typ_list[0]).casefold()) == ("F1A".casefold()):
                        print("\nOK:- Correct mapping for Activity Type")
                        Error_14=""

                    elif (str(WBS_Typ_list[0]).casefold())!=("F1A".casefold()) and WBS_Typ_list[0]!='nan':
                        df_WBS_Typ_1=df21.loc[df21['[2] Activity Type'] == WBS_Typ_list[0]]
                        band_WBS_Typ_1=df_WBS_Typ_1['[2] NE-Typ-Detail'].to_list()
                        print("Activity Type is wrong")
                        Error_14="Activity Type Wrong: " + str(band_WBS_Typ_1[0])
                        WBS_wrong.append(Error_14)
                    elif (str(WBS_Typ_list[0]).casefold())!=("F1A".casefold()) and WBS_Typ_list[0]=='nan':
                        df_WBS_Typ_1=df21.loc[df21['[2] Activity Type'] == WBS_Typ_list[0]]
                        band_WBS_Typ_1=df_WBS_Typ_1['[2] NE-Typ-Detail'].to_list()
                        print("Activity Type is Missing")
                        Error_14 ="Activity Type Missing: " + str(band_WBS_Typ_1[0])
                        WBS_wrong.append(Error_14)


                elif length_WBS_Typ_list>1:
                    for p2 in range(length_WBS_Typ_list):
                        if (str(WBS_Typ_list[p2]).casefold())==("F1A".casefold()):
                            print("\nOK:- Correct mapping for Activity Type")
                            Error_14 =""
                            continue
                        elif (str(WBS_Typ_list[p2]).casefold())!=("F1A".casefold()) and WBS_Typ_list[p2]!= 'nan':
                            print("Activity Type is wrong")
                            df_WBS_Typ=df21.loc[df21['[2] Activity Type'] == WBS_Typ_list[p2]]
                            WBS_Typ_Band_list=df_WBS_Typ['[2] NE-Typ-Detail'].to_list()
                            band14=' - '.join([str(v2) for v2 in WBS_Typ_Band_list])
                            Error_14 ="Activity Type Wrong: " + band14
                            WBS_wrong.append(Error_14)
                        elif (str(WBS_Typ_list[p2]).casefold())!=("F1A".casefold()) and WBS_Typ_list[p2]== 'nan':
                            print("Activity Type is Missing")
                            df_WBS_Typ=df21.loc[df21['[2] Activity Type'] == WBS_Typ_list[p2]]
                            WBS_Typ_Band_list=df_WBS_Typ['[2] NE-Typ-Detail'].to_list()
                            band14=' - '.join([str(v2) for v2 in WBS_Typ_Band_list])
                            Error_14 ="Activity Type Missing: " + band14
                            WBS_wrong.append(Error_14) 

                combined_14=' And '.join([str(x9) for x9 in WBS_wrong])
                Error_14=combined_14
                print(Error_14)
                
            


            ####--------------" Check for WBS AUC or FA"--------------

                WBS_type=[]

                df31=df_third
                df31[['[2] WBS-Typ']] = df31[['[2] WBS-Typ']].astype('str')

                WBS_Typ1="AuC"

                WBS_Typ1_list=df31['[2] WBS-Typ'].to_list()

                WBS_Typ1_list=list(set(WBS_Typ1_list))

                print(WBS_Typ1_list)

                length_WBS_Typ1_list=len(WBS_Typ1_list)

                check_for_nan_WBS_Typ1 = df31['[2] WBS-Typ'].isnull().values.any()

                if length_WBS_Typ1_list==1:
                    if (str(WBS_Typ1_list[0]).casefold()) == ("AuC".casefold()):
                        print("\nOK:- Correct mapping for WBS Type")
                        Error_15=""

                    elif (str(WBS_Typ1_list[0]).casefold())!=("AuC".casefold()) and WBS_Typ1_list[0]!='nan':
                        df_WBS_Typ1_1=df31.loc[df31['[2] WBS-Typ'] == WBS_Typ1_list[0]]
                        band_WBS_Typ1_1=df_WBS_Typ1_1['[2] NE-Typ-Detail'].to_list()
                        print("WBS Type is wrong")
                        Error_15="WBS Type Wrong: " + str(band_WBS_Typ1_1[0])
                        WBS_type.append(Error_15)
                    elif (str(WBS_Typ1_list[0]).casefold())!=("AuC".casefold()) and WBS_Typ1_list[0]=='nan':
                        df_WBS_Typ1_1=df31.loc[df31['[2] WBS-Typ'] == WBS_Typ1_list[0]]
                        band_WBS_Typ1_1=df_WBS_Typ1_1['[2] NE-Typ-Detail'].to_list()
                        print("WBS Type is is Missing")
                        Error_14 ="WBS Type Missing: " + str(band_WBS_Typ1_1[0])
                        WBS_type.append(Error_15)


                elif length_WBS_Typ1_list>1:
                    for d2 in range(length_WBS_Typ1_list):
                        if (str(WBS_Typ1_list[d2]).casefold())==("AuC".casefold()):
                            print("\nOK:- Correct mapping for WBS Type")
                            Error_15 =""
                            continue
                        elif (str(WBS_Typ1_list[d2]).casefold())!=("AuC".casefold()) and WBS_Typ1_list[d2]!= 'nan':
                            print("WBS Type is wrong")
                            df_WBS_Typ1=df31.loc[df31['[2] WBS-Typ'] == WBS_Typ1_list[d2]]
                            WBS_Typ1_Band_list=df_WBS_Typ1['[2] NE-Typ-Detail'].to_list()
                            band15=' - '.join([str(v2) for v2 in WBS_Typ1_Band_list])
                            Error_15 ="WBS type Wrong: " + band15
                            WBS_type.append(Error_15)
                        elif (str(WBS_Typ1_list[d2]).casefold())!=("AuC".casefold()) and WBS_Typ1_list[d2]== 'nan':
                            print("WBS Type is Missing")
                            df_WBS_Typ1=df31.loc[df31['[2] WBS-Typ'] == WBS_Typ1_list[d2]]
                            WBS_Typ1_Band_list=df_WBS_Typ1['[2] NE-Typ-Detail'].to_list()
                            band15=' - '.join([str(v2) for v2 in WBS_Typ1_Band_list])
                            Error_15 ="WBS Missing: " + band15
                            WBS_type.append(Error_15) 

                combined_15=' And '.join([str(z9) for z9 in WBS_type])
                Error_15=combined_15
                print(Error_15)
                
#                 --------------------check for missing or wrong StoB_Art-----------------
                
                df_third['Stob_combined']=df_third['[2] StoB_Art']+df_third['[2] NE-Typ-Detail']
                art_typ="Klassisch ohne Materialdämpfung"
                band_list=[]
                band_lists=[]
                for m in df_third['Stob_combined']:
                #     print(m)
                    if m.startswith(art_typ):
                        continue
                        #print("\nOK:- Correct mapping for Stob_art")
                    elif m.startswith(art_typ)==False and m.startswith("nan")==False:
                        #print("Incorrect mapping for Stob_art")
                #         print(m)
                        df_nan=df_third.loc[df_third['Stob_combined'] == m]
                #         print(df_nan)
                        band_list.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                    elif m.startswith("nan"):
                        #print("Stob_art type missing")
                        df_nan=df_third.loc[df_third['Stob_combined'] == m]
                        band_lists.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                #         print(band_lists)
                # band_str=' - '.join(band_list)
                # print(band_list)
                # print(band_lists)
                # print(band_str)

                Error19A="Stob_art Type Incorrect: " + ' - '.join(band_list)
                Error19B="Stob_art Type Missing: " + ' - '.join(band_lists)
                if len(band_list)==0 and len(band_lists)>0:
                    Error_19=Error19B
                elif len(band_list)==0 and len(band_lists)==0:
                    Error_19=''
                elif len(band_list)>0 and len(band_lists)==0:
                    Error_19=Error19A
                else:
                    Error_19=Error19A+' And '+Error19B
                print(Error_19)
                
#                 --------------------check for missing or wrong Site_Sharing_Adapter ------------
                
                
                df_third['SSA_combined']=df_third['[2] Site_Sharing_Adapter']+df_third['[2] NE-Typ-Detail']
                df_20=df_third.loc[df_third['[2] Status'] == "offen"]
                df_Lte=df_20.loc[df_20['[2] NE-Typ-Detail'].str.startswith('LTE')==True]
                df_GSM=df_20.loc[df_20['[2] NE-Typ-Detail'].str.startswith('GSM')==True]
                df_NR=df_20.loc[df_20['[2] NE-Typ-Detail'].str.startswith('NR')==True]
                Incorrect_bands=[]
                Missing_bands=[]
                ssa='ohne SSA'
                for d in df_Lte['SSA_combined']:
                    if d.startswith(ssa):
                #         print('All Good in LTE')
                        continue
                    elif d.startswith('nan'):
                        df_nan=df_Lte.loc[df_Lte['SSA_combined'] == d]
                        Missing_bands.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                    elif d.startswith('nan')==False and d.startswith(ssa)==False:
                        df_nan=df_Lte.loc[df_Lte['SSA_combined'] == d]
                        Incorrect_bands.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                for d in df_GSM['SSA_combined']:
                    if d.startswith(ssa):
                #         print('All Good in GSM')
                        continue
                    elif d.startswith('nan'):
                        df_nan=df_GSM.loc[df_GSM['SSA_combined'] == d]
                        Missing_bands.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                    elif d.startswith('nan')==False and d.startswith(ssa)==False:
                        df_nan=df_GSM.loc[df_GSM['SSA_combined'] == d]
                        Incorrect_bands.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
                for d in df_NR['SSA_combined']:
                    if d.startswith('nan'):
                #         print('All good in NR')
                        continue
                    else:
                        df_nan=df_NR.loc[df_NR['SSA_combined'] == d]
                        Incorrect_bands.append(df_nan['[2] NE-Typ-Detail'].to_string(index=False))
#                 print(Incorrect_bands)
#                 print(Missing_bands)
                Error20A="Site_Sharing_Adapter Incorrect: " + ' - '.join(Incorrect_bands)
                Error20B="Site_Sharing_Adapter Missing: " + ' - '.join(Missing_bands)
                if len(Incorrect_bands)==0 and len(Missing_bands)>0:
                    Error_20=Error20B
                elif len(Incorrect_bands)>0 and len(Missing_bands)==0:
                    Error_20=Error20A
                elif len(Incorrect_bands)==0 and len(Missing_bands)==0:
                    Error_20=''
                else:
                    Error_20=Error20A+' And '+Error20B
                print(Error_20)



                final_dict={'Ticket Number' : Tkt_list[i],'Loc Identifier':site,'Er1_Ticket missing ownership': Error_1,
                            'Er2_Status (In process or other)': Error_2, 'Er3_Ticket missing infra': Error_3,
                            'Er4_Ticket with wrong infra': Error_4,'Er5_Infra missing Inception comment': Error_5,
                            'Er6_Infra type is wrong (Neubau)': Error_6, 
                            'Er7_Check if site has multiple Infras with Inception': Error_7,
                            'Er8_There is empty infra from Marius S. and we didnt use it': Error_8,
                            'Er9_Check if there are multiple Fun NEU for the same layer': Error_9,
                            'Er10_Measures under infra missing Ticket comment or ticket number is wrong': Error_10,
                            'Er11_Check if same infra has multiple projects from same BO user': Error_11,
                            'Er12_Check for attribute errors - FUN_SGC_Partner': Error_12,
                            'Er13_Check for attribute errors - Zieltermin_Soll': Error_13,
                            'Er14_Check for missing WBS or wrong prefix': Error_14,'Er15_Check for WBS AUC or FA': Error_15,
                            'Er16_Number of MS advanced': Error_16,'Er17_Bands Present': Error_17,'Er18_Open measures': Error_18,
                            'Er19_check for missing or wrong StoB_Art': Error_19,
                            'Er20_check for missing or wrong Site_Sharing_Adapter': Error_20,'Error Summary':''}
                df_output = df_output.append(final_dict, ignore_index = True)
                
            except KeyError:
                continue
            except IndexError:
                continue
            except ValueError:
                continue

        print(df_output)
        time.sleep(2)

        ##

        cols = ['Er1_Ticket missing ownership','Er2_Status (In process or other)','Er3_Ticket missing infra','Er4_Ticket with wrong infra','Er5_Infra missing Inception comment', 'Er6_Infra type is wrong (Neubau)', 'Er7_Check if site has multiple Infras with Inception','Er9_Check if there are multiple Fun NEU for the same layer','Er10_Measures under infra missing Ticket comment or ticket number is wrong','Er11_Check if same infra has multiple projects from same BO user','Er12_Check for attribute errors - FUN_SGC_Partner','Er13_Check for attribute errors - Zieltermin_Soll','Er14_Check for missing WBS or wrong prefix','Er14_Check for missing WBS or wrong prefix','Er15_Check for WBS AUC or FA','Er19_check for missing or wrong StoB_Art','Er20_check for missing or wrong Site_Sharing_Adapter']

        #df_output['Error Summary'] = df_output[['Er8_There is empty infra from Marius S. and we didnt use it', 'Er9_Check if there are multiple Fun NEU for the same layer']].apply(lambda x: '/'.join(x.dropna().astype(str)),axis=1)

        df_output = df_output.replace(r'^\s*$', np.NaN, regex=True)

        df_output['Error Summary'] = df_output[cols].apply(lambda x: '\n'.join(x.dropna()), axis=1)

        writer = pd.ExcelWriter(OUTPATH + "output.xlsx", engine='xlsxwriter')
        df_output.to_excel(writer,sheet_name = "Op", index=False)

        workbook=writer.book
        worksheet = writer.sheets['Op']

        for column in df_output:
            column_width = max(df_output[column].astype(str).map(len).max(), len(column))
            col_idx = df_output.columns.get_loc(column)
            writer.sheets['Op'].set_column(col_idx, col_idx, column_width)
            
        
        col_idx = df_output.columns.get_loc('Error Summary')
        writer.sheets['Op'].set_column(col_idx, col_idx, 70)
        
        col_idx = df_output.columns.get_loc('Er18_Open measures')
        writer.sheets['Op'].set_column(col_idx, col_idx, 70)


        border_fmt = workbook.add_format({'bottom':5, 'top':5, 'left':5, 'right':5})
        worksheet.conditional_format(xlsxwriter.utility.xl_range(0, 0, len(df_output), (len(df_output.columns)-1)), {'type': 'no_errors', 'format': border_fmt})
        writer.save()

    #except KeyError:
        #sys.exit('Seems like input sheet is empty, please check and re-run..')

    except PermissionError:
            sys.exit('Seems like input_file is open in your system, Please close & re-run the BOT..')

    except TypeError:
            sys.exit('Please verify all the input workbooks & re-run & contact developer for further support...')

    except NameError:
            sys.exit('Please co-ordinate with the developer to figure out the issue...')
            
    except AttributeError:
            sys.exit('Please co-ordinate with the developer to figure out the issue...')

    #except IndexError:
        #sys.exit('Please check the TTWOS sheet & check for small alphabtes in Loc Identifier, example (xxxxs or xxxxf) for every ticket in input list & rectify & re-run...')

    #writer.close()
##
doProcess()


# In[ ]:





# In[ ]:




