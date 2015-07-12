import requests
import os
import pandas as pd
import zipfile
import numpy as np
import re
import sys
import pickle

data_from_date="2009-04-01"

nse_op_path=os.path.join("NSE","unadjusted_data","all_data","op.data")
nse_hi_path=os.path.join("NSE","unadjusted_data","all_data","hi.data")
nse_lo_path=os.path.join("NSE","unadjusted_data","all_data","lo.data")
nse_cl_path=os.path.join("NSE","unadjusted_data","all_data","cl.data")
nse_vl_path=os.path.join("NSE","unadjusted_data","all_data","vl.data")

bse_op_path=os.path.join("BSE","unadjusted_data","all_data","op.data")
bse_hi_path=os.path.join("BSE","unadjusted_data","all_data","hi.data")
bse_lo_path=os.path.join("BSE","unadjusted_data","all_data","lo.data")
bse_cl_path=os.path.join("BSE","unadjusted_data","all_data","cl.data")
bse_vl_path=os.path.join("BSE","unadjusted_data","all_data","vl.data")

nse_ad_op_path=os.path.join("NSE","adjusted_data","all_data","op.data")
nse_ad_hi_path=os.path.join("NSE","adjusted_data","all_data","hi.data")
nse_ad_lo_path=os.path.join("NSE","adjusted_data","all_data","lo.data")
nse_ad_cl_path=os.path.join("NSE","adjusted_data","all_data","cl.data")
nse_ad_vl_path=os.path.join("NSE","adjusted_data","all_data","vl.data")

bse_ad_op_path=os.path.join("BSE","adjusted_data","all_data","op.data")
bse_ad_hi_path=os.path.join("BSE","adjusted_data","all_data","hi.data")
bse_ad_lo_path=os.path.join("BSE","adjusted_data","all_data","lo.data")
bse_ad_cl_path=os.path.join("BSE","adjusted_data","all_data","cl.data")
bse_ad_vl_path=os.path.join("BSE","adjusted_data","all_data","vl.data")

comp_scrip_csv=pd.read_csv("ListOfScrips.csv")
# checking for scrip shape
def check_for_new_companies():
    last_shape_path="scripshape.data"
    if os.path.exists(last_shape_path):
        with open(last_shape_path,'rb') as f:
            last_shape=pickle.load(f)
        if comp_scrip_csv.shape[0]<last_shape[0]:
            #print "Less Companies today"
            #print "Not Possible"
            sys.exit()
        elif comp_scrip_csv.shape[0]>last_shape[0]:
            #print comp_scrip_csv.shape[0]-last_shape[0], "Companies added"
            last_shape=comp_scrip_csv.shape
            with open(last_shape_path,'wb') as f:
                pickle.dump(last_shape,f)
        else:
            #print "No new companies added"
    else:
        with open(last_shape_path,'wb') as f:
            pickle.dump(comp_scrip_csv.shape,f)

check_for_new_companies()
code_2_symbol=dict(zip(comp_scrip_csv['Security Code'],comp_scrip_csv['Security Id']))
symbol_2_name=dict(zip(comp_scrip_csv['Security Id'],comp_scrip_csv['Security Name']))
code_2_name=dict(zip(comp_scrip_csv['Security Code'],comp_scrip_csv['Security Name']))

nse_error=[]
bse_error=[]

if os.path.exists(nse_op_path) and os.path.exists(nse_hi_path) and os.path.exists(nse_lo_path) and \
        os.path.exists(nse_cl_path) and os.path.exists(nse_vl_path):
    op_nse=pd.read_pickle(nse_op_path)
    hi_nse=pd.read_pickle(nse_hi_path)
    lo_nse=pd.read_pickle(nse_lo_path)
    cl_nse=pd.read_pickle(nse_cl_path)
    vl_nse=pd.read_pickle(nse_vl_path)
else:
    op_nse=pd.DataFrame()
    hi_nse=pd.DataFrame()
    lo_nse=pd.DataFrame()
    cl_nse=pd.DataFrame()
    vl_nse=pd.DataFrame()

if os.path.exists(bse_op_path) and os.path.exists(bse_hi_path) and os.path.exists(bse_lo_path) and \
        os.path.exists(bse_cl_path) and os.path.exists(bse_vl_path):
    op_bse=pd.read_pickle(bse_op_path)
    hi_bse=pd.read_pickle(bse_hi_path)
    lo_bse=pd.read_pickle(bse_lo_path)
    cl_bse=pd.read_pickle(bse_cl_path)
    vl_bse=pd.read_pickle(bse_vl_path)
else:
    op_bse=pd.DataFrame()
    hi_bse=pd.DataFrame()
    lo_bse=pd.DataFrame()
    cl_bse=pd.DataFrame()
    vl_bse=pd.DataFrame()

def gen_today_date(starting_from):
    start_date=starting_from
    now=pd.datetime.now()
    updation_time=pd.datetime(now.year,now.month,now.day,21)
    if now<updation_time:
        return pd.date_range(start_date,str(pd.datetime.now().date()))[:-1]
    elif now>=updation_time:
        return pd.date_range(start_date,str(pd.datetime.now().date()))

dates=gen_today_date(data_from_date)

def convert_to_nse_date(date):
    """ Takes pandas datetime and returns NSE url extension for bhavcopies """

    months=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    month_dict=dict(zip(range(1,13),months))
    sep='/'
    t_str="cm"+"%02d"%date.day+month_dict[date.month]+str(date.year)+"bhav.csv.zip"
    attach=sep.join([str(date.year),month_dict[date.month],t_str])

    rest_url="http://www.nseindia.com/content/historical/EQUITIES/"

    data_url=rest_url+attach

    f_name="%02d"%date.day+month_dict[date.month]+str(date.year)+".zip"

    return data_url,f_name

def download_nse_data():
    success=0
    error=[]
    for items in dates:
        try:
            url,name=convert_to_nse_date(items)
            if not os.path.exists(os.path.join("NSE","unadjusted_zip",name)):
                r=requests.get(url)
                f=open(os.path.join("NSE","unadjusted_zip",name),'wb')
                f.write(r.content)
                f.close()
                success=success+1
                #print success,"downloaded"
        except:
            error.append(name)
            #print "Error: ",name
    #print "NSE: ",len(dates),success,len(error)

def convert_to_bse_date(date):
    months=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    month_dict=dict(zip(range(1,13),months))
    base="http://www.bseindia.com/download/BhavCopy/Equity/eq{0}_csv.zip"
    temp=str(date)
    insrt=temp[8:10]+temp[5:7]+temp[2:4]
    data_url=base.format(insrt)
    f_name="%02d"%date.day+month_dict[date.month]+str(date.year)+".zip"
    return data_url,f_name

def download_bse_data():
    success=0
    error=[]
    for items in dates:
        try:
            url,name=convert_to_bse_date(items)
            if not os.path.exists(os.path.join("BSE","unadjusted_zip",name)):
                r=requests.get(url)
                f=open(os.path.join("BSE","unadjusted_zip",name),'wb')
                f.write(r.content)
                f.close()
                success=success+1
                #print success,"downloaded"
        except:
            error.append(name)
            #print "Error: ",name
    #print "BSE: ",len(dates),success,len(error)

def ToDateTime(day_string):
    name_month=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    num_months=["%02d"%i for i in xrange(1,13)]
    month_to_num_dict=dict(zip(name_month,num_months))
    day=day_string[:2]
    month=month_to_num_dict[day_string[2:5]]
    year=day_string[5:9]
    return pd.to_datetime("%s-%s-%s"%(year,month,day))

def wrangle(key,value,frame,day):
    dct=dict(zip(key,value))
    temp=pd.DataFrame(dct,index=[ToDateTime(day)])
    frame=pd.concat([frame,temp],axis=0)
    return frame

def get_required_fields_dict(mydict,data):
    converted=[]
    use=[]
    for i in xrange(len(data)):
        try:
            converted.append(mydict[data.ix[i]])
            use.append(i)
        except:
            pass
    return converted,use

def unprocessed_zip_list(path):
    temp_file_names=os.listdir(path)
    if "NSE" in path:
        if op_nse.empty:
            #print "All files: ",len(temp_file_names)
            latest_update_date=pd.to_datetime(data_from_date)
        else:
            latest_update_date=max(op_nse.index)
        temp_dates=pd.date_range(str(latest_update_date.date()),str(pd.datetime.now().date()))[1:]
        selection_list=[]
        for items in temp_dates:
            unused_url,f_name=convert_to_nse_date(items)
            selection_list.append(f_name)
        final_list=[]
        for filenames in selection_list:
            if filenames in temp_file_names:
                final_list.append(filenames)
        #print final_list
        return final_list
    elif "BSE" in path:
        if op_bse.empty:
            #print "All files: ",len(temp_file_names)
            latest_update_date=pd.to_datetime(data_from_date)
        else:
            latest_update_date=max(op_bse.index)
        temp_dates=pd.date_range(str(latest_update_date.date()),str(pd.datetime.now().date()))[1:]
        selection_list=[]
        for items in temp_dates:
            unused_url,f_name=convert_to_bse_date(items)
            selection_list.append(f_name)
        final_list=[]
        for filenames in selection_list:
            if filenames in temp_file_names:
                final_list.append(filenames)
        #print final_list
        return final_list


def process_nse_zip():
    global op_nse,hi_nse,lo_nse,cl_nse,vl_nse
    count=0
    success=0
    file_names=unprocessed_zip_list(os.path.join("NSE","unadjusted_zip"))
    for files in file_names:
        try:
            f=open(os.path.join("NSE","unadjusted_zip",files),'rb')
            z=zipfile.ZipFile(f)
            d=pd.read_csv(z.open(z.filelist[0]))
            f.close()
            op_nse=wrangle(np.array(d['SYMBOL'],dtype=str),d['OPEN'],op_nse,files)
            hi_nse=wrangle(np.array(d['SYMBOL'],dtype=str),d['HIGH'],hi_nse,files)
            lo_nse=wrangle(np.array(d['SYMBOL'],dtype=str),d['LOW'],lo_nse,files)
            cl_nse=wrangle(np.array(d['SYMBOL'],dtype=str),d['CLOSE'],cl_nse,files)
            vl_nse=wrangle(np.array(d['SYMBOL'],dtype=str),d['TOTTRDQTY'],vl_nse,files)
            success=success+1
        except:
            nse_error.append(files)
            count=count+1
    op_nse.to_pickle(nse_op_path)
    hi_nse.to_pickle(nse_hi_path)
    lo_nse.to_pickle(nse_lo_path)
    cl_nse.to_pickle(nse_cl_path)
    vl_nse.to_pickle(nse_vl_path)
    #print "NSE successful:",success
    #print "NSE fialed:",count

def process_bse_zip():
    global op_bse,hi_bse,lo_bse,cl_bse,vl_bse,splits
    count=0
    success=0
    file_names=unprocessed_zip_list(os.path.join("BSE","unadjusted_zip"))
    for files in file_names:
        try:
            f=open(os.path.join("BSE","unadjusted_zip",files),'rb')
            z=zipfile.ZipFile(f)
            d=pd.read_csv(z.open(z.filelist[0]))
            f.close()
            symbols,useful=get_required_fields_dict(code_2_symbol,d['SC_CODE'])
            op_bse=wrangle(symbols,d['OPEN'].ix[useful],op_bse,files)
            hi_bse=wrangle(symbols,d['HIGH'].ix[useful],hi_bse,files)
            lo_bse=wrangle(symbols,d['LOW'].ix[useful],lo_bse,files)
            cl_bse=wrangle(symbols,d['CLOSE'].ix[useful],cl_bse,files)
            vl_bse=wrangle(symbols,d['NO_OF_SHRS'].ix[useful],vl_bse,files)
            success=success+1
        except:
            bse_error.append(files)
            count=count+1
    op_bse.to_pickle(bse_op_path)
    hi_bse.to_pickle(bse_hi_path)
    lo_bse.to_pickle(bse_lo_path)
    cl_bse.to_pickle(bse_cl_path)
    vl_bse.to_pickle(bse_vl_path)
    #print "BSE successful:",success
    #print "BSE fialed:",count

def process_splits():
    def using_re(myarray):
        old_new=[]
        for items in myarray:
            old_new.append(re.findall('\d+',items))
        for i in xrange(len(old_new)):
            if old_new[i]==[]:
                old_new[i]=[1,1]
        return np.array(old_new,dtype=float)
    def gen_split_dates(mydatearray):
        mydates=[]
        for items in mydatearray:
            #print items
            params=items.upper().split(' ')
            date_string="%02d%s%s"%(int(params[0]),params[1],params[2])
            mydates.append(ToDateTime(date_string))
        return mydates
    split_data=pd.read_csv("Corporate_Actions.csv")
    symbols,useful=get_required_fields_dict(code_2_symbol,split_data['Security Code'])
    old_new_values=using_re(split_data['Purpose'].ix[useful])
    split_dates=gen_split_dates(split_data['Ex Date'].ix[useful])
    #print len(symbols),len(old_new_values),len(split_dates)
    #print symbols[100],old_new_values[100],split_dates[100]
    #print old_new_values.shape

    ops_nse=op_nse.copy()
    his_nse=hi_nse.copy()
    los_nse=lo_nse.copy()
    cls_nse=cl_nse.copy()
    vls_nse=vl_nse.copy()

    ops_bse=op_bse.copy()
    his_bse=hi_bse.copy()
    los_bse=lo_bse.copy()
    cls_bse=cl_bse.copy()
    vls_bse=vl_bse.copy()

    for i in xrange(len(symbols)):
        if symbols[i] in op_nse.columns:
            ops_nse[symbols[i]].ix[ops_nse.index<split_dates[i]]=ops_nse[symbols[i]].ix[ops_nse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            his_nse[symbols[i]].ix[his_nse.index<split_dates[i]]=his_nse[symbols[i]].ix[his_nse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            los_nse[symbols[i]].ix[los_nse.index<split_dates[i]]=los_nse[symbols[i]].ix[los_nse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            cls_nse[symbols[i]].ix[cls_nse.index<split_dates[i]]=cls_nse[symbols[i]].ix[cls_nse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])

        if symbols[i] in op_bse.columns:
            ops_bse[symbols[i]].ix[ops_bse.index<split_dates[i]]=ops_bse[symbols[i]].ix[ops_bse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            his_bse[symbols[i]].ix[his_bse.index<split_dates[i]]=his_bse[symbols[i]].ix[his_bse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            los_bse[symbols[i]].ix[los_bse.index<split_dates[i]]=los_bse[symbols[i]].ix[los_bse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])
            cls_bse[symbols[i]].ix[cls_bse.index<split_dates[i]]=cls_bse[symbols[i]].ix[cls_bse.index<split_dates[i]]/(old_new_values[i,0]/old_new_values[i,1])

    ops_nse.to_pickle(nse_ad_op_path)
    his_nse.to_pickle(nse_ad_hi_path)
    los_nse.to_pickle(nse_ad_lo_path)
    cls_nse.to_pickle(nse_ad_cl_path)
    vls_nse.to_pickle(nse_ad_vl_path)

    ops_bse.to_pickle(bse_ad_op_path)
    his_bse.to_pickle(bse_ad_hi_path)
    los_bse.to_pickle(bse_ad_lo_path)
    cls_bse.to_pickle(bse_ad_cl_path)
    vls_bse.to_pickle(bse_ad_vl_path)


if __name__=='__main__':
    download_bse_data()
    download_nse_data()
    process_bse_zip()
    process_nse_zip()
    #process_splits()