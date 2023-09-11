from dotenv import load_dotenv
import os
import pandas as pd
import json
# import googlemaps
import numpy as np
from sqlalchemy import create_engine, text, Table, Column, Integer, JSON, String, DateTime, MetaData
import boto3
from botocore.config import Config
from pathlib import Path
import logging
import requests

BASE_DIR = Path(__file__).parent.parent.absolute()

def get_env():
    load_dotenv()
    ## Getting elastic creds
    cloud_id = os.environ.get("CLOUD_ID")
    api_id = os.environ.get("API_ID")
    api_key = os.environ.get("API_KEY")
    index_name = os.environ.get("INDEX_NAME")
    
    ## Getting AWS creds
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_sceret = os.environ.get("AWS_SECRET_KEY")
    
    ## bucket details
    bucket = os.environ.get("AWS_bucket_name")
    html_dir = os.environ.get("html_dir")
    json_dir = os.environ.get("json_dir")
    
    ## postgres db details
    db_string = os.environ.get("DB_STRING")
    cnr_fir_idx = os.environ.get("cnr_fir_idx")
    cnr_fir_pincode = os.environ.get("cnr_fir_pincode")
    court_pincode = os.environ.get("court_pincode")
    cnr_request_queue = os.environ.get("cnr_request_queue")
    cnr_request_status = os.environ.get("cnr_request_status")
    cnr_request_report = os.environ.get("cnr_request_report")
    cnr_request_result = os.environ.get("cnr_request_result")

    ##notify api details
    notify_url = os.environ.get("notify_url")
    notify_token = os.environ.get("notify_token")
    
    return {
        "cloud_id" : cloud_id, 
        "api_id" : api_id,
        "api_key" : api_key,
        "index_name": index_name,
        "aws_access_key": aws_access_key,
        "aws_secret": aws_sceret,
        "bucket": bucket,
        "html_dir": html_dir,
        "json_dir": json_dir,
        "db_string": db_string,
        "cnr_fir_idx": cnr_fir_idx,
        "cnr_fir_pincode": cnr_fir_pincode,
        "court_pincode": court_pincode,
        "cnr_request_queue": cnr_request_queue,
        "cnr_request_status": cnr_request_status,
        "cnr_request_report": cnr_request_report, 
        "cnr_request_result": cnr_request_result,
        "notify_url": notify_url,
        "notify_token": notify_token
            }
    
    

def get_geocode(args):
    gmaps, idx, text = args
    geocode_result = gmaps.geocode(text)
    geocode = []
    
    for gc in geocode_result:
        try:
            for ac in gc['address_components']:
                if 'locality' in ac['types']:
                    locality = ac['long_name']
                    # locality_short = ac['short_name']
                if 'administrative_area_level_3' in ac['types']:
                    admin_level_3 = ac['long_name']
                if 'administrative_area_level_1' in ac['types']:
                    admin_level_1 = ac['long_name']
                if 'postal_code' in ac['types']:
                    postal_code = ac['long_name']
                    geocode.append([idx, admin_level_1, admin_level_3, locality, postal_code])
        except:
            pass
    return pd.DataFrame(geocode, columns=['idx', 
                                          'admin_level_1',
                                          'admin_level_3',
                                          'locality',
                                          'postal_code'])

def clean_corrupt(text):
    to_find = "fir details police station"
    loc = text.find(to_find)
    if loc > 0:
        return text[loc + len(to_find):].strip()
    else:
        return text

def haversine_vectorize(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    newlon = lon2 - lon1
    newlat = lat2 - lat1
    haver_formula = (
        np.sin(newlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(newlon / 2.0) ** 2
    )
    dist = 2 * np.arcsin(np.sqrt(haver_formula))
    km = 6367 * dist
    return km

def get_case(env):
    db_string = env['db_string']
    engine = create_engine(db_string)
    query_idx = f"""delete from {env['cnr_request_status']} cr using 
                      (select * from {env['cnr_request_status']} where status = 'in_progress' limit 1
                      for update skip locked) crs
                      where cr.idx = crs.idx returning crs.idx"""
                    
    with engine.connect() as connect:
        idx = connect.execute(text(query_idx)).fetchall()
        if len(idx) > 0:
            idx = idx[0][0]
            query_details = f"""select * from {env['cnr_request_queue']} where idx = '{idx}'"""
            case_details = connect.execute(text(query_details)).fetchall()
            connect.commit()
        else:
            case_details = []
    return case_details

def get_court_names(env, district, state):
    db_string = env['db_string']
    engine = create_engine(db_string)
    court_district_query = f"""select distinct court_name, state_code_num from court_data cd 
                            WHERE state ilike '{state}' and district ilike '{district}' 
                            limit 15;"""
    
    court_state_query = f"""select distinct court_name, state_code_num from court_data cd 
                            WHERE state ilike '{state}'
                            limit 15;"""


    with engine.connect() as connect:
        try:
            #first priority on to query court names based on state and district
            court_names_district = connect.execute(text(court_district_query)).fetchall()
            if len(court_names_district) > 0:
                court_names = [f"{court_name}" for court_name, st in court_names_district]
                connect.commit()
            #if no courts based on district(or district is empty), then query court names based on state
            elif len(court_names_district) == 0:
                court_names_state = connect.execute(text(court_state_query)).fetchall()
                if len(court_names_state) > 0:
                    court_names = [f"{court_name}" for court_name, st in court_names_state]
                    connect.commit()
                else:
                    court_names = []
        except:
            court_names = []

    return court_names

def get_temporary_s3_url(s3_client, bucket_name, object_key, expiration=600000):
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration
        )
    except Exception as e:
        logging.info(f"Error generating temporary URL {e}")
        return None
    return response

def list_objects_with_key(s3_client, bucket_name, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    except Exception as e:
        print(f"No order copy for {prefix}")
        return None
    objects = []
    if "Contents" in response:
        for obj in response['Contents']:
            objects.append(obj['Key'])
    if len(objects) > 0:
        return objects[0]
    return None
    
def mark_green(env, df):
    db_string = env['db_string']
    engine = create_engine(db_string)
    query_ipc = f"""SELECT type, code FROM court_ipc_green;"""
    ipc_cols = ['TYPE', 'CODE']
    with engine.connect() as connect:
        res = connect.execute(text(query_ipc)).fetchall()
    ipc_dct = [dict(zip(ipc_cols, r)) for r in res]
    green_report_df = pd.DataFrame(ipc_dct)
    # ipc_sheet_path = "IPC_reporting_list.xlsx"
    # green_report_df = pd.read_excel(ipc_sheet_path, sheet_name='GREEN')
    failed_cnr=[]

    for index,val in df.iterrows():
        try:
            if val['act'].lower().strip() in green_report_df['TYPE'].str.lower().str.strip().unique():
                df_new=green_report_df.loc[(green_report_df['TYPE'].str.lower().str.strip()==val['act'].lower().strip()),:]
                try:
                    for ev in val["section"]:
                        try:
                            if int(ev) in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'green'
                            elif ev in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'green'
                        except:
                            if ev in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'green'
                except:
                    failed_cnr.append(val['cnr'])
        except:
            failed_cnr.append(val['cnr'])
    return df, failed_cnr

def mark_red(env, df):
    db_string = env['db_string']
    engine = create_engine(db_string)
    query_ipc = f"""SELECT type, code FROM court_ipc_red;"""
    ipc_cols = ['TYPE', 'CODE']
    with engine.connect() as connect:
        res = connect.execute(text(query_ipc)).fetchall()
    ipc_dct = [dict(zip(ipc_cols, r)) for r in res]
    red_report_df = pd.DataFrame(ipc_dct)
    # ipc_sheet_path = "IPC_reporting_list.xlsx"
    # red_report_df = pd.read_excel(ipc_sheet_path, sheet_name='RED')
    failed_cnr=[]

    for index,val in df.iterrows():
        try:
            if val['act'].lower().strip() in red_report_df['TYPE'].str.lower().str.strip().unique():
                df_new=red_report_df.loc[(red_report_df['TYPE'].str.lower().str.strip()==val['act'].lower().strip()),:]
                try:
                    for ev in val["section"]:
                        try:
                            if int(ev) in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'red'
                            elif ev in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'red'
                        except:
                            if ev in list(df_new['CODE']):
                                df.loc[index, "case_status"] = 'red'
                except:
                    failed_cnr.append(val['cnr'])
        except:
            failed_cnr.append(val['cnr'])

    return df, failed_cnr

def json_response_builder(args):
    
    emp_id, idx, name, full_address, state, district, case_result, case_status_code, court_names = args
    matched_cases = []
    
    response = {
        "status": case_status_code,
        "service_request_id": emp_id,
        "verify_id": idx,
        "name": name,
        "address": full_address,
        "report": case_result,
        "matched_case_details": matched_cases,
        "jurisdiction": {
            "state_name": state,
            "dist_name": district
        }
    }

    if court_names != []:
        response["jurisdiction"]["court_name"] = court_names
    
    return json.dumps(response)

def write_final_response(args):

    emp_id, idx, initiated_on, completed_on, case_result, case_status_code, response, report_table = args
    metadata = MetaData()
    result_table = Table(report_table, metadata, 
                            Column('idx', String, primary_key=True),
                            Column('emp_id', String),
                            Column('initiated_on', DateTime),
                            Column('completed_on', DateTime),
                            Column('case_status', String),
                            Column('status_code', Integer),
                            Column('report', JSON)
                            )
    query_response = result_table.insert().values(
                                        idx=idx, 
                                        emp_id=emp_id, 
                                        initiated_on=initiated_on,
                                        completed_on=completed_on,
                                        case_status=case_result, 
                                        status_code=case_status_code, 
                                        report=response)
    return query_response

def call_notify_api(env, verify_id):
    # Set the URL and headers for notify api
    url = env['notify_url']
    headers = {'Content-Type': 'application/json',
            'Authorization': env['notify_token']}
    data = {"ref_id": str(verify_id)}
    data = json.dumps(data)
    # Send the curl request to notify api
    try:
        response = requests.post(url, headers=headers, data=data)
        return response.status_code
    except Exception as e:
        print(f"Failed to send request to notify api, {e}")
        return 0