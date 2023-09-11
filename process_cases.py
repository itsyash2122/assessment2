import pandas as pd
from utils import get_env, get_case
from elastic_utils import QueryBuilder, get_search, process_response, get_act_section
from custom_exceptions import CaseStatusException
from sqlalchemy import create_engine, text, Table, Column, Integer, JSON, String, MetaData
import json
import asyncio
import boto3
from datetime import datetime
from utils import get_temporary_s3_url, list_objects_with_key
from utils import mark_green, mark_red, get_court_names, json_response_builder, write_final_response, call_notify_api
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.logger import logger as fastapi_logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    level=logging.INFO,
                    datefmt="%Y%m%d %H:%M:%S")
#
gunicorn_error_logger = logging.getLogger("gunicorn.error")
gunicorn_logger = logging.getLogger("gunicorn")
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.handlers = gunicorn_error_logger.handlers

fastapi_logger.handlers = gunicorn_error_logger.handlers
fastapi_logger.setLevel(logging.DEBUG)
fastapi_logger = logging.getLogger(__name__)

# class CustomException(Exception):
#     def __init__(self, message, status_code, case_status):
#         super().__init__(message)
#         self.status_code = status_code
#         self.case_status = case_status

app = FastAPI(title="crc worker",
              description="worker service to read the crc details from the postgres queue and process it accordingly")
scheduler = AsyncIOScheduler()


async def process_crc():
    fastapi_logger.info("Getting environment variable")
    env = get_env()
    fastapi_logger.info("Getting cases from the queue")
    case = get_case(env)
    
    if len(case) > 0:
        fastapi_logger.info("There are cases to be processed")
        case = case[0]
    else:
        fastapi_logger.info("There are no cases to be processed")
        return
    idx, emp_id, initiated_on, name, pincode, dob, father_name, state, district, full_address = case
    fastapi_logger.info(f"""Processing case with following details
                verify_id: {idx}
                emp_id: {emp_id}
                name: {name}
                father name : {father_name}
                pincode: {pincode}
                """)

    try:
        qb = QueryBuilder(env, name, father_name, pincode, state, district)
        qb.get_query_list()
        fastapi_logger.info("Getting result from elastic search")
        print("Getting result from elastic search")
        responses = await get_search(env, qb, size=500)
        fastapi_logger.info("Elastic search query completed")

        fastapi_logger.info("Processing elastic response")
        result = process_response(env, qb, responses)
        fastapi_logger.info("we do not have lat-lon for all police station and lot of time we dont have police station information")
        fastapi_logger.info("In above cases, we replace missing police station distance as twice the max distance")
        # check if results are empty
        if result.empty:
            raise CaseStatusException("No cases found", 201, "green")
        result.police_station_distance.fillna(result.police_station_distance.max()*2, inplace=True)
        result.court_distance.fillna(result.court_distance.max()*2, inplace=True)
        
        cols_to_keep = ['cnr', 'name', 'party_type', 'relative', 'relation_type', 'case_location', 
                    'case_state', 'case_district', 'case_court',
                    'case_stage', 'fir_police_station', 'act_section', 'order_exists',
                    'name_match', 'father_name_match' ,'percentage_father_in_name','father_in_name', 
                    'police_station_distance', 'court_distance','in_same_district', 'in_same_state','modified_name_match' , ]
        result = result[cols_to_keep]
        fastapi_logger.info("Filtering the data based on some logic, this is subject to change")
        max_name_match = result.name_match.max()
        if max_name_match > 70:
            result = result[result['name_match'] > 70]
            father_in_name_any = (result.father_in_name == 1).any()
            if father_in_name_any:
                result = result[result.father_in_name == 1]
            min_court_distance = result.court_distance.min()
            # min_police_station_distance = result.police_station_distance.min()
            if min_court_distance < 100:
                result = result[(result['court_distance'] < 100)]
            # else:
            #     result = pd.DataFrame(columns = result.columns)
        else:
            result = pd.DataFrame(columns = result.columns)

        # check if results are empty
        if result.empty:
            raise CaseStatusException("No cases found", 201, "green")

        # modify below to get the URLs
        fastapi_logger.info(f"Length of results from elastic search, {len(result)}")
        # fastapi_logger.info(f"CNRs found are", {result["cnr"]})
        fastapi_logger.info("Getting pre-signed URLs for case details and order copy")
        fastapi_logger.info("NOTE: pre-signed URLs are valid only for a week")
        session = boto3.Session(aws_access_key_id=env['aws_access_key'], aws_secret_access_key=env['aws_secret'], region_name ='ap-south-1')
        s3_client = session.client("s3", )
        bucket_name = env['bucket']
      
        result[ 'case_details_url'] = result['cnr'].apply(lambda x: 
            get_temporary_s3_url(s3_client, bucket_name, f'html_v1/{x}.html', 600000))
        result['order_copy_details'] = result['cnr'].apply(lambda x: 
            list_objects_with_key(s3_client,bucket_name, f"order_copy/{x}"))
        result['order_copy_url'] = result['order_copy_details'].apply(lambda x: 
            get_temporary_s3_url(s3_client, bucket_name, x, 600000) if x is not None else "")

        # Separating Act-Section:
        res = result.apply(lambda x: get_act_section(x['cnr'], x['case_details_url']), axis=1)
        #check if act-section were extracted (if blank return series)
        if all(r.empty for r in res):
            raise CaseStatusException("Act section not found", 204, "red")
        
        res_new= pd.concat(list(res))
        result=pd.merge(res_new,result, how='left', left_on='cnr', right_on='cnr')
        result.drop_duplicates(inplace=True)

        # marking cases green or red:
        result.reset_index(inplace=True, drop=True)
        result["section"] = result['section'].apply(lambda x: [s.strip() for s in x.split(',')] if(pd.notnull(x)) else x)
        result, green_failed = mark_green(env, result)
        result, red_failed = mark_red(env, result)
        #mark petitioner cases as green
        result.loc[result["party_type"] == 'petitioner', "case_status"] = 'green'
        #check if cases were marked or not
        if all(result["case_status"].isna()):
            raise CaseStatusException("Unable to mark any case", 205, "red")

        #TODO: add confidence score based on the match, presence of father's name, father name match, distance 
       # result['confidence_score']= (result['name_match'] + result['father_name_match'] + result['modified_name_match'] + result['percentage_father_in_name'] + result['in_same_district'] + result['in_same_state'] + 1/result['court_distance'])/7 
        result['confidence_score'] = (
            ((result['name_match'].fillna(0)/100) + (result['father_name_match'].fillna(0)/100) + (result['modified_name_match'].fillna(0)/100) +
    (result['percentage_father_in_name'].fillna(0)/100) + result['in_same_district'].fillna(0) + result['in_same_state'].fillna(0) + 1 / result['court_distance'].fillna(1))/ 7)


        #TODO: of the court and police station from the candidate
        result.drop(['police_station_distance', 'order_copy_details'], axis=1, inplace=True)
        result.sort_values("name_match", ascending=False, inplace=True)
        
        # result_json = json.dumps(result.to_dict(orient="list"))
        result_json = result.to_json(orient="records")
        query_status = f"insert into {env['cnr_request_status']} (idx, emp_id, status) values ('{idx}', '{emp_id}', 'completed')"
        query_result = f"insert into {env['cnr_request_result']} (idx, result) values ('{idx}', '{result_json}')"

        #inserting case status to a new table
        case_result = 'red' if 'red' in result["case_status"].values else 'green'
        case_status_code = 202 if case_result == 'red' else 200

        #get court names for green case
        if case_result == 'green':
            # cnr_part_list = tuple(set(result["cnr"].str[:6].to_list()))
            court_names = get_court_names(env, district=district, state=state)
        else:
            court_names = []

        #build final json reponse
        args = emp_id, idx, name, full_address, state, district, case_result, case_status_code, court_names
        response = json_response_builder(args)

        completed_on = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        #build write query to store the response
        report_table = env['cnr_request_report']
        args = emp_id, idx, initiated_on, completed_on, case_result, case_status_code, response, report_table
        query_case_result = write_final_response(args)

        ## Trying new method to insert the data
        metadata = MetaData()
        #TODO: add time stamp to the result table, req for re-generating the URLs
        result_table = Table(env['cnr_request_result'], metadata, 
                             Column('idx', String, primary_key=True),
                             Column('result', JSON)
                             )
        query_result = result_table.insert().values(idx=idx, result=result_json)
        #TODO: take the POST endpoint from the env, and update the status for engineering team
        #TODO: above will be based on the environment
        fastapi_logger.info("Inserting result and updating status")
        if not result.empty:
            fastapi_logger.info("Result is not empty")
            engine = create_engine(env['db_string'])
            with engine.connect() as connect:
                connect.execute(query_result)
                connect.execute(text(query_status))
                connect.execute(query_case_result)
                connect.commit()
            notify_status_code = call_notify_api(env, idx)
            fastapi_logger.info(f"Notify api status code: {notify_status_code}")
        else:
            fastapi_logger.info("Result is empty")
            engine = create_engine(env['db_string'])
            with engine.connect() as connect:
                connect.execute(text(query_status))
                connect.commit()
    except CaseStatusException as e:
        fastapi_logger.info(f"Case status exception raised for verify id {idx}")
        fastapi_logger.info(f"status_code: {e.status_code} and case_status: {e.case_status}")
        engine = create_engine(env['db_string'])
        #get court names for green case
        if str(e.case_status) == 'green':
            court_names = get_court_names(env, district=district, state=state)
        else:
            court_names = []
        #build final json reponse
        args = emp_id, idx, name, full_address, state, district, e.case_status, e.status_code, court_names
        response = json_response_builder(args)

        completed_on = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        #build write query to store the response
        report_table = env['cnr_request_report']
        args = emp_id, idx, initiated_on, completed_on, e.case_status, e.status_code, response, report_table
        query_case_result = write_final_response(args)
        query_status = f"insert into {env['cnr_request_status']} (idx, emp_id, status) values ('{idx}', '{emp_id}', 'completed')"
        with engine.connect() as connect:
            connect.execute(query_case_result)
            connect.execute(text(query_status))
            connect.commit()
        notify_status_code = call_notify_api(env, idx)
        fastapi_logger.info(f"Notify api status code: {notify_status_code}")
    except Exception as e:
        fastapi_logger.info(f"Failed for verify id: {idx}")
        fastapi_logger.info(f"Failed reason: {e}")
        engine = create_engine(env['db_string'])
        query_status = f"insert into {env['cnr_request_status']} (idx, emp_id, status) values ('{idx}', '{emp_id}', 'failed')"
        with engine.connect() as connect:
            connect.execute(text(query_status))
            connect.commit()
    fastapi_logger.info(f"Completed for verify id {idx}")

# @aiocron.crontab('*/1 * * * *')
# async def cronjob_process():
#     loop = asyncio.get_event_loop()
#     coroutine = process_crc()
#     loop.run_until_complete(coroutine)
#     # asyncio.create_task(process_crc)

scheduler.add_job(process_crc, "interval", seconds=10, max_instances=3)
scheduler.start()

@app.get("/")
async def index():
    return JSONResponse(content={"message": "crc worker is up and running"})

if __name__ == "__main__":
    while 1:
        loop = asyncio.get_event_loop()
        coroutine = process_crc()
        loop.run_until_complete(coroutine)
    
        
    
