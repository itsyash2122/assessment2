import json
import logging
from typing import Dict
from elasticsearch import AsyncElasticsearch
import tqdm
import asyncio
from thefuzz import fuzz
import pandas as pd
from functools import partial
from sqlalchemy import create_engine, text
from utils import haversine_vectorize
from bs4 import BeautifulSoup
import requests

from custom_exceptions import CaseStatusException


def get_es_client(cloud_id, api_id, api_key):
    return AsyncElasticsearch(
        cloud_id=cloud_id,
        api_key=(api_id , api_key), timeout=100

)

class QueryBuilder:
    
    def __init__(self, env, name, father_name, pincode, case_state, case_district):
        self.name = name.lower()
        self.father_name = father_name.lower()
        father_name_diff = list(set(father_name.lower().split(" ")) - set(name.lower().split(" ")))
        if len(father_name_diff) > 0:
            self.modified_name = ' '.join(name.split(" ")[:-1] + [father_name_diff[0]] + [name.split(" ")[-1]] ) 
            self.use_modified_name = 1
        else:
            self.modified_name = name.lower()
            self.use_modified_name = 0
        self.pincode = pincode
        db_string = env['db_string']
        engine = create_engine(db_string)
        with engine.connect() as connect:
            query = f"select district, state from pincode where pincode = {pincode}"
            try:
                district, state = connect.execute(text(query)).fetchall()[0]
                connect.commit()
            except IndexError:
                raise CaseStatusException("Pincode not found", 203, "red")
            
        self.case_state = state.lower()
        self.case_district = district.lower()
        
        self.name_father_district_state_query = {
        "query":{
            "bool":{
            "must": [ 
                {
                "match":{"name":{"query": self.name, "boost": 10}}
                },
                # {
                # "match": {"relation_type": self.relation }
                # },
                {
                "match":  {"relative": {"query": self.father_name, "boost": 10}}
                }
                ],
                "filter": [
                {
                    "bool": {
                    "must": [
                    {
                        "term": {
                        "case_state": self.case_state
                        }
                    },
                        {
                        "term": {
                            "case_district": self.case_district
                        }
                        }
                    ]  
                } 
                    
                }
                ]
            }
            
        }
    }

        self.name_father_district_query = {
        "query":{
            "bool":{
            "must": [ 
                {
                "match":{"name":{"query": self.name, "boost": 10}}
                },
                # {
                # "match": {"relation_type": self.relation }
                # },
                {
                "match":  {"relative": {"query": self.father_name, "boost": 10}}
                }
                ],
                "filter": [
                        {
                        "term": {
                            "case_district": self.case_district
                        }
                        }  
                ]
            }
        }
    }

        self.name_father_state_query = {
        "query":{
            "bool":{
            "must": [ 
                {
                "match":{"name":{"query": self.name, "boost": 10}}
                },
                # {
                # "match": {"relation_type": self.relation }
                # },
                {
                "match":  {"relative": {"query": self.father_name, "boost": 10}}
                }
                ],
                "filter": [
                        {
                        "term": {
                            "case_state": self.case_state
                        }
                        }  
                ]
            }
        }
    }

        self.name_father_query = {
        "query":{
            "bool":{
            "must": [ 
                {
                "match":{"name":{"query": self.name, "boost": 10}}
                },
                # {
                # "match": {"relation_type": self.relation }
                # },
                {
                "match":  {"relative": {"query": self.father_name, "boost": 10}}
                }
                ]
            }
        }
    }

        self.name_district_query = {
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "name": self.name
                                }
                                }
                            ],
                            "filter": [
                                {
                                "term": {
                                    "case_district": {"value": self.case_district}
                                }
                                }
                            ]
                            }
                        }
                        }

        self.name_state_query = {
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "name": self.name
                                }
                                }
                            ],
                            "filter": [
                                {
                                "term": {
                                    "case_state": {"value": self.case_state}
                                }
                                }
                            ]
                            }
                        }
                        } 
        self.name_query = {
            "query":{
                "match":{"name": self.name}
                }
            }
        self.modified_name_query = {
            "query": {
                "match": {"name": self.modified_name}
            }
        }

    def get_query_list(self):
        if self.use_modified_name == 1:
            query_list = [self.name_query,
                          self.modified_name_query, 
                          self.name_district_query, 
                          self.name_state_query, 
                          self.name_father_query,
                          self.name_father_district_query, 
                          self.name_father_state_query
                        ]
        else:
            query_list = [self.name_query, 
                          self.name_district_query, 
                          self.name_state_query, 
                          self.name_father_query,
                          self.name_father_district_query, 
                          self.name_father_state_query
                          ]
        return query_list


async def search_query(env, query, size=100):
    async with AsyncElasticsearch(cloud_id=env['cloud_id'], api_key=(env['api_id'], env['api_key']), timeout=100) as es:
        result = await es.search(index=env['index_name'], body=query, size=100, )
        return result
    
async def get_search(env, qb_instance, size=100):
    query_list = qb_instance.get_query_list()
    tasks = [asyncio.create_task(search_query(env, query, size )) for query in query_list]
    
    responses = await asyncio.gather(*tasks)
    return responses

def process_response(env, qb_instance, responses):
    parse_partial = partial(parse_elastic_result, qb_instance)
    result = [parse_partial(resp) for resp in responses]
    result = pd.concat(result)
    result['cnr_part'] = result['cnr'].str[:6]
    result = get_distance(result, env)
    return result

def get_match(name, name_list):
    return [fuzz.ratio(name, nl) for nl in name_list]

def get_act_section(cnr, html_link):
    
    contents = requests.get(html_link).text

    soup = BeautifulSoup(contents, 'html.parser')
    table = soup.find("table", {"class": 'Acts_table'})
    if table is not None:
        # Find all the rows in the table
        rows = table.find_all('tr')

        data = []
        for row in rows:
            # Find all columns in each row
            cols = row.find_all('td')

            # Get the text from each column
            cols = [col.text for col in cols]

            data.append(cols)

        # Print the data
        act_section = []
        for d in data:
            if len(d) > 0:
                act_section.append(d)
        act_section_df = pd.DataFrame(act_section, columns=['act', 'section'])
        act_section_df['cnr'] = cnr
        return act_section_df[['cnr', 'act', 'section']]
    else:
        return pd.DataFrame()


def parse_elastic_result(qb_instance, result):
    name = qb_instance.name
    father_name = qb_instance.father_name
    modified_name = qb_instance.modified_name
    district = qb_instance.case_district
    state = qb_instance.case_state
    hits = result['hits']['hits']
    result_list = []
    for hit in hits:
        source = hit['_source']
        values = list(source.values())
        keys = list(source.keys())
        result_list.append(values)
    if len(result_list) > 0:
        result_df = pd.DataFrame (result_list, columns=keys)
        result_df['input_name'] = name
        result_df['input_father_name'] = father_name
        result_df['input_district'] = district
        result_df['input_state'] = state
        result_df['input_modified_name'] = modified_name
        result_df['input_pincode'] = qb_instance.pincode
        result_df['name_match'] = get_match(name, 
                                            result_df['name'].values.tolist())

        result_df['percentage_father_in_name'] = get_match(father_name, 
                                            result_df['name'].values.tolist())

        result_df['modified_name_match'] = get_match(modified_name, 
                                                     result_df['name'].values.tolist())
        result_df['father_name_match'] = get_match(father_name, 
                                                   result_df['relative'].values.tolist())

        
        # result_df["district_match"] = get_match(district, 
        #                                         result_df['case_district'].values.tolist())
        # result_df['state_match'] = get_match(state, result_df['case_state'].values.tolist())

        result_df['in_same_district']=result_df.apply(lambda x: 1 if len(set(x['input_district'].lower().strip()).intersection(
                x['case_district'].lower().strip())) > 0 else 0 , axis=1)
    
        result_df['in_same_state']=result_df.apply(lambda x: 1 if len(set(x['input_state'].lower().strip()).intersection(
                x['case_state'].lower().strip())) > 0 else 0 , axis=1)
       

        result_df['father_in_name']  = result_df.apply(lambda x: 
            1 if len(set(x['input_father_name'].lower().split(" ")).intersection(x['name'].lower().split(" "))) > 0 else 0 , axis=1)
    else:
        result_df = pd.DataFrame()
    return result_df


def get_distance(result, env):
    cnr_list = tuple(result.cnr.unique().tolist())
    cnr_part_list = tuple(result.cnr_part.unique().tolist())
    candidate_pincode = result.input_pincode.unique()[0]
    db = create_engine(env['db_string'])
    with db.connect() as connect:
        candidate_lat_lon = connect.execute(text(f"select latitude, longitude from pincode where pincode = {candidate_pincode}")).fetchall()
        fir_idx = pd.DataFrame(
            connect.execute(
                text(
                    f"select cnr, idx_list from {env['cnr_fir_idx']} where cnr in {cnr_list}"
                    )))
        fir_idx_expanded = fir_idx.explode("idx_list")
        fir_idx_expanded['cnr'] = fir_idx_expanded['cnr'].str.strip()
        fir_idx_expanded['idx_list'] =fir_idx_expanded['idx_list'].astype(int)
        idx_list = tuple(fir_idx_expanded.idx_list.values.tolist())
        cnr_lat_lon = pd.DataFrame(
            connect.execute(
                text(
                    f"select cnr, latitude, longitude, idx from {env['cnr_fir_pincode']} where idx in {idx_list}"
                    )))
        
        cnr_court_lat_lon = pd.DataFrame(
            connect.execute(
                text(
                    f"select cnr_part, latitude, longitude from {env['court_pincode']} where cnr_part in {cnr_part_list}"
        )))
    latitude, longitude = candidate_lat_lon[0]
    cnr_court_lat_lon['court_distance'] = haversine_vectorize(longitude, 
                                                              latitude, 
                                                              cnr_court_lat_lon.longitude.values,
                                                              cnr_court_lat_lon.latitude.values
                                                              )
    
    cnr_lat_lon['police_station_distance'] = haversine_vectorize(longitude, 
                                                             latitude, 
                                                             cnr_lat_lon.longitude.values,
                                                             cnr_lat_lon.latitude.values)
    # If a police station have multiple lat-lon, we might end up getting multiple distance
    # for a CNR, we retain the minimum distance
    cnr_lat_lon.sort_values("police_station_distance", inplace=True)
    cnr_lat_lon.drop_duplicates(subset="police_station_distance", keep="first", inplace=True)
    result = pd.merge(result, cnr_lat_lon[['cnr', 'police_station_distance']], on="cnr", how="left")
    result = pd.merge(result, cnr_court_lat_lon[['cnr_part', 'court_distance']], on="cnr_part", how="left")
    
    return result
    
        
    