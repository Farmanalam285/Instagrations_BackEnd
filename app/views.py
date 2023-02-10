from typing import Optional
from app import app
from flask import request, make_response, jsonify
from requests.auth import HTTPBasicAuth
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from enum import Enum

import sqlalchemy
import pandas as pd
import requests
import json
from copy import deepcopy

db_engine = sqlalchemy.create_engine(
    "sqlite:////root/instagrations_backend/integrations_db", echo=True
)


@dataclass_json
@dataclass
class ApiRequest:
    stage: str
    url: str
    type_of_request: str
    params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    body: dict = field(default_factory=dict)
    auth_data: Optional[dict] = field(default_factory=dict)
    auth_type: Optional[str] = None
    output: Optional[str] = None
    previous_stages_output: Optional[dict[str, str]] = None
    previous_stages: Optional[list[dict[str, str]]] = None
    finished: Optional[bool] = None
    integration_name: Optional[str] = None
    vertica_id: Optional[int] = None
    vertica_table_name: Optional[str] = None


@app.route("/list_integrations")
def list_integrations():
    integrations = pd.read_sql("select * from integrations", db_engine)
    return make_response(integrations.to_dict("records"), 200)


@app.route("/run_stage", methods=["POST"])
def run_stage():
    if request.method == "POST":
        print(request.json)
        api_response_df, stage, _ = make_and_fire_request(request.json)
        response = {}
        response["api_response"] = api_response_df.to_dict("records")
        response["stage"] = stage
        return make_response(response, 200)


@app.route("/next_stage", methods=["POST"])
def next_stage():
    if request.method == "POST":
        print(request.json)
        request_data = request.json
        api_request = ApiRequest.from_dict(request_data)
        previous_stages = api_request.previous_stages
        current_stage_entry = {}
        for k, v in vars(api_request).items():
            if k == "previous_stages":
                continue
            current_stage_entry[k] = v
        previous_stages.append(current_stage_entry)

        # current_stage = previous_stages[-1]
        # current_stage_output = api_response_df.to_dict("records")[0][
        #     request_data["output"]
        # ]

        # response[f"{stage}_output"] = current_stage_output
        # current_stage[f"{stage}_output"] = current_stage_output
        # response["previous_stages"] = previous_stages
        return make_response(previous_stages, 200)


@app.route("/end_stage", methods=["POST"])
def end_stage():
    if request.method == "POST":
        print(request.json)
        request_data = request.json
        api_request = ApiRequest.from_dict(request_data)
        response = {}
        previous_stages = api_request.previous_stages
        current_stage_entry = {}
        for k, v in vars(api_request).items():
            if k == "previous_stages":
                continue
            current_stage_entry[k] = v
        previous_stages.append(current_stage_entry)


def make_and_fire_request(request_data) -> pd.DataFrame:
    api_request = ApiRequest.from_dict(request_data)
    print(api_request)
    new_api_request_dict = substitute_special_params(vars(api_request))
    api_request_sanitised = ApiRequest.from_dict(new_api_request_dict)
    api_request = api_request_sanitised

    auth = None
    if api_request.auth_type == "BASIC_AUTH":
        username = api_request.auth_data["username"]
        password = api_request.auth_data["password"]
        auth = (username, password)
    elif api_request.auth_type == "BEARER_AUTH":
        api_request.headers[
            "Authorization"
        ] = f"""Bearer: {api_request.auth_data["token"]}"""
    resp = requests.request(
        method=api_request.type_of_request,
        url=api_request.url,
        headers=api_request.headers,
        params=api_request.params,
        data=api_request.body,
        auth=auth,
    )
    print(resp.text)
    resp_dict = json.loads(resp.text)
    df = pd.json_normalize(resp_dict)
    if api_request.previous_stages is None:
        api_request.previous_stages = []

    api_request.previous_stages.append(request_data)
    return df, api_request.stage, api_request.previous_stages


def substitute_special_params(api_request_dict):
    previous_stages_output = api_request_dict["previous_stages_output"]
    new_api_request_dict = deepcopy(api_request_dict)
    for k, v in api_request_dict.items():
        if isinstance(v, dict):
            v_dash = get_if_any_special_key_value_pair(v, previous_stages_output)
        else:
            v_dash = check_and_replace(v, previous_stages_output)
        new_api_request_dict[k] = v_dash
        print(k, v, v_dash)
    return new_api_request_dict


def check_and_replace(val: str | int, data_dict):
    val_dash = val
    if isinstance(val, str) and val.startswith("@"):
        val_dash_var = val.split("@")[-1]
        val_dash = data_dict[val_dash_var]
    return val_dash


def get_if_any_special_key_value_pair(val_dict: dict, data_dict):
    new_val_dict = deepcopy(val_dict)
    for k, v in val_dict.items():
        if isinstance(v, str) and v.startswith("@"):
            new_val_dict[k] = check_and_replace(v, data_dict)
    return new_val_dict
