import json
import os
from io import StringIO
from zipfile import ZipFile

import pandas as pd
from Utility import postgres_conn
from Utility import sdl_utility as sdl_util
from Utility import sdvmodels_utility
from UnstructuredDataParsingUtility import validation_utility

def get_data_services(request):
    try:
        if request.method == 'GET':
            if 'userid' not in request.GET:
                return json.dumps(
                    {"result": [], "message": "Request parameter 'userid' is missing"})
            if request.GET.get("userid") != user_id:
                return json.dumps(
                    {"result": [], "message": "Invalid User"})
            data_services_json = pd.read_json("response_json/dataservices_response.json")
            return json.dumps({"result": json.loads(data_services_json.to_json(orient="records")),
                               "message": "success"})
        else:
            return json.dumps(
                {"result": [], "message": "Request method does not support"})
    except Exception as e:
        print(e)
        return json.dumps({"result": [], "message": "failed to retrieve data services."})


def get_single_table_model():
    query = "SELECT CONCAT('S_', id) as id, REPLACE(sdv_model,'.pkl','') as name FROM public.sdv_models WHERE " \
            "sdv_model IN(SELECT model_file FROM synthetic_data_generation WHERE model_type='Single Table' and " \
            "status='Completed'); "
    response = postgres_conn.execute_get_query(query, [])
    return response["data"]


def get_relational_table_model():
    query = "SELECT CONCAT('M_', id) as id, REPLACE(sdv_model,'.pkl','') as name FROM public.rel_sdv_models " \
            "WHERE sdv_model IN(SELECT model_file FROM synthetic_data_generation WHERE model_type='Multi Table' and " \
            "status='Completed'); "
    response = postgres_conn.execute_get_query(query, [])
    return response["data"]


def get_model_response():
    single_table_model = get_single_table_model()
    multi_table_model = get_relational_table_model()
    for data in multi_table_model:
        single_table_model.append(data)
    return single_table_model


def get_log_parsing_response():
    query = "SELECT task_name as id, task_name as name FROM regex_tasks WHERE task_name IN (SELECT selected_task_name " \
            "FROM logvalidationdtl WHERE status='Completed') "
    response = postgres_conn.execute_get_query(query, [])
    return response["data"]


def get_semantic_model_list():
    query = "SELECT bod.id, bod.object_name as name FROM sdl_domain_objects bod WHERE bod.access_level='Public'"
    response = postgres_conn.execute_get_query(query, [])
    return response["data"]


def get_metadata(request):
    if request.method == 'GET':
        if 'dataserviceid' not in request.GET:
            return json.dumps({"result": [], "message": "Request parameter 'dataserviceid' is missing"})
        if 'userid' not in request.GET:
            return json.dumps({"result": [], "message": "Request parameter 'userid' is missing"})
        if request.GET.get("userid") != user_id:
            return json.dumps(
                {"result": [], "message": "Invalid User"})
        data_service_id = request.GET.get("dataserviceid")
        if data_service_id == "0":
            response = get_model_response()
            return json.dumps({"result": response, "message": "success"})
        elif data_service_id == "1":
            response = get_log_parsing_response()
            return json.dumps({"result": response, "message": "success"})
        elif data_service_id == "2":
            response = get_semantic_model_list()
            return json.dumps({"result": response, "message": "success"})
        else:
            return json.dumps({"result": [], "message": "Data service does not exist"})
    else:
        return json.dumps({"result": [], "message": "Request method does not support"})


single_model_folder = "SDV/generated_files"
relational_model_folder = "SDV_MULTI/generated_files"
log_parsing_folder = "UnstructuredDataParsing/Validation/OutputCSV"


def get_single_table_model_data(model_id):
    query = f"SELECT gen.id, gen.model_name, models.train_file, record_count FROM sdv_models models INNER JOIN " \
            f"synthetic_data_generation gen ON models.sdv_model = gen.model_file WHERE gen.status='Completed' and models.id={model_id} ORDER BY " \
            f"gen.end_time desc LIMIT 1 "
    response = postgres_conn.execute_get_query(query, [])
    result = []
    if len(response["data"]) > 0:
        model_name = response["data"][0]["model_name"]
        train_file = response["data"][0]["train_file"].split('.')
        record_count = response["data"][0]["record_count"]
        file_name = model_name + "_" + train_file[0] + "_" + record_count + ".csv"
        file_path = os.path.join(single_model_folder, file_name)

        if not os.path.exists(file_path):
            file_blob = sdvmodels_utility.get_synth_generated_file_object(id=response["data"][0]["id"])
            with open(file_path, "wb") as f:
                f.write(file_blob)

        result.append({train_file[0]: json.loads(pd.read_csv(file_path).to_json(orient="records"))})
    return result


def get_multi_table_model_data(model_id):
    query = f"SELECT gen.id, gen.model_name, record_count FROM public.rel_sdv_models model INNER JOIN " \
            f"public.synthetic_data_generation gen ON model.sdv_model = gen.model_file WHERE gen.status='Completed' and model.id = {model_id} " \
            f"ORDER BY gen.end_time desc LIMIT 1 "
    response = postgres_conn.execute_get_query(query, [])
    result = []
    if len(response["data"]) > 0:
        model_name = response["data"][0]["model_name"]
        record_count = response["data"][0]["record_count"]
        zip_file_name = model_name + "_" + record_count + ".zip"

        zip_file_path = os.path.join(relational_model_folder, zip_file_name)
        if not os.path.exists(zip_file_path):
            file_blob = sdvmodels_utility.get_synth_generated_file_object(id=response["data"][0]["id"])
            with open(zip_file_path, "wb") as f:
                f.write(file_blob)

        with ZipFile(os.path.join(relational_model_folder, zip_file_name), "r") as zf:
            for file in zf.namelist():
                response = {}
                if file.endswith('csv'):
                    file_name = file.split('/')[1].split('.')[0]
                    bytes_str = str(zf.read(file), 'utf-8')
                    data = StringIO(bytes_str)
                    df = pd.read_csv(data)
                    response[file_name] = json.loads(df.to_json(orient="records"))
                    result.append(response)
    return result


def get_model_last_generated_data(meta_data_id):
    split_meta_data = meta_data_id.split("_")
    result = []
    if split_meta_data[0] == "S":
        model_id = split_meta_data[1]
        return get_single_table_model_data(model_id)
        result.append({file_name: json.loads(response.to_json(orient="records"))})
        return result
    elif split_meta_data[0] == "M":
        model_id = split_meta_data[1]
        return get_multi_table_model_data(model_id)
    else:
        return result


def get_last_parsed_log_data(meta_data_id):
    query = f"SELECT val.id as val_id, val.taskname, val.filepath FROM public.logvalidationdtl val INNER JOIN public.regex_tasks regex " \
            f"ON val.selected_task_name=regex.task_name WHERE val.status='Completed' and val.selected_task_name='{meta_data_id}' ORDER BY " \
            f"val.createddate desc LIMIT 1 "
    response = postgres_conn.execute_get_query(query, [])
    result = []
    if len(response["data"]) > 0:
        val_task_name = response["data"][0]["taskname"]
        file_name = response["data"][0]["filepath"].split(".")[0] + ".csv"
        file_path = os.path.join(log_parsing_folder, file_name)

        if not os.path.exists(file_path):
            with open(file_path, "wb") as f:
                id = response["data"][0]["val_id"]
                result_csv_object = validation_utility.db_get_validation_result_csv_object(id)
                f.write(result_csv_object)

        result.append({val_task_name: json.loads(pd.read_csv(file_path).to_json(orient="records"))})
    return result


def get_semantic_data(userid, meta_data_id):
    sdl_obj = sdl_util.get_business_object_data_for_user_by_id(meta_data_id, userid)
    result = []
    if len(sdl_obj) > 0:
        object_name = sdl_obj[0]["object_name"]
        response = sdl_util.get_data_from_adls(userid, object_name, filter_data='', api_response=True)
        result.append({object_name: json.loads(response.to_json(orient="records"))})
    return result


def get_data(request):
    if request.method == 'GET':
        if 'dataserviceid' not in request.GET:
            return json.dumps({"result": [], "message": "Request parameter 'dataserviceid' is missing"})
        if 'metadataid' not in request.GET:
            return json.dumps({"result": [], "message": "Request parameter 'metadataid' is missing"})
        if 'userid' not in request.GET:
            return json.dumps({"result": [], "message": "Request parameter 'userid' is missing"})
        if request.GET.get("userid") != user_id:
            return json.dumps(
                {"result": [], "message": "Invalid User"})
        data_service_id = request.GET.get("dataserviceid")
        meta_data_id = request.GET.get("metadataid")
        if data_service_id == "0":
            result = get_model_last_generated_data(meta_data_id)
        elif data_service_id == "1":
            result = get_last_parsed_log_data(meta_data_id)
        elif data_service_id == "2":
            result = get_semantic_data(request.GET.get("userid"), meta_data_id)
        else:
            return json.dumps({"result": [], "message": "Data service id is not valid"})
        if len(result) < 1:
            return json.dumps({"result": [], "message": "Metadata id is not valid"})
        return json.dumps({"result": result, "message": "success"})
    else:
        return json.dumps({"result": [], "message": "Request method does not support"})
