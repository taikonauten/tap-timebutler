#!/usr/bin/env python3

import os

import backoff
import requests
import csv
import numpy as np
import pandas as pd
from datetime import timedelta, date, datetime

import singer
from singer import Transformer, utils

LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = [
    "auth_token",
    "x_dfa_token"
]

BASE_API_URL = "https://timebutler.de/api/v1/"
HOLIDAY_API_URL = "https://deutsche-feiertage-api.de/api/v1/"
CONFIG = {}
STATE = {}
AUTH = {}
HOLIDAYS = {}


class Auth:
    def __init__(self, auth_token):
        self._auth_token = auth_token

    def get_auth_token(self):
        return self._auth_token

class XDFA:

    holidays = {}

    def __init__(self, xdfa_token):
        self._xdfa_token = xdfa_token

    def get_xdfa_token(self):
        return self._xdfa_token

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_and_write_schema(name, key_properties='id', bookmark_property='updated_at'):
    schema = load_schema(name)
    singer.write_schema(name, schema, key_properties, bookmark_properties=[bookmark_property])
    return schema

def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]

def get_url(endpoint):
    return BASE_API_URL + endpoint

def get_holiday_url(year):
    return HOLIDAY_API_URL + year

def handle_absence_types(absence_type, field):

    absences_map = {
      "Vacation": {
        "absence_shorthandle": "URL",
        "absence_id": 101,
      },
      "Sickness": {
        "absence_shorthandle": "KRA",
        "absence_id": 102,
      },
      "Feiertag": {
        "absence_shorthandle": "FEI",
        "absence_id": 103,
      },
      "miscellaneous": {
        "absence_shorthandle": "SON",
        "absence_id": 104,
      },
      "Ze": {
        "absence_shorthandle": "ZAG",
        "absence_id": 105,
      },
      "Berufsschule/Uni": {
        "absence_shorthandle": "BER",
        "absence_id": 106,
      },
      "Pflicht/AS": {
        "absence_shorthandle": "PFL",
        "absence_id": 107,
      },
      "TaikoWeekend": {
        "absence_shorthandle": "TAW",
        "absence_id": 108,
      },
      "Overtime": {
        "absence_shorthandle": "OVT",
        "absence_id": 109,
      },
      "Overtime reduction request": {
        "absence_shorthandle": "OVT-R",
        "absence_id": 110,
      },
      "Un": {
        "absence_shorthandle": "BER",
        "absence_id": 111,
      },
    }

    return absences_map[absence_type][field]

@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2)

@utils.ratelimit(100, 15)

def request(url, params={}, headers={}):
    req = requests.Request("POST", url=url, params=params, headers=headers).prepare()
    LOGGER.info("POST {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()

    return resp

# Any date-times values can either be a string or a null.
# If null, parsing the date results in an error.
# Instead, removing the attribute before parsing ignores this error.
def remove_empty_date_times(item, schema):
    fields = []

    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        if item.get(field) is None:
            del item[field]


def get_holidays(year):

    schema_name = "absences"
    schema = load_schema(schema_name)

    singer.write_schema(schema_name,
                        schema,
                        ["id"])

    xdfa_token = XDFA.get_xdfa_token()
    headers = {"X-DFA-Token": xdfa_token}
    params = {}

    with Transformer() as transformer:
        url = get_holiday_url(year)

        response = request(url, params, headers)
        time_extracted = utils.now()
        response = response.json()

        holidays = {}
        
        for row in response["holidays"]:

            if row["holiday"]["regions"]["be"] == True:

                date_split = row["holiday"]["date"].split("-")

                formatted_date = datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))

                holidays["the_day"] = formatted_date.strftime("%d.%m.%Y")
                holidays["absence_type"] = "Feiertag"
                holidays["absence_state"] = "Approved"
                holidays["comment"] = row["holiday"]["name"]
                holidays["absence_shorthandle"] = handle_absence_types(holidays["absence_type"], "absence_shorthandle")
                holidays["absence_id"] = handle_absence_types(holidays["absence_type"], "absence_id")

                item = transformer.transform(holidays, schema)

                singer.write_record(schema_name,
                                    item,
                                    time_extracted=time_extracted)

    singer.write_state(STATE)

def sync_absences(schema_name, year):
    schema = load_schema(schema_name)

    auth_token = AUTH.get_auth_token()
    auth_params = {"auth": auth_token}
    params = {**auth_params, **year}

    singer.write_schema(schema_name,
                        schema,
                        ["id"])

    with Transformer() as transformer:
        url = get_url(schema_name)
        response = request(url, params, headers={})
        response = response.content.decode('utf-8')
        cr = csv.reader(response.splitlines(), delimiter=',')
        response = list(cr)

        time_extracted = utils.now()

        properties = list(schema['properties'])

        del response[0]

        for row in response:

            aligned_schema_row = {}

            row = np.array(row[0].split(';'))

            i = 0

            while i < len(row):

                if properties[i] == 'the_day':

                    continue

                elif properties[i] == 'absence_shorthandle':

                  continue

                elif properties[i] == 'absence_id':

                  continue

                else:

                    aligned_schema_row[properties[i]] = None if row[i].strip() == "" else row[i].strip()
                
                i += 1

            
            # LOGGER.info(aligned_schema_row)

            date_from = aligned_schema_row['day_from'].split('/')
            date_to = aligned_schema_row['day_to'].split('/')

            k = 0

            for dt in pd.date_range(start=date_from[2] + '-' + date_from[1] + '-' + date_from[0], end=date_to[2] + '-' + date_to[1] + '-' + date_to[0]):

                date_aligned_shema_row = aligned_schema_row

                date = dt.strftime("%d.%m.%Y")
              
                date_aligned_shema_row['id'] = int(date_aligned_shema_row['id']) + k
                date_aligned_shema_row['the_day'] = date

                date_aligned_shema_row["absence_shorthandle"] = handle_absence_types(date_aligned_shema_row['absence_type'], "absence_shorthandle")
                date_aligned_shema_row["absence_id"] = handle_absence_types(date_aligned_shema_row['absence_type'], "absence_id")

                k += 1

                remove_empty_date_times(date_aligned_shema_row, schema)

                item = transformer.transform(date_aligned_shema_row, schema)

                singer.write_record(schema_name,
                                    item,
                                    time_extracted=time_extracted)

    singer.write_state(STATE)

def sync_endpoint(schema_name, params={}):
    schema = load_schema(schema_name)

    auth_token = AUTH.get_auth_token()
    auth_params = {"auth": auth_token}
    params = {**auth_params, **params}

    singer.write_schema(schema_name,
                        schema,
                        ["id"])

    with Transformer() as transformer:
        url = get_url(schema_name)
        response = request(url, params, headers={})
        response = response.content.decode('utf-8')
        cr = csv.reader(response.splitlines(), delimiter=',')
        time_extracted = utils.now()
        response = list(cr)

        properties = list(schema['properties'])

        del response[0]

        for row in response:

            aligned_schema_row = {}

            row = np.array(row[0].split(';'))

            i = 0

            while i < len(row):

                aligned_schema_row[properties[i]] = None if row[i].strip() == "" else row[i].strip()

                i += 1

            remove_empty_date_times(aligned_schema_row, schema)

            item = transformer.transform(aligned_schema_row, schema)

            singer.write_record(schema_name,
                                item,
                                time_extracted=time_extracted)

    singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    today = datetime.now()
    years = range(2010,today.year + 1)

    for year in years:
        get_holidays(str(year))

    for year in years:
        sync_absences("absences", {"year": year})

    sync_endpoint("users")

    for year in years:
        sync_endpoint("holidayentitlement", {"year": year})

    sync_endpoint("workdays")

    sync_endpoint("worktime")

    sync_endpoint("projects")

    sync_endpoint("services")
    
    LOGGER.info("Sync complete")

def do_discover():
    print('{"streams":[]}')

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    global AUTH  # pylint: disable=global-statement
    AUTH = Auth(CONFIG['auth_token'])
    global XDFA
    XDFA = XDFA(CONFIG['x_dfa_token'])
    STATE.update(args.state)
    if args.discover:
        do_discover()
    else:
        do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
