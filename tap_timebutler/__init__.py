#!/usr/bin/env python3

import os

import backoff
import requests
import pendulum
import csv
import json
import numpy as np

import singer
from singer import Transformer, utils

LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = [
    "auth_token"
]

BASE_API_URL = "https://timebutler.de/api/v1/"
CONFIG = {}
STATE = {}
AUTH = {}


class Auth:
    def __init__(self, auth_token):

        self._auth_token = auth_token

    def get_auth_token(self):
        return self._auth_token


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


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2)
@utils.ratelimit(100, 15)
def request(url):
    auth_token = AUTH.get_auth_token()
    params = {"auth": auth_token}
    req = requests.Request("POST", url=url, params=params).prepare()
    LOGGER.info("POST {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    response = resp.content.decode('utf-8')
    cr = csv.reader(response.splitlines(), delimiter=',')
    return list(cr)

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


def append_times_to_dates(item, date_fields):
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))


def sync_endpoint(schema_name, endpoint=None, date_fields=None): #pylint: disable=too-many-arguments
    schema = load_schema(schema_name)

    singer.write_schema(schema_name,
                        schema,
                        ["id"])

    with Transformer() as transformer:
        url = get_url(endpoint or schema_name)
        response = request(url)
        time_extracted = utils.now()

        properties = list(schema['properties'])

        del response[0]

        for row in response:

            aligned_schema_row = {}

            row = np.array(row[0].split(';'))

            i = 0

            while i < len(row):

                  aligned_schema_row[properties[i]] = row[i].strip()

                  i += 1

            remove_empty_date_times(aligned_schema_row, schema)

            item = transformer.transform(aligned_schema_row, schema)

            append_times_to_dates(item, date_fields)

            singer.write_record(schema_name,
                                item,
                                time_extracted=time_extracted)

    singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    sync_endpoint("absences")
    
    LOGGER.info("Sync complete")

def do_discover():
    print('{"streams":[]}')

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    global AUTH  # pylint: disable=global-statement
    AUTH = Auth(CONFIG['auth_token'])
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
