import singer
import singer.metrics as metrics
from singer import utils
import json
import urllib
import requests
import os
import sys
import backoff
import itertools
import attr
from dateutil import parser

# importing filter modules
import tap_recruitee.filters.job_boards as jb
import tap_recruitee.filters.candidates as cand
import tap_recruitee.filters.offers as ofr


REQUIRED_CONFIG_KEYS = ["url", "company_id", "auth_token", "start_date"]
LOGGER = singer.get_logger()

CONFIG = {
    "url": "https://api.recruitee.com/",
    "company_id": "3233",
    "auth_token": "auth_token",
    "start_date": "1900-02-26T09:43:49.000000Z"
}

ENDPOINTS = {
    "job_boards": "c/{0}/job_boards?auth_token={1}",
    "candidates": "c/{0}/search/new/candidates?auth_token={1}&sort_by=created_at_desc&page={2}&limit=100",
    "offers": "c/{0}/offers?auth_token={1}"
}


def get_endpoint(endpoint, kwargs):
    '''Get the full url of the endpoint'''
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    company_id = urllib.parse.quote(CONFIG["company_id"])
    auth_token = urllib.parse.quote(CONFIG["auth_token"])
    if len(kwargs) > 0:
        page = kwargs[0]
    else:
        page = None
    return CONFIG["url"] + ENDPOINTS[endpoint].format(company_id, auth_token, page)


def iso_format(datetime):
    return parser.parse(datetime).isoformat()


def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG["start_date"]
    return current_bookmark


def load_schema(entity):
    '''Retruns the schema for the specifiedsource'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))
    return schema


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((backoff.expo, requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_request(stram_id, url):
    with metrics.http_request_timer(stram_id) as timer:
        resp = requests.get(url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def get_streams_to_sync(streams, state):
    '''Get the Streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams
    if current_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != current_stream, streams))
    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))
    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if(hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas():
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({
            'stream': stream.tap_stream_id,
            'tap_stream_id': stream.tap_stream_id,
            'schema': load_discovered_schema(stream)
        })

    return result


def do_discover():
    '''JSON dump the schemas to stdout'''
    LOGGER.info("Loading Schemas")
    json.dump(discover_schemas(), sys.stdout, indent=4)


def update_state(stream, last_update):
    with open(get_abs_path("state.json"), "r+") as state_file:
        if state_file is not None:
            state = json.load(state_file)
            state["bookmarks"][stream]["last_update"] = last_update
            state_file.seek(0)
            json.dump(state, state_file, indent=4)
            state_file.truncate()


def select_stream(stream_to_select, catalog, selected):
    with open(catalog, "r+") as catalog_file:
        if catalog_file is not None:
            catalog = json.load(catalog_file)
            for index, stream in enumerate(catalog["streams"]):
                if stream["stream"] == stream_to_select:
                    catalog["streams"][index]["schema"]["selected"] = selected
                    LOGGER.info("Stream {} is selected = {}".format(stream["stream"], selected))
            catalog_file.seek(0)
            json.dump(catalog, catalog_file, indent=4)
            catalog_file.truncate()


# syncing function for each stream


def sync_job_boards(STATE, catalog):
    schema = load_schema("job_boards")
    singer.write_schema("job_boards", schema, "id")

    start = get_start(STATE, "job_boards", "last_update")
    LOGGER.info("Only syncing job_boards updated since " + start)
    last_update = start
    with metrics.record_counter("job_boards") as counter:

        endpoint = get_endpoint("job_boards", [])
        LOGGER.info("GET %s", endpoint)
        resp = gen_request("job_boards", endpoint)
        job_boards = resp["job_boards"]
        for job_board in job_boards:
            counter.increment()
            job_board = jb.filter_job_board(job_board)
            singer.write_record("job_boards", job_board)

    STATE = singer.write_bookmark(STATE, 'job_boards', 'last_update', last_update)
    singer.write_state(STATE)
    LOGGER.info("Completed Job Boards Sync")
    return STATE


def sync_candidate_tags(candidate):
    LOGGER.info("-----/ Syncing candidate_tags")
    candidate_id = int(candidate["id"])
    if "tags" in candidate and len(candidate["tags"]) > 0:
        with metrics.record_counter("candidate_tags") as counter:
            candidate_tags = [cand.filter_tag(tag, candidate_id) for tag in candidate["tags"]]
            for tag in candidate_tags:
                counter.increment()
                singer.write_record("candidate_tags", tag)


def sync_candidate_placements(candidate):
    LOGGER.info("-----/ Syncing candidate_placements")
    candidate_id = int(candidate["id"])
    if "placements" in candidate and len(candidate["placements"]) > 0:
        with metrics.record_counter("candidate_placements") as counter:
            candidate_placements = [cand.filter_placement(placement, candidate_id) for placement in candidate["placements"]]
            for placement in candidate_placements:
                counter.increment()
                singer.write_record("candidate_placements", placement)


def sync_candidate_sources(candidate):
    LOGGER.info("-----/ Syncing candidate_sources")
    candidate_id = int(candidate["id"])
    if "sources" in candidate and len(candidate["sources"]) > 0:
        with metrics.record_counter("candidate_sources") as counter:
            candidate_sources = [cand.filter_source(source, candidate_id) for source in candidate["sources"]]
            for source in candidate_sources:
                counter.increment()
                singer.write_record("candidate_sources", source)


def sync_candidates(STATE, catalog):
    schema = load_schema("candidates")
    singer.write_schema("recruitee_candidates", schema, ["id"])

    tag_schema = load_schema("candidate_tags")
    singer.write_schema("candidate_tags", tag_schema, ["tag_id", "candidate_id"])

    placement_schema = load_schema("candidate_placements")
    singer.write_schema("candidate_placements", placement_schema, ["id"])

    source_schema = load_schema("candidate_sources")
    singer.write_schema("candidate_sources", source_schema, ["source_id", "candidate_id"])

    start = get_start(STATE, "candidates", "last_update")
    LOGGER.info("Only syncing candidates updated since " + start)
    last_update = start
    page_number = 1
    with metrics.record_counter("candidates") as counter:
        while True:
            endpoint = get_endpoint("candidates", [page_number])
            LOGGER.info("GET %s", endpoint)
            resp = gen_request("candidates", endpoint)
            candidates = resp["hits"]
            for candidate in candidates:
                if iso_format(candidate["updated_at"]) > iso_format(start):
                    counter.increment()
                    sync_candidate_tags(candidate)
                    sync_candidate_placements(candidate)
                    sync_candidate_sources(candidate)
                    candidate = cand.filter_candidate(candidate)
                    singer.write_record("recruitee_candidates", candidate)
                    last_update = max(iso_format(candidate["updated_at"]), iso_format(last_update))
            if len(candidates) < 100:
                break
            else:
                page_number += 1
    STATE = singer.write_bookmark(STATE, 'candidates', 'last_update', last_update)
    singer.write_state(STATE)
    update_state("candidates", last_update)
    LOGGER.info("Completed candidates, placements and tags Sync, last update : " + last_update)
    return STATE


def sync_offer_stages(offer):
    LOGGER.info("-----/ Syncing offer_stages")
    offer_id = int(offer["id"])
    if "stages" in offer and len(offer["stages"]) > 0:
        with metrics.record_counter("offer_stages") as counter:
            offer_stages = [ofr.filter_stage(stage, offer_id) for stage in offer["stages"]]
            for stage in offer_stages:
                counter.increment()
                singer.write_record("offer_stages", stage)


def sync_offers(STATE, catalog):
    schema = load_schema("offers")
    singer.write_schema("offers", schema, "id")

    stage_schema = load_schema("offer_stages")
    singer.write_schema("offer_stages", stage_schema, ["id"])

    start = get_start(STATE, "offers", "last_update")
    LOGGER.info("Only syncing offers updated since " + start)
    last_update = start
    with metrics.record_counter("offers") as counter:

        endpoint = get_endpoint("offers", [])
        LOGGER.info("GET %s", endpoint)
        resp = gen_request("offers", endpoint)
        offers = resp["offers"]
        for offer in offers:
            if iso_format(offer["updated_at"]) > iso_format(start):
                counter.increment()
                sync_offer_stages(offer)
                offer = ofr.filter_offer(offer)
                singer.write_record("offers", offer)
                last_update = max(iso_format(offer["updated_at"]), iso_format(last_update))

    STATE = singer.write_bookmark(STATE, 'offers', 'last_update', last_update)
    singer.write_state(STATE)
    update_state("offers", last_update)
    LOGGER.info("Completed offers Sync, last update : " + last_update)
    return STATE

# [END] syncing function for each stream


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()


STREAMS = [
    Stream("job_boards", sync_job_boards),
    Stream("candidates", sync_candidates),
    Stream("offers", sync_offers)


]


def do_sync(STATE, catalogs):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Stremas selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [cat for cat in catalogs.streams if cat.stream == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog)
        except Exception as e:
            LOGGER.critical(e)
            raise e


@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(STATE, args.properties)
    elif args.catalog:
        LOGGER.info(args.catalog)
        do_sync(STATE, args.catalog)
    else:
        LOGGER.info("No Streams were selected")


if __name__ == "__main__":
    main()
