from dateutil import parser

TZ_TEMPLATE = "2002-02-26T09:43:49.000000Z"


def filter_placement(placement, candidate_id):
    offer_id = None if placement["offer"] is None else\
        int(placement["offer"]["id"])
    stage_id = None if placement["stage"] is None else\
        int(placement["stage"]["id"])

    filtered = {
        "id": int(placement["id"]),
        "offer_id": offer_id,
        "stage_id": stage_id,
        "disqualified": bool(placement["disqualified"]),
        "disqualify_reason": str(placement["disqualify_reason"]),
        "candidate_id": candidate_id
    }
    return filtered


def filter_tag(tag, candidate_id):
    filtered = {
        "tag_id": int(tag["id"]),
        "name": str(tag["name"]),
        "candidate_id": candidate_id
    }
    return filtered


def filter_source(source, candidate_id):
    filtered = {
        "source_id": int(source["id"]),
        "name": str(source["name"]),
        "candidate_id": candidate_id
    }
    return filtered


def filter_candidate(candidate):
    tzinfo = parser.parse(TZ_TEMPLATE).tzinfo
    if len(candidate["emails"]) > 0:
        email = candidate["emails"][0]
    else:
        email = None
    if len(candidate["phones"]) > 0:
        phone = candidate["phones"][0]
    else:
        phone = None

    positive_ratings = None if candidate["positive_ratings"] is None else\
        int(candidate["positive_ratings"])

    created_at = None if candidate["created_at"] is None else\
        parser.parse(candidate["created_at"]).replace(tzinfo=tzinfo).isoformat()
    updated_at = None if candidate["updated_at"] is None else\
        parser.parse(candidate["updated_at"]).replace(tzinfo=tzinfo).isoformat()

    filtered = {
        "id": int(candidate["id"]),
        "name": str(candidate["name"]),
        "email": email,
        "phone": phone,
        "source": str(candidate["source"]),
        "created_at": created_at,
        "updated_at": updated_at,
        "positive_ratings": positive_ratings,
    }
    return filtered
