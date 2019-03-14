from dateutil import parser

TZ_TEMPLATE = "2002-02-26T09:43:49.000000Z"


def filter_stage(stage, offer_id):
    filtered = {
        "id": int(stage["id"]),
        "name": str(stage["name"]),
        "category": str(stage["category"]),
        "offer_id": offer_id,
        "position": int(stage["position"])
    }
    return filtered


def filter_offer(offer):
    tzinfo = parser.parse(TZ_TEMPLATE).tzinfo
    created_at = None if offer["created_at"] is None else\
        parser.parse(offer["created_at"]).replace(tzinfo=tzinfo).isoformat()
    updated_at = None if offer["updated_at"] is None else\
        parser.parse(offer["updated_at"]).replace(tzinfo=tzinfo).isoformat()
    published_at = None if offer["published_at"] is None else\
        parser.parse(offer["published_at"]).replace(tzinfo=tzinfo).isoformat()

    filtered = {
        "id": int(offer["id"]),
        "title": str(offer["title"]),
        "kind": str(offer["kind"]),
        "created_at": created_at,
        "updated_at": updated_at,
        "published_at": published_at,
        "country_code": str(offer["country_code"]),
        "city": str(offer["city"]),
        "department": str(offer["department"]),
        "url": str(offer["url"]),
        "status": str(offer["status"]),
        "candidates_count": int(offer["candidates_count"]),
        "qualified_candidates_count": int(offer["qualified_candidates_count"])
    }
    return filtered
