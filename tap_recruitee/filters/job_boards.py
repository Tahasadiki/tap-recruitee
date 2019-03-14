def filter_job_board(job_board):
    filtered = {
        "id": int(job_board["id"]),
        "name": str(job_board["name"]),
        "provider": str(job_board["provider"]),
        "location": str(job_board["location"]),
        "website": str(job_board["website"]),
        "description": str(job_board["description"]),
        "codename": str(job_board["codename"]),
        "category": str(job_board["category"]),
        "require_additional_options": bool(job_board["require_additional_options"]),
        "premium": bool(job_board["premium"]),
        "position": int(job_board["position"]),
        "logo_url": str(job_board["logo_url"])
    }
    return filtered
