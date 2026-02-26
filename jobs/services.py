import requests
from django.utils.dateparse import parse_datetime
from .models import Job

API_URL = "https://api.reliefweb.int/v1/jobs"

APPNAME = "RishiAryal-ICTFetcher5702-8HCZDumw"

ICT_QUERY = (
    "ICT OR Technology OR Data OR IT OR "
    "Software OR Developer OR Engineer OR "
    "Network OR System"
)


def fetch_ict_jobs():
    limit = 50
    offset = 0
    total_saved = 0
    total_skipped = 0
    total_failed = 0

    while True:
        response = requests.post(
            f"{API_URL}?appname={APPNAME}",  # ✅ appname in URL
            headers={
                "Content-Type": "application/json",
                "User-Agent": f"{APPNAME}"
            },
            json={
    "filter": {
        "field": "status",
        "value": "open"
    },
    "sort": ["date.created:desc"],
    "limit": 10,
    "fields": {
        "include": [
            "id",
            "title",
            "url_alias"
        ]
    }
}
        )

        if response.status_code != 200:
            print(f"API Error {response.status_code}: {response.text}")
            break

        data = response.json()
        jobs = data.get("data", [])

        if not jobs:
            break

        for item in jobs:
            fields = item.get("fields", {})

            reliefweb_id = item.get("id")
            title = fields.get("title")
            url_alias = fields.get("url_alias")

            if not reliefweb_id or not title or not url_alias:
                total_skipped += 1
                continue

            url = f"https://reliefweb.int{url_alias}"

            organization = (
                fields["source"][0]["name"]
                if fields.get("source")
                else None
            )

            country = (
                fields["country"][0]["name"]
                if fields.get("country")
                else None
            )

            closing_date_str = fields.get("date", {}).get("closing")
            closing_date = (
                parse_datetime(closing_date_str)
                if closing_date_str else None
            )

            try:
                Job.objects.update_or_create(
                    reliefweb_id=reliefweb_id,
                    defaults={
                        "title": title,
                        "organization": organization,
                        "country": country,
                        "closing_date": closing_date,
                        "url": url,
                    }
                )
                total_saved += 1

            except Exception as e:
                print(f"DB Error: {e}")
                total_failed += 1

        offset += limit

    print(
        f"Completed. Saved: {total_saved}, "
        f"Skipped: {total_skipped}, "
        f"Failed: {total_failed}"
    )

    return "ICT Jobs Updated Successfully"