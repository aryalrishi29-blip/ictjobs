# jobs/management/commands/fetch_ict_jobs.py
import requests
import time
from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from django.db import transaction
from jobs.models import Job

APPNAME = "RishiAryal-ICTFetcher5702-8HCZDumw"
API_URL = f"https://api.reliefweb.int/v1/jobs?appname={APPNAME}"


class Command(BaseCommand):
    help = "Fetch Information Technology jobs from ReliefWeb and save active jobs to DB"

    def add_arguments(self, parser):
        parser.add_argument('--max-pages', type=int, default=5, help='Maximum pages to fetch')

    def handle(self, *args, **options):
        self.stdout.write("Fetching ICT jobs...\n")
        max_pages = options['max_pages']
        offset = 0
        limit = 100
        total_saved = 0
        today = datetime.now(timezone.utc).date()

        for page in range(max_pages):
            payload = {
                "profile": "full",
                "limit": limit,
                "offset": offset,
                "filter": {
                    "conditions": [
                        # broad ICT keyword set — include other categories if desired
                        {"field": "career_categories.name", "value": "Information Technology"}
                    ]
                }
            }

            try:
                response = requests.post(API_URL, json=payload, timeout=20)
                if response.status_code != 200:
                    self.stdout.write(self.style.ERROR(f"API ERROR: {response.status_code} {response.text}"))
                    break

                data = response.json()
                batch = data.get("data", [])
                if not batch:
                    self.stdout.write(self.style.SUCCESS("No more jobs to fetch."))
                    break

                for item in batch:
                    fields = item.get("fields", {})
                    reliefweb_id = item.get("id")
                    if not reliefweb_id:
                        continue

                    # parse closing date to date object
                    date_closing_raw = fields.get("date", {}).get("closing")
                    if date_closing_raw:
                        try:
                            closing_date = datetime.fromisoformat(date_closing_raw.replace("Z", "+00:00")).date()
                        except Exception:
                            closing_date = None
                    else:
                        closing_date = None

                    # skip expired
                    if closing_date and closing_date < today:
                        continue

                    sources = fields.get("source", [])
                    organization = ", ".join([s.get("name") for s in sources]) if sources else None

                    countries = fields.get("country", [])
                    country = ", ".join([c.get("name") for c in countries]) if countries else None

                    categories = fields.get("career_categories", [])
                    category = ", ".join([c.get("name") for c in categories]) if categories else None

                    job_data = {
                        "title": fields.get("title"),
                        "organization": organization,
                        "countries": country,
                        "career_categories": category,
                        "closing_date": closing_date,
                        "url": fields.get("url"),
                    }

                    try:
                        with transaction.atomic():
                            obj, created = Job.objects.update_or_create(
                                reliefweb_id=reliefweb_id,
                                defaults=job_data
                            )
                            if created:
                                total_saved += 1
                    except Exception as db_err:
                        self.stdout.write(self.style.ERROR(f"DB ERROR: {db_err}"))

                offset += limit
                time.sleep(0.5)
                self.stdout.write(self.style.SUCCESS(f"Fetched page {page+1}: {len(batch)} jobs, total saved: {total_saved}"))

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"ERROR: {e}"))
                break

        self.stdout.write(self.style.SUCCESS(f"\nFinished. Total saved: {total_saved}"))