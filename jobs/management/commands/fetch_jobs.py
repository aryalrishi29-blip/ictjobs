# jobs/management/commands/fetch_jobs.py
import requests
import time
from datetime import datetime, timezone
from django.core.management.base import BaseCommand
from django.db import transaction
from jobs.models import Job

APPNAME = "RishiAryal-ICTFetcher5702-8HCZDumw"
API_URL = f"https://api.reliefweb.int/v2/jobs?appname={APPNAME}"
LIMIT = 100

ICT_CATEGORIES = [
    "Information Technology",
    "Information Management",
    "Telecommunications",
]


class Command(BaseCommand):
    help = "Fetch ICT jobs from ReliefWeb and save to DB"

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages', type=int, default=20,
            help='Max pages per category (default: 20 = up to 2000 jobs)'
        )
        parser.add_argument(
            '--clear', action='store_true',
            help='Delete all existing jobs before fetching fresh data'
        )

    def handle(self, *args, **options):
        if options['clear']:
            count = Job.objects.all().delete()[0]
            self.stdout.write(self.style.WARNING(f"Cleared {count} existing jobs from DB."))

        self.stdout.write("Fetching ICT jobs...\n")
        max_pages = options['max_pages']
        grand_total_new = 0
        grand_total_updated = 0
        today = datetime.now(timezone.utc).date()

        for category in ICT_CATEGORIES:
            self.stdout.write(f"\n--- Category: {category} ---")
            offset = 0
            cat_new = 0
            cat_updated = 0

            for page in range(max_pages):
                payload = {
                    "limit": LIMIT,
                    "offset": offset,
                    "fields": {
                        "include": [
                            "title",
                            "url_alias",
                            "source.name",
                            "country.name",
                            "career_categories.name",
                            "date.closing",
                        ]
                    },
                    "filter": {
                        "operator": "AND",
                        "conditions": [
                            {"field": "career_categories.name", "value": category},
                            {"field": "status", "value": "published"},
                        ]
                    },
                    "sort": ["date.created:desc"],
                }

                try:
                    response = requests.post(API_URL, json=payload, timeout=20)

                    if response.status_code != 200:
                        self.stdout.write(self.style.ERROR(
                            f"API ERROR {response.status_code}: {response.text}"
                        ))
                        break

                    data = response.json()
                    total_count = data.get("totalCount", 0)
                    batch = data.get("data", [])

                    if not batch:
                        self.stdout.write("  No more jobs.")
                        break

                    self.stdout.write(
                        f"  Page {page + 1}: {len(batch)} jobs "
                        f"(offset {offset} of {total_count} total)"
                    )

                    for item in batch:
                        fields = item.get("fields", {})
                        reliefweb_id = str(item.get("id", "")).strip()
                        if not reliefweb_id:
                            continue

                        # url_alias is the full working URL including slug
                        # e.g. https://reliefweb.int/job/4194878/data-quality-field-lead
                        url = fields.get("url_alias", "").strip()
                        if not url:
                            self.stdout.write(self.style.WARNING(
                                f"  Skipping job {reliefweb_id}: no url_alias"
                            ))
                            continue

                        # Parse closing date
                        date_closing_raw = fields.get("date", {}).get("closing")
                        closing_date = None
                        if date_closing_raw:
                            try:
                                closing_date = datetime.fromisoformat(
                                    date_closing_raw.replace("Z", "+00:00")
                                ).date()
                            except Exception:
                                closing_date = None

                        # Skip already-expired jobs
                        if closing_date and closing_date < today:
                            continue

                        title = fields.get("title", "").strip()
                        if not title:
                            continue

                        sources = fields.get("source", [])
                        organization = ", ".join(
                            [s.get("name", "") for s in sources if s.get("name")]
                        ) or None

                        countries = fields.get("country", [])
                        country = ", ".join(
                            [c.get("name", "") for c in countries if c.get("name")]
                        ) or None

                        categories_list = fields.get("career_categories", [])
                        career_categories = ", ".join(
                            [c.get("name", "") for c in categories_list if c.get("name")]
                        ) or None

                        job_data = {
                            "title": title,
                            "organization": organization,
                            "country": country,
                            "career_categories": career_categories,
                            "closing_date": closing_date,
                            "url": url,
                        }

                        try:
                            with transaction.atomic():
                                obj, created = Job.objects.update_or_create(
                                    reliefweb_id=reliefweb_id,
                                    defaults=job_data
                                )
                                if created:
                                    cat_new += 1
                                else:
                                    cat_updated += 1
                        except Exception as db_err:
                            self.stdout.write(self.style.ERROR(
                                f"  DB ERROR for job {reliefweb_id}: {db_err}"
                            ))

                    offset += LIMIT
                    time.sleep(0.5)

                    if offset >= total_count:
                        self.stdout.write(self.style.SUCCESS(
                            f"  All {total_count} jobs fetched for this category."
                        ))
                        break

                except requests.exceptions.RequestException as e:
                    self.stdout.write(self.style.ERROR(f"  Request ERROR: {e}"))
                    break
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  Unexpected ERROR: {e}"))
                    break

            self.stdout.write(f"  Category done — New: {cat_new}, Updated: {cat_updated}")
            grand_total_new += cat_new
            grand_total_updated += cat_updated

        self.stdout.write(self.style.SUCCESS(
            f"\nAll done. Total new: {grand_total_new}, Total updated: {grand_total_updated}"
        ))