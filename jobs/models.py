# jobs/models.py
from django.db import models

class Job(models.Model):
    reliefweb_id = models.CharField(max_length=50, unique=True)
    title = models.CharField(max_length=500)
    organization = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=255, null=True, blank=True)
    closing_date = models.DateTimeField(null=True, blank=True)
    url = models.URLField()
    career_categories = models.CharField(max_length=500, null=True, blank=True)  # new field to store categories

    def __str__(self):
        return self.title
