from django.urls import path
from .views import job_list

urlpatterns = [
    path("", job_list, name="job_list"),
]