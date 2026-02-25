from django.core.paginator import Paginator
from django.shortcuts import render
from .models import Job

def job_list(request):
    jobs = Job.objects.all().order_by("-closing_date")

    paginator = Paginator(jobs, 10)  # 10 jobs per page
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    return render(request, "jobs/job_list.html", {"page_obj": page_obj})