from dagster import RepositoryDefinition
from weather_blr import weather_blr_job, hourly_weather_blr_schedule
from weather_hyd import weather_hyd_job, hourly_weather_hyd_schedule

def define_repo():
    return RepositoryDefinition(
        name="weather_repository",
        jobs=[weather_blr_job,weather_hyd_job],
        schedules=[hourly_weather_blr_schedule,hourly_weather_hyd_schedule],
    )
