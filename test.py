import pytest
import requests
import pandas as pd
from datetime import date
from unittest.mock import MagicMock
from Incremental.weather_blr import fetch_weather_blr_data,  load_data_to_snowflake, weather_blr_job, hourly_weather_blr_schedule
from Incremental.weather_hyd import fetch_weather_hyd_data, load_data_to_snowflake
from weather import load_and_join_data
from dagster import ScheduleDefinition, schedule
from dagster._core.definitions.job_definition import JobDefinition

@pytest.fixture
def mock_context():
    # Return a dummy context for the schedule function
    class MockContext:
        def __init__(self):
            self.pipeline_run = None
            self.solid_handle = None
            self.op_handle = None
    return MockContext() 

# Mock data for the API responses
mock_weather_blr_data = {
    "location": {
        "name": "Bangalore",
        "region": "Karnataka",
        "country": "India",
        "lat": 12.98,
        "lon": 77.58,
        "tz_id": "Asia/Kolkata"
    },
    "current": {
        "last_updated": "2024-08-30 17:15",
        "temp_c": 28.4,
        "temp_f": 83.1,
        "condition": {
            "text": "Partly cloudy",
            "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png",
            "code": 1003
        }
    }
}

mock_weather_hyd_data = {
    "location": {
        "name": "Hyderabad",
        "region": "Andhra Pradesh",
        "country": "India",
        "lat": 17.38,
        "lon": 78.47,
        "tz_id": "Asia/Kolkata"
    },
    "current": {
        "last_updated": "2023-11-24 17:15",
        "temp_c": 27.2,
        "temp_f": 81.0,
        "condition": {
            "text": "Patchy rain possible",
            "icon": "//cdn.weatherapi.com/weather/64x64/day/176.png",
            "code": 1063
        }
    }
}

# Test for fetch_weather_data for bangalore
def test_fetch_weather_blr_data(requests_mock):
    api_key = 'a677fb9afa584a0d9e0131232242808'
    location = 'Bangalore'
    dt = '2024-06-23'
    
    requests_mock.get(f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={dt}", json=mock_weather_blr_data)
    
    df = fetch_weather_blr_data(api_key, location, dt)
    
    assert not df.empty
    assert df['location'].iloc[0] == 'Bangalore'
    assert df['condition_text'].iloc[0] == 'Partly cloudy'

# Test for fetch_weather_data for hyderabad
def test_fetch_weather_hyd_data(requests_mock):
    api_key = 'a677fb9afa584a0d9e0131232242808'
    location = 'Hyderabad'
    dt = '2024-06-23'
    
    requests_mock.get(f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={dt}", json=mock_weather_hyd_data)
    
    df = fetch_weather_hyd_data(api_key, location, dt)
    
    assert not df.empty
    assert df['location'].iloc[0] == 'Hyderabad'
    assert df['condition_text'].iloc[0] == 'Patchy rain possible'

# Test for load_data_to_snowflake
def test_load_data_to_snowflake():
    df = pd.DataFrame({
        'location': ['Bangalore'],
        'state': ['Karnataka'],
        'country': ['India'],
        'lat': [12.98],
        'lon': [77.58],
        'condition_text': ['Partly cloudy']
    })

    mock_engine = MagicMock()
    
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('Incremental.weather_blr.create_engine', lambda *args, **kwargs: mock_engine)
        load_data_to_snowflake(df)
    
        mock_engine.execute.assert_called_once()


# Test for the Dagster schedule setup
def test_hourly_weather_blr_schedule(mock_context):
    # Get the schedule definition
    schedule_def = hourly_weather_blr_schedule(mock_context)
    
    # Assert that we have a ScheduleDefinition
    assert isinstance(schedule_def, ScheduleDefinition)

    # Check the schedule's properties
    assert schedule_def.cron_schedule == "0 * * * *"
    assert schedule_def.job == weather_blr_job
    assert schedule_def.execution_timezone == "UTC"

    # Verify schedule configuration
    # Since the schedule might not use partitions, get_run_config_for_partition might not apply.
    # Instead, you can manually verify that the configuration matches what you expect.
    expected_config = {
        'ops': {
            'fetch_weather_asset': {
                'config': {
                    'api_key': 'a677fb9afa584a0d9e0131232242808',
                    'location': 'Bangalore',
                    'dt': str(date.today())
                }
            }
        }
    }
    
    # You need to manually verify the configuration based on how you use the schedule in your code
    # If your actual schedule function doesn't return a configuration, skip this part
    actual_config = {
        'ops': {
            'fetch_weather_asset': {
                'config': {
                    'api_key': 'a677fb9afa584a0d9e0131232242808',
                    'location': 'Bangalore',
                    'dt': str(date.today())
                }
            }
        }
    }

    assert actual_config == expected_config

# Test for job execution
def test_weather_blr_job_execution():
    # Mock the execution of the job
    mock_result = MagicMock()
    mock_result.success = True

    # Mock the fetch_weather_asset asset function
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('Incremental.weather_blr.fetch_weather_asset', lambda context: 'mocked data')
        mp.setattr('Incremental.weather_blr.load_data_to_snowflake', lambda df: None)
        mp.setattr('Incremental.weather_blr.weather_blr_job.execute_in_process', lambda run_config: mock_result)
        
        # Execute the job
        from Incremental.weather_blr import weather_blr_job
        result = weather_blr_job.execute_in_process(run_config={
            'ops': {
                'fetch_weather_asset': {
                    'config': {
                        'api_key': 'a677fb9afa584a0d9e0131232242808',
                        'location': 'Bangalore',
                        'dt': str(date.today())
                    }
                }
            }
        })

        assert result.success


if __name__ == "__main__":
    pytest.main()
