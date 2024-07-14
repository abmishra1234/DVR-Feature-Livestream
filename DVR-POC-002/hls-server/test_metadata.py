# test_metadata.py

from fastapi.testclient import TestClient
from main import app
import pytest #type: ignore
import subprocess
import time
from datetime import datetime, timedelta


@pytest.fixture(scope="module")
def test_client():
    # Start the server as a subprocess
    server_process = subprocess.Popen(["uvicorn", "main:app", "--reload"])
    
    # Allow some time for the server to start
    time.sleep(5)
    
    client = TestClient(app)
    '''
        The code before yield runs before the test function is executed (setup).
        The code after yield (if any) runs after the test function has completed (teardown).
    '''
    yield client
    
    # Terminate the server after tests
    server_process.terminate()

#############################
# For adding the tsmetadata 
#############################
def generate_random_tsmetadata(sequence_number, start_time, duration):
    end_time = start_time + timedelta(seconds=duration)  # Calculate end time
    metadata = {
        "resolution": "1920x1080",
        "date": start_time.date().isoformat(),
        "start_timestamp": start_time.strftime("%H:%M:%S.%f")[:-3],  # Format like '13:40:43.104'
        "sequence_number": sequence_number,
        "duration": duration,
        "ts_file": f"segment_{sequence_number}.ts"
    }
    return metadata, end_time

def test_add_tsmetadata(test_client):
    start_time = datetime.strptime("13:40:43.104", "%H:%M:%S.%f")  # Starting time
    duration = 6.006  # Fixed duration in seconds

    for i in range(1, 501):  # Generating and adding 500 records
        metadata, end_time = generate_random_tsmetadata(i, start_time, duration)
        response = test_client.post("/add_tsmetadata", json=metadata)
        assert response.status_code == 200
        assert response.json() == {"status": "TS metadata added successfully"}
        start_time = end_time + timedelta(milliseconds=1)  # Update start_time for next segment

def generate_random_vttmetadata(sequence_number, start_time, duration):
    end_time = start_time + timedelta(seconds=duration)  # Calculate end time
    metadata = {
        "language": "eng",
        "date": start_time.date().isoformat(),
        "start_timestamp": start_time.strftime("%H:%M:%S.%f")[:-3],  # Format like '13:40:43.104'
        "sequence_number": sequence_number,
        "duration": duration,
        "vtt_file": f"segment_{sequence_number}.vtt"
    }
    return metadata, end_time

def test_add_vttmetadata(test_client):
    start_time = datetime.strptime("13:40:43.104", "%H:%M:%S.%f")  # Starting time
    duration = 6.006  # Fixed duration in seconds

    for i in range(1, 501):  # Generating and adding 500 records
        metadata, end_time = generate_random_vttmetadata(i, start_time, duration)
        response = test_client.post("/add_vttmetadata", json=metadata)
        assert response.status_code == 200
        assert response.json() == {"status": "VTT metadata added successfully"}
        start_time = end_time + timedelta(milliseconds=1)  # Update start_time for next segment

'''

def test_remove_tsmetadata(test_client):
    response = test_client.delete("/remove_tsmetadata/1920x1080/1")
    assert response.status_code == 200
    assert response.json() == {"status": "TS metadata removed successfully"}

def test_remove_vttmetadata(test_client):
    response = test_client.delete("/remove_vttmetadata/eng/1")
    assert response.status_code == 200
    assert response.json() == {"status": "VTT metadata removed successfully"}

def test_get_live_tsplaylist(test_client):
    response = test_client.get("/get_live_tsplaylist/1920x1080?max_segments=10")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 10

def test_get_dvr_tsplaylist(test_client):
    response = test_client.get("/get_dvr_tsplaylist/1920x1080?date=2024-07-01&timestamp=12:00:00.000&max_segments=10")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 10

def test_get_live_vttplaylist(test_client):
    response = test_client.get("/get_live_vttplaylist/eng?max_segments=10")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 10

def test_get_dvr_vttplaylist(test_client):
    response = test_client.get("/get_dvr_vttplaylist/eng?date=2024-07-01&timestamp=12:00:00.000&max_segments=10")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 10

# Additional tests

def test_get_master_playlist(test_client):
    response = test_client.get("/playlist.m3u8")
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        assert response.headers["content-type"] == "application/vnd.apple.mpegurl"

def test_get_resolution_playlist(test_client):
    response = test_client.get("/playlist_1920x1080.m3u8")
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        assert response.headers["content-type"] == "application/vnd.apple.mpegurl"
'''        

if __name__ == "__main__":
    pytest.main()
