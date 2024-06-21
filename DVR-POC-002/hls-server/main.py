import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response, Request
from sortedcontainers import SortedDict
import asyncio
from concurrent.futures import ThreadPoolExecutor
import aiofiles  # Import aiofiles for asynchronous file operations

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure logger
log_filename = LOGS_DIR / f'hls_server_{datetime.now().strftime("%Y-%m-%d")}.log'
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s'
)

# Load configuration
try:
    with open('../hls-server/config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.error("Config file not found.")
    raise
except json.JSONDecodeError:
    logging.error("Error decoding config file.")
    raise

# These directories are defined in config file so modification is 
# easy to change as per need
SEGMENTS_DIR = Path(config["segments_dir"])
SUBTITLE_DIR_ENG = Path(config["subtitle_dir_eng"])

# Configurable time logging
ENABLE_TIME_LOGGING = config.get("enable_time_logging", False)

# In-memory data structures
class HLSData:
    def __init__(self):
        self.segment_data = SortedDict()  # Key: timestamp, Value: (sequence_number, ts_file, vtt_file)
        self.pause_data = {}  # Key: client_id, Value: pause_timestamp

hls_data = HLSData()

app = FastAPI()

# Thread pool for blocking IO operations
executor = ThreadPoolExecutor(max_workers=10)

def log_time(func):
    async def wrapper(*args, **kwargs):
        if ENABLE_TIME_LOGGING:
            start_time = datetime.utcnow()
        result = await func(*args, **kwargs)
        if ENABLE_TIME_LOGGING:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Execution time for {func.__name__}: {duration} seconds")
        return result
    return wrapper

class StreamHandler:
    def __init__(self, hls_data: HLSData):
        self.hls_data = hls_data

    async def start_streaming(self, url: str):
        try:
            await asyncio.get_event_loop().run_in_executor(executor, trigger_hls_downloader, url)

            # Initialize some segments for demo purposes
            initial_segments = [
                (datetime.utcnow(), 837223, "playlist_1920x1080_080950__837223.ts", "playlist_1920x1080_080950__837223.vtt"),
                # Add more segments as needed
            ]
            for timestamp, seq, ts_file, vtt_file in initial_segments:
                self.hls_data.segment_data[timestamp] = (seq, ts_file, vtt_file)
            logging.debug(f"Streaming started with URL: {url}")
            return {"status": "streaming started", "master_manifest_url": url}
        except Exception as e:
            logging.error(f"Error starting streaming: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_master_playlist(self):
        try:
            master_playlist_path = SEGMENTS_DIR / "playlist.m3u8"
            if not master_playlist_path.exists():
                raise HTTPException(status_code=404, detail="Master playlist not found")

            async with aiofiles.open(master_playlist_path, 'r') as file:
                master_playlist_content = await file.read()
            
            return Response(content=master_playlist_content, media_type="application/vnd.apple.mpegurl")
        except Exception as e:
            logging.error(f"Error fetching master playlist: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_resolution_playlist(self, resolution: str):
        try:
            resolution_dir = SEGMENTS_DIR / resolution
            if not resolution_dir.exists():
                logging.error(f"Resolution directory not found: {resolution_dir}")
                raise HTTPException(status_code=404, detail="Resolution directory not found")

            ts_files = sorted([f for f in os.listdir(resolution_dir) if f.endswith('.ts')])
            if not ts_files:
                logging.error(f"No segments found in the resolution directory: {resolution_dir}")
                raise HTTPException(status_code=404, detail="No segments found in the resolution directory")

            segments = ts_files[-10:]  # Get the last 10 segments
            playlist = []
            media_sequence = 0
            if segments:
                try:
                    media_sequence = int(segments[0].split("__")[1].split('.')[0])
                except Exception as e:
                    logging.error(f"Error parsing media sequence from file name: {segments[0]}, error: {e}")
                    raise

            playlist.append("#EXTM3U")
            playlist.append("#EXT-X-VERSION:3")
            playlist.append("#EXT-X-TARGETDURATION:7")
            playlist.append(f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}")
            # TBD - abinash.km, missing tag in this response :: #EXT-X-PROGRAM-DATE-TIME:2024-06-12T02:28:07.920Z
            for idx, ts_file in enumerate(segments):
                # if idx % 5 == 0:  # Example condition to insert ads every 5 segments
                #     playlist.append("#EXT-X-DISCONTINUITY")
                #     playlist.append("#EXTINF:10.000,")
                #     playlist.append("ad_segment.ts")
                playlist.append("#EXTINF:6.00600,")
                playlist.append(f"{ts_file}")
            resolution_playlist_content = "\n".join(playlist)
            return Response(content=resolution_playlist_content, media_type="application/vnd.apple.mpegurl")
        except HTTPException as he:
            raise he
        except Exception as e:
            logging.error(f"Error fetching resolution playlist: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_ts_file(self, resolution: str, timestamp: str, seq: int):
        try:
            resolution_dir = SEGMENTS_DIR / resolution
            ts_file_path = resolution_dir / f"playlist_{resolution}_{timestamp}__{seq}.ts"
            if not ts_file_path.exists():
                raise HTTPException(status_code=404, detail="TS file not found")

            async with aiofiles.open(ts_file_path, 'rb') as file:
                ts_file_content = await file.read()
            
            return Response(content=ts_file_content, media_type="video/MP2T")
        except Exception as e:
            logging.error(f"Error fetching TS file: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_subtitle_playlist(self):
        try:
            # abinash.km - TBD, as of now added for eng only, will improve
            subtitle_eng_dir = SUBTITLE_DIR_ENG
            if not subtitle_eng_dir.exists():
                logging.error(f"eng sub titles directory not found: {subtitle_eng_dir}")
                raise HTTPException(status_code=404, detail="eng sub titles directory not found")

            vtt_files = sorted([f for f in os.listdir(subtitle_eng_dir) if f.endswith('.vtt')])
            if not vtt_files:
                logging.error(f"No segments found in the resolution directory: {subtitle_eng_dir}")
                raise HTTPException(status_code=404, detail="No vtt files found in the subtitles directory")

            segments = vtt_files[-10:]  # Get the last 10 segments
            playlist = []
            media_sequence = 0
            if segments:
                try:
                    media_sequence = int(segments[0].split("__")[1].split('.')[0])
                except Exception as e:
                    logging.error(f"Error parsing media sequence from file name: {segments[0]}, error: {e}")
                    raise

            playlist.append("#EXTM3U")
            playlist.append("#EXT-X-VERSION:3")
            playlist.append("#EXT-X-TARGETDURATION:7")
            playlist.append(f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}")
            # TBD - abinash.km, missing tag in this response :: #EXT-X-PROGRAM-DATE-TIME:2024-06-12T02:28:07.920Z
            for idx, vtt_file in enumerate(segments):
                # if idx % 5 == 0:  # Example condition to insert ads every 5 segments
                #     playlist.append("#EXT-X-DISCONTINUITY")
                #     playlist.append("#EXTINF:10.000,")
                #     playlist.append("ad_segment.ts")
                playlist.append("#EXTINF:6.00600,")
                playlist.append(f"{vtt_file}")
            subtitle_playlist_content = "\n".join(playlist)
            return Response(content=subtitle_playlist_content, media_type="application/vnd.apple.mpegurl")
        except HTTPException as he:
            raise he
        except Exception as e:
            logging.error(f"Error fetching resolution playlist: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_vtt_file(self, timestamp: str, seq: int):
        try:
            # For now, just statically fixing eng subtitles but later
            # will do more dynamic multi-language support
            subtitles_eng_dir = SUBTITLE_DIR_ENG
            vtt_file_path = subtitles_eng_dir / f"playlist_webvtt_{timestamp}__{seq}.vtt"
            if not vtt_file_path.exists():
                raise HTTPException(status_code=404, detail="VTT file not found")

            async with aiofiles.open(vtt_file_path, 'rb') as file:
                vtt_file_content = await file.read()
            
            return Response(content=vtt_file_content, media_type="text/vtt")
        except Exception as e:
            logging.error(f"Error fetching VTT file: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def pause_stream(self, client_id: str, timestamp: datetime):
        try:
            self.hls_data.pause_data[client_id] = timestamp
            logging.debug(f"Stream paused for client {client_id} at {timestamp}")
            return {"status": "paused"}
        except Exception as e:
            logging.error(f"Error pausing stream: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def resume_stream(self, client_id: str):
        try:
            if client_id not in self.hls_data.pause_data:
                raise HTTPException(status_code=404, detail="Client not found")
            
            timestamp = self.hls_data.pause_data[client_id]
            segments = self.hls_data.segment_data.irange(minimum=timestamp, maximum=timestamp + timedelta(minutes=30))
            
            playlist = []
            for ts, (seq, ts_file, vtt_file) in segments:
                playlist.append({"sequence_number": seq, "ts_file": ts_file, "vtt_file": vtt_file})
            
            logging.debug(f"Stream resumed for client {client_id} from {timestamp}")
            return {"playlist": playlist}
        except Exception as e:
            logging.error(f"Error resuming stream: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def cleanup_data(self):
        try:
            threshold_time = datetime.utcnow() - timedelta(minutes=35)
            old_segments = self.hls_data.segment_data.keys().irange(maximum=threshold_time)
            
            for key in list(old_segments):
                del self.hls_data.segment_data[key]
            
            logging.debug("Old segments cleaned up")
            return {"status": "cleaned"}
        except Exception as e:
            logging.error(f"Error cleaning up data: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

def trigger_hls_downloader(url: str):
    # Implement the function to trigger the HLS Downloader service
    # For example, sending a request to the HLS Downloader service API
    pass

# Initialize StreamHandler
stream_handler = StreamHandler(hls_data)

@app.post("/start")
async def start_streaming(url: str):
    return await stream_handler.start_streaming(url)

@app.get("/playlist.m3u8")
async def get_master_playlist():
    return await stream_handler.get_master_playlist()

@app.get("/playlist_webvtt.m3u8")
async def get_subtitle_playlist():
    return await stream_handler.get_subtitle_playlist()

@app.get("/playlist_{resolution}.m3u8")
async def get_resolution_playlist(resolution: str):
    return await stream_handler.get_resolution_playlist(resolution)

@app.get("/playlist_{resolution}_{timestamp}__{seq}.ts")
async def get_ts_file(resolution: str, timestamp: str, seq: int):
    return await stream_handler.get_ts_file(resolution, timestamp, seq)

@app.get("/playlist_webvtt_{timestamp}__{seq}.vtt")
async def get_vtt_file(timestamp: str, seq: int):
    return await stream_handler.get_vtt_file(timestamp, seq)

@app.post("/pause")
async def pause_stream(client_id: str, timestamp: datetime):
    return await stream_handler.pause_stream(client_id, timestamp)

@app.post("/resume")
async def resume_stream(client_id: str):
    return await stream_handler.resume_stream(client_id)

@app.post("/cleanup")
async def cleanup_data():
    return await stream_handler.cleanup_data()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
