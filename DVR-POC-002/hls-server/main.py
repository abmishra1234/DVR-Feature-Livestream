# main.py

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response, Request, Depends
from sortedcontainers import SortedDict
import asyncio
from concurrent.futures import ThreadPoolExecutor
import aiofiles  # Import aiofiles for asynchronous file operations
from logging.handlers import TimedRotatingFileHandler
from pydantic import BaseModel
from datetime import datetime

from ts_metadata_manager import TSMetadataManager
from vtt_metadata_manager import VTTMetadataManager

from fastapi.middleware.cors import CORSMiddleware

####################Loading of configuration start here##########
# Load configuration
try:
    with open('config.json', 'r') as config_file:
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

# read the master playlist name
MASTER_PLAYLIST_NAME = config.get("master_playlist_name", "")


####################Loading of configuration ends here##########

###################Logging Configeration Starts here############

# Create logs directory if it doesn't exist
#LOGS_DIR = Path("logs")
LOGS_DIR = Path(config["logging_dir"])
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Configure logger
log_filename = LOGS_DIR / 'hls_server.log'
handler = TimedRotatingFileHandler(
    log_filename,
    when='midnight',
    interval=1,
    # Keep last 30 days logs as backup
    backupCount=30  # Number of backup files to keep, adjust as needed
)
handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

###################Logging Configeration Ends here############

app = FastAPI()

######################Handling for CORS Calling Error#########
# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
######################Handling Ends here#####################

# Thread pool for blocking IO operations, go with default number of workers
executor = ThreadPoolExecutor(max_workers=10)

import logging
from datetime import datetime, timezone

ENABLE_TIME_LOGGING = True

def log_time(func):
    async def wrapper(*args, **kwargs):
        if ENABLE_TIME_LOGGING:
            start_time = datetime.now(timezone.utc)
        result = await func(*args, **kwargs)
        if ENABLE_TIME_LOGGING:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Execution time for {func.__name__}: {duration} seconds\n\n")
        return result
    return wrapper
    
# Initialize the Metadata Managers
ts_manager = TSMetadataManager(logger)
vtt_manager = VTTMetadataManager(logger)

# Models for the API requests
class TSMetadataRequest(BaseModel):
    resolution: str
    date: str
    start_timestamp: str
    sequence_number: int
    duration: float
    ts_file: str

class VTTMetadataRequest(BaseModel):
    language: str
    date: str
    start_timestamp: str
    sequence_number: int
    duration: float
    vtt_file: str

# Dependency for TSMetadataManager
def get_ts_manager():
    return ts_manager

# Dependency for VTTMetadataManager
def get_vtt_manager():
    return vtt_manager
    

class StreamHandler:
    def __init__(self):
        logger.info("StreamHandler -> init method is called!!!")
    async def start_streaming(self, url: str):
        ''' Comment block:
            I 'm not sure the purpose of the start streaming as 
            of now so let's comeback later   
        '''
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
            logger.info("'/playlist.m3u8' api is Entered!!!")
            master_playlist_path = SEGMENTS_DIR / MASTER_PLAYLIST_NAME #"playlist.m3u8"
            if not master_playlist_path.exists():
                logger.info("'/playlist.m3u8' api is Exited!!!\n")
                raise HTTPException(status_code=404, detail="Master playlist not found")

            logger.info(" master playlist content preperation started")                

            async with aiofiles.open(master_playlist_path, 'r') as file:
                master_playlist_content = await file.read()

            logger.info(" master playlist content preperation successfully completed!!!")                
            
            logger.info("'/playlist.m3u8' api now Exited!!!\n")
            return Response(content=master_playlist_content, media_type="application/vnd.apple.mpegurl")
        except Exception as e:
            logger.error(f"Error fetching master playlist: {e}")
            logger.info("'/playlist.m3u8' api is Exited!!!\n")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_resolution_playlist(self, resolution: str):
        '''
            The below code is for the handling of resolution based
            playlist fetching
        '''
        logger.info(f'get_resolution_playlist is called for the resolution : {resolution}')
        try:
            resolution_dir = SEGMENTS_DIR / resolution
            logger.info(f'resolution_dir : {resolution_dir}')
            if not resolution_dir.exists():
                logger.error(f"Resolution directory not found: {resolution_dir}")
                raise HTTPException(status_code=404, detail="Resolution directory not found")

            '''
                For now we are sorting the complete list everytime which is not the good design
                and later we will maintain our dynamic container in sorted order and we don't
                need to sort explicitly, which is the performance booster for this api
                ::ts_files, is the sorted list of files
            '''
            ts_files = sorted([f for f in os.listdir(resolution_dir) if f.endswith('.ts')])
            # Ensure we have at least 20 segments to choose the last 10 from -20th position
            if len(ts_files) < 20:
                logger.error(f"Not enough segments to get last 10 from the -20th position: {resolution_dir}")
                raise HTTPException(status_code=404, detail="Not enough segments to generate playlist")

            # Get the last 20 segments and then pick the last 10 from these
            last_20_segments = ts_files[-20:]
            segments = sorted(last_20_segments)[+10:]

            playlist = []
            
            media_sequence = 0
            # if segments is null that means we don't have any file in the perticular directory
            if segments:
                try:
                    #if this is getting called means that segments is not none
                    media_sequence = int(segments[0].split("__")[1].split('.')[0])
                except Exception as e:
                    logger.error(f"Error parsing media sequence from file name: {segments[0]}, error: {e}")
                    raise
            
            # print the media_sequence of the first ts file among the list files
            logger.info(f'The media_sequence number for the first sorted ts_file is : {media_sequence}')

            playlist.append("#EXTM3U")
            playlist.append("#EXT-X-VERSION:3")
            playlist.append("#EXT-X-TARGETDURATION:7")
            playlist.append(f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}")
            
            # TBD - abinash.km, missing tag in this response :: #EXT-X-PROGRAM-DATE-TIME:2024-06-12T02:28:07.920Z
            for idx, ts_file in enumerate(segments):
                playlist.append("#EXTINF:6.00600,")
                playlist.append(f"{ts_file}")
            resolution_playlist_content = "\n".join(playlist)
            return Response(content=resolution_playlist_content, media_type="application/vnd.apple.mpegurl")
        except HTTPException as he:
            logger.error(f"Error fetching resolution playlist: {he}")
            raise he
        except Exception as e:
            logger.error(f"Error fetching resolution playlist: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_ts_file(self, resolution: str, timestamp: str, seq: int):
        '''Block for downloading the ts file
            The below code is utility method designed for the handling of 
            ts file download
        '''
        # this method is for the download of perticular ts file
        logger.info(f'resolution: {resolution}, timestamp: {timestamp}, and seq: {seq}')
        try:
            resolution_dir = SEGMENTS_DIR / resolution
            ts_file_path = resolution_dir / f"playlist_{resolution}_{timestamp}__{seq}.ts"
            logger.info(f'ts_file_path: {ts_file_path}')
            if not ts_file_path.exists():
                logger.error(f'The requested {ts_file_path} does not exist')
                raise HTTPException(status_code=404, detail="TS file not found")

            async with aiofiles.open(ts_file_path, 'rb') as file:
                ts_file_content = await file.read()

            logger.info("The ts file download completed successfully!!!")            
            return Response(content=ts_file_content, media_type="video/MP2T")
        
        except Exception as e:
            logger.error(f"Error fetching TS file: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_subtitle_playlist(self):
        ''' Utility method for subtitle playlist manifest
            This Utility method is designed for the handling of 
            get_subtitle_playlist method
            Observations to improve the code ?
        '''
        logger.info("get_subtitle_playlist utility method is called!")
        try:
            # abinash.km - TBD, as of now added for eng only, will improve
            # as we get the more clarity and also when the actual metadata
            # dynamic storage is maintained
            subtitle_eng_dir = SUBTITLE_DIR_ENG
            logger.info(f'The subtitle_eng_dir = {subtitle_eng_dir}')

            if not subtitle_eng_dir.exists():
                logger.error(f"eng sub titles directory not found: {subtitle_eng_dir}")
                raise HTTPException(status_code=404, detail="eng sub titles directory not found")

            # The below code need to be changed after we able to implement
            # Dynamic metadata container for handling the metadat for *.ts and *.vtt
            vtt_files = sorted([f for f in os.listdir(subtitle_eng_dir) if f.endswith('.vtt')])

            if len(vtt_files) < 20:
                logger.error(f"Not enough segments to get last 10 from the -20th position: {subtitle_eng_dir}")
                raise HTTPException(status_code=404, detail="Not enough segments to generate playlist")

            # Get the last 20 segments and then pick the last 10 from these
            last_20_segments = vtt_files[-20:]
            segments = sorted(last_20_segments)[+10:]

            playlist = []
            '''
                If we are getting media_sequence as 0 , it means that 
                there is no *.vtt files in the directory
            '''
            media_sequence = 0
            if segments:
                try:
                    # I 'm trying to extract the sequence number of first .vtt file
                    # in the last 10 *.vtt file
                    media_sequence = int(segments[0].split("__")[1].split('.')[0])
                    logger.info(f'The media_sequence of first vtt file is : {media_sequence}')
                except Exception as e:
                    logging.error(f"Error parsing media sequence from file name: {segments[0]}, error: {e}")
                    raise

            playlist.append("#EXTM3U")
            playlist.append("#EXT-X-VERSION:3")
            playlist.append("#EXT-X-TARGETDURATION:7")
            playlist.append(f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}")
            ''' TBD - abinash.km, 
                missing tag in this response :: #EXT-X-PROGRAM-DATE-TIME:2024-06-12T02:28:07.920Z
                Please note that this tag definately required to be updated when you will complete the
                metadata storage container is ready and integerated with this code 
            '''

            for idx, vtt_file in enumerate(segments):
                playlist.append("#EXTINF:6.00600,")
                playlist.append(f"{vtt_file}")

            subtitle_playlist_content = "\n".join(playlist)
            logger.info("This method is completed successfully!")
            return Response(content=subtitle_playlist_content, 
                media_type="application/vnd.apple.mpegurl")

        except HTTPException as he:
            logger.error(f"Error fetching resolution playlist: {he}")
            raise he

        except Exception as e:
            logger.error(f"Error fetching resolution playlist: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @log_time
    async def get_vtt_file(self, timestamp: str, seq: int):
        ''' Function brief discription
            This below code is for downloading the vtt file
        '''
        logger.info(f'The timestamp: {timestamp}, and the seq: {seq}')
        try:
            ''' Block comment for the title language
                For now, just statically fixing eng subtitles but later
                will do more dynamic multi-language support as per the metadata 
                collected 
            '''
            subtitles_eng_dir = SUBTITLE_DIR_ENG
            vtt_file_path = subtitles_eng_dir / f"playlist_webvtt_{timestamp}__{seq}.vtt"
            logger.info(f'vtt_file_path: {vtt_file_path}')

            if not vtt_file_path.exists():
                logger.error(f'vtt_file_path: {vtt_file_path} doesnot exist')
                raise HTTPException(status_code=404, detail="VTT file not found")

            async with aiofiles.open(vtt_file_path, 'rb') as file:
                vtt_file_content = await file.read()
            
            logger.info("The requested vtt file download executed successfully!!!")
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

# not sure the purpose of below method
def trigger_hls_downloader(url: str):
    # Implement the function to trigger the HLS Downloader service
    # For example, sending a request to the HLS Downloader service API
    pass

# Initialize StreamHandler
logger.info("\n\n Start of hls-server!!!")
stream_handler = StreamHandler()

##################Utility method block ends here######

# This below api is created for the start of live stream
@app.post("/start")
async def start_streaming(url: str):
    return await stream_handler.start_streaming(url)

##########################

# The below code is for GET api for fetching the master playlist
@app.get("/playlist.m3u8")
async def get_master_playlist():
    return await stream_handler.get_master_playlist()

##########################

#This below code is for the GET '/playlist_webvtt.m3u8' fetching of webvtt playlist
@app.get("/playlist_webvtt.m3u8")
async def get_subtitle_playlist():
    logger.info("'/playlist_webvtt.m3u8' api is called!!!")
    return await stream_handler.get_subtitle_playlist()

##########################

#This below code is for the GET '/playlist_{resolution}.m3u8' fetching of webvtt playlist
@app.get("/playlist_{resolution}.m3u8")
async def get_resolution_playlist(resolution: str):
    logger.info("'/playlist_{resolution}.m3u8' is getting called!!!")
    return await stream_handler.get_resolution_playlist(resolution)

##########################

@app.get("/playlist_{resolution}_{timestamp}__{seq}.ts")
async def get_ts_file(resolution: str, timestamp: str, seq: int):
    logger.info("'/playlist_{resolution}_{timestamp}__{seq}.ts' this api is called!!!")
    return await stream_handler.get_ts_file(resolution, timestamp, seq)

@app.get("/playlist_webvtt_{timestamp}__{seq}.vtt")
async def get_vtt_file(timestamp: str, seq: int):
    logger.info("'/playlist_webvtt_{timestamp}__{seq}.vtt' api is called!!!")
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

#############newly added metadata handling of api's####################

# Adding the code for handling the Metadata for ts and vtt files
@app.post("/add_tsmetadata")
async def add_tsmetadata(request: TSMetadataRequest, 
    manager: TSMetadataManager = Depends(get_ts_manager)):
    try:
        manager.add_tsmetadata(request.resolution, request.date, request.start_timestamp, request.sequence_number, request.duration, request.ts_file)
        return {"status": "TS metadata added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding TS metadata: {e}")

@app.post("/add_vttmetadata")
async def add_vttmetadata(request: VTTMetadataRequest, 
    manager: VTTMetadataManager = Depends(get_vtt_manager)):
    try:
        manager.add_vttmetadata(request.language, request.date, request.start_timestamp, request.sequence_number, request.duration, request.vtt_file)
        return {"status": "VTT metadata added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding VTT metadata: {e}")

@app.delete("/remove_tsmetadata/{resolution}/{sequence_number}")
async def remove_tsmetadata(resolution: str, sequence_number: int, 
    manager: TSMetadataManager = Depends(get_ts_manager)):
    try:
        manager.remove_tsmetadata(resolution, sequence_number)
        return {"status": "TS metadata removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing TS metadata: {e}")

@app.delete("/remove_vttmetadata/{language}/{sequence_number}")
async def remove_vttmetadata(language: str, sequence_number: int, 
    manager: VTTMetadataManager = Depends(get_vtt_manager)):
    try:
        manager.remove_vttmetadata(language, sequence_number)
        return {"status": "VTT metadata removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing VTT metadata: {e}")

@app.get("/get_live_tsplaylist/{resolution}")
async def get_live_tsplaylist(resolution: str, max_segments: int = 10, 
    manager: TSMetadataManager = Depends(get_ts_manager)):
    try:
        data = manager.get_live_playlist(resolution, max_segments)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting live TS playlist: {e}")

@app.get("/get_dvr_tsplaylist/{resolution}")
async def get_dvr_tsplaylist(resolution: str, date: str, 
    timestamp: str, max_segments: int = 10, 
    manager: TSMetadataManager = Depends(get_ts_manager)):
    try:
        data = manager.get_dvr_playlist(resolution, date, timestamp, max_segments)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting DVR TS playlist: {e}")

@app.get("/get_live_vttplaylist/{language}")
async def get_live_vttplaylist(language: str, max_segments: int = 10, 
    manager: VTTMetadataManager = Depends(get_vtt_manager)):
    try:
        data = manager.get_live_playlist(language, max_segments)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting live VTT playlist: {e}")

@app.get("/get_dvr_vttplaylist/{language}")
async def get_dvr_vttplaylist(language: str, date: str, 
    timestamp: str, max_segments: int = 10, 
    manager: VTTMetadataManager = Depends(get_vtt_manager)):
    try:
        data = manager.get_dvr_playlist(language, date, timestamp, max_segments)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting DVR VTT playlist: {e}")

########newly added metadata handling of api's end here####################

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
