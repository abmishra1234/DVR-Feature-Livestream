from fastapi import FastAPI, HTTPException
from pathlib import Path
import os
from datetime import datetime, timedelta
import logging

app = FastAPI()

# Configuration
SEGMENTS_DIR = Path("../hls-download/hls_data")
SEGMENT_DURATION = 6  # Duration of each segment in seconds
MAX_PAUSE_DURATION = 30  # Max pause duration in minutes

# For demonstration, using a simple dictionary to store paused positions and timestamps
paused_positions = {}

def get_segments(resolution: str) -> list:
    """Returns a list of all segment files for a given resolution sorted by name."""
    resolution_dir = SEGMENTS_DIR / resolution
    if not resolution_dir.exists():
        raise HTTPException(status_code=404, detail=f"Resolution directory {resolution} not found.")
    return sorted(resolution_dir.glob('*.ts'), key=lambda x: x.name)

def get_subtitle_segments() -> list:
    """Returns a list of all subtitle files sorted by name."""
    subtitle_dir = SEGMENTS_DIR / 'eng'
    if not subtitle_dir.exists():
        raise HTTPException(status_code=404, detail="Subtitle directory 'eng' not found.")
    return sorted(subtitle_dir.glob('*.vtt'), key=lambda x: x.name)

@app.get("/manifest/{resolution}")
async def get_manifest(resolution: str, pause_id: str = None):
    """Generate and return the HLS manifest dynamically based on pause position."""
    try:
        segment_files = get_segments(resolution)
        subtitle_files = get_subtitle_segments()
        start_index = max(0, len(segment_files) - 10)  # Default to the latest 10 segments

        if pause_id and pause_id in paused_positions:
            pause_info = paused_positions[pause_id]
            paused_time = pause_info['timestamp']
            current_time = datetime.now()

            if current_time - paused_time < timedelta(minutes=MAX_PAUSE_DURATION):
                pause_position = pause_info['segment']
                start_index = max(0, pause_position - 1)

        end_index = min(start_index + 10, len(segment_files))
        segments_to_serve = segment_files[start_index:end_index]
        subtitles_to_serve = subtitle_files[start_index:end_index] if len(subtitle_files) >= end_index else []

        manifest_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
        manifest_content += f"#EXT-X-TARGETDURATION:{SEGMENT_DURATION}\n"
        manifest_content += f"#EXT-X-MEDIA-SEQUENCE:{start_index}\n"
        
        for seg in segments_to_serve:
            manifest_content += f"#EXTINF:{SEGMENT_DURATION},\n{seg.name}\n"

        if subtitles_to_serve:
            manifest_content += "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE=\"en\",URI=\"subtitles.m3u8\"\n"

        return manifest_content
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/subtitles.m3u8")
async def get_subtitle_manifest():
    """Generate and return the subtitle manifest."""
    try:
        subtitle_files = get_subtitle_segments()
        start_index = max(0, len(subtitle_files) - 10)  # Default to the latest 10 subtitles

        end_index = min(start_index + 10, len(subtitle_files))
        subtitles_to_serve = subtitle_files[start_index:end_index]

        manifest_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
        manifest_content += f"#EXT-X-TARGETDURATION:{SEGMENT_DURATION}\n"
        manifest_content += f"#EXT-X-MEDIA-SEQUENCE:{start_index}\n"

        for sub in subtitles_to_serve:
            manifest_content += f"#EXTINF:{SEGMENT_DURATION},\n{sub.name}\n"

        return manifest_content
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pause/{pause_id}")
async def pause_stream(pause_id: str, resolution: str):
    """Store the current position of the stream when paused."""
    segment_files = get_segments(resolution)
    if not segment_files:
        raise HTTPException(status_code=404, detail="No segments found.")
    current_segment = len(segment_files) - 1
    paused_positions[pause_id] = {
        "segment": current_segment,
        "timestamp": datetime.now(),
        "resolution": resolution
    }
    return {"pause_id": pause_id, "paused_at_segment": current_segment}

@app.post("/resume/{pause_id}")
async def resume_stream(pause_id: str):
    """Resume the stream from the last paused position or live if paused too long."""
    if pause_id not in paused_positions:
        raise HTTPException(status_code=404, detail="Pause ID not found.")
    # Return the manifest which starts from the paused segment or live position
    pause_info = paused_positions[pause_id]
    resolution = pause_info['resolution']
    return await get_manifest(resolution, pause_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
