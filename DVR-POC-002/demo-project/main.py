from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os
from read_config import read_config
#from get_ts_files import get_ts_files
from generate_manifest import generate_manifest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from get_ts_files import TSFileBatcher

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
def read_root():
    return {"Hello": "World"}

# Read config
directory = read_config()
tsbatch = TSFileBatcher(directory)

@app.get("/playlist.m3u8")
async def get_manifest():
    ts_files = tsbatch.get_next_batch()#get_ts_files(directory)
    if not ts_files:
        raise HTTPException(status_code=404, 
            detail="No TS files found in directory.")
    
    manifest_path = generate_manifest(directory, ts_files)
    return FileResponse(manifest_path, 
        media_type='application/vnd.apple.mpegurl')

@app.get("/{filename}")
async def get_ts(filename: str):
    file_path = os.path.join(directory, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='video/MP2T')
    raise HTTPException(status_code=404, detail="File not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
