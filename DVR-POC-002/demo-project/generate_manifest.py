import os

def generate_manifest(directory, ts_files):
    manifest_content = "#EXTM3U\n"
    manifest_content += "#EXT-X-VERSION:3\n"
    manifest_content += "#EXT-X-TARGETDURATION:7\n"
    # this sequence you might need to update
    manifest_content += "#EXT-X-MEDIA-SEQUENCE:0\n"

    for ts_file in ts_files:
        manifest_content += "#EXTINF:6.00600,\n"
        manifest_content += f"{ts_file}\n"
    
    manifest_content += "#EXT-X-ENDLIST\n"

    manifest_path = os.path.join(directory, 'playlist.m3u8')
    with open(manifest_path, 'w') as file:
        file.write(manifest_content)

    return manifest_path
