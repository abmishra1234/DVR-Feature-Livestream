import os
from datetime import datetime, timedelta

# Directory where the TS and VTT files are stored
DOWNLOAD_DIR = r"C:\Users\abmis\OneDrive\Desktop\POC\LiveStream-Pause-Feature\DVR-POC-002\hls-download\hls_data\eng"

# Function to parse the timestamp from the filename
def parse_time_from_filename(filename):
    """
    Extracts the time from the given filename and combines it with the current date.

    Assumes the filename format is playlist_webvtt_HHmmss__sequence.vtt
    Adjust the parsing logic based on your actual filename format.
    """
    try:
        base_name = os.path.basename(filename)
        time_str = base_name.split('_')[2]  # Extract the HHMMSS part
        time_obj = datetime.strptime(time_str, "%H%M%S").time()
        
        # Combine with the current date
        now = datetime.now()
        timestamp = datetime.combine(now.date(), time_obj)
        
        # Ensure the timestamp is in the past or present
        if timestamp > now:
            timestamp -= timedelta(days=1)
            
        return timestamp
    except (IndexError, ValueError) as e:
        print(f"Error parsing time from filename '{filename}': {e}")
        return None

# Function to parse the sequence number from the filename
def parse_sequence_from_filename(filename):
    """
    Extracts the sequence number from the given filename.

    Assumes the filename format is playlist_webvtt_HHmmss__sequence.vtt
    Adjust the parsing logic based on your actual filename format.
    """
    try:
        base_name = os.path.basename(filename)
        sequence_str = base_name.split('__')[1].split('.')[0]
        return int(sequence_str)
    except (IndexError, ValueError) as e:
        print(f"Error parsing sequence from filename '{filename}': {e}")
        return None

# Function to generate the manifest file
def generate_manifest():
    """
    Generates an HLS manifest file with the latest 10 segments.

    Returns the path to the generated manifest file.
    """
    try:
        if not os.path.exists(DOWNLOAD_DIR):
            print(f"Directory does not exist: {DOWNLOAD_DIR}")
            return None

        # Get the list of VTT files
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.vtt')]

        if not files:
            print("No VTT files found in the directory.")
            return None

        # Sort files by sequence number
        files.sort(key=lambda x: parse_sequence_from_filename(x))

        # Debugging output
        print(f"Sorted files: {files}")

        # Get the latest 10 segments
        latest_segments = files[-10:]

        # Ensure there are at least 10 segments
        if len(latest_segments) < 10:
            print("Not enough segments to generate manifest.")
            return None

        # Calculate the program date-time for the first segment
        first_segment = latest_segments[0]
        first_segment_timestamp = parse_time_from_filename(first_segment)

        if not first_segment_timestamp:
            print("Error: Invalid timestamp in the first segment.")
            return None

        # Generate the manifest content
        manifest_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "#EXT-X-TARGETDURATION:7",
            f"#EXT-X-MEDIA-SEQUENCE:{parse_sequence_from_filename(first_segment)}",
            f"#EXT-X-PROGRAM-DATE-TIME:{first_segment_timestamp.isoformat()}Z"
        ]

        for segment in latest_segments:
            segment_duration = 6.00600  # Assuming each segment is approximately 6 seconds
            manifest_lines.append(f"#EXTINF:{segment_duration},")
            manifest_lines.append(segment)

        manifest_content = "\n".join(manifest_lines)

        # Save the manifest file      
        
        manifest_path = os.path.join(DOWNLOAD_DIR, "playlist.m3u8")
        with open(manifest_path, 'w') as manifest_file:
            manifest_file.write(manifest_content)

        print(f"Manifest file generated at: {manifest_path}")
        return manifest_path

    except Exception as e:
        print(f"Error generating manifest: {e}")
        return None

# Example usage
manifest_path = generate_manifest()

