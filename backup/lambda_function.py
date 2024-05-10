import json

def lambda_handler(event, context):
    # For verification purpose I put this as a static url in lambda method
    hls_url = "https://live-par-2-cdn-alt.livepush.io/live/bigbuckbunnyclip/index.m3u8"

    return {
        'statusCode': 200,
        'body': json.dumps({'hlsUrl': hls_url})
    }
