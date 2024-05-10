document.addEventListener('DOMContentLoaded', function() {
  var videoPlayer = document.getElementById('videoPlayer');
  var playButton = document.getElementById('playButton');
  var stopButton = document.getElementById('stopButton');
  var streamUrlInput = document.getElementById('streamUrl');
  var videoFileInput = document.getElementById('videoFile');
  var lastPauseTime = 0;
  var isLiveStream = false; // Flag to check if the current source is a live stream

  var hls = new Hls();

  // Function to initialize HLS or native HLS
  function setupLiveStream(url) {
    if (Hls.isSupported()) {
      hls.attachMedia(videoPlayer);
      hls.on(Hls.Events.MEDIA_ATTACHED, function() {
        console.log('Video and HLS.js are now bound together!');
        console.log("The value of url passed here ---> " + url);
        hls.loadSource(url);
        hls.on(Hls.Events.MANIFEST_PARSED, function() {
          console.log('Manifest parsed and loaded');
          videoPlayer.play();
        });
      });
    } else if (videoPlayer.canPlayType('application/vnd.apple.mpegurl')) {
      videoPlayer.src = url;
      videoPlayer.addEventListener('canplay', function() {
        videoPlayer.play();
      });
    }
    isLiveStream = true;
    stopButton.disabled = false;
  }

  // Live stream URL input change event - replaced with keyup to handle Enter key
  streamUrlInput.addEventListener('keyup', function(event) {
    if (event.key === "Enter") {  // Checks if the key pressed is 'Enter'
      setupLiveStream(streamUrlInput.value);
    }
  });

  // File input change event for MP4 files
  videoFileInput.addEventListener('change', function(event) {
    var file = event.target.files[0];
    if (file) {
      var url = URL.createObjectURL(file);
      videoPlayer.src = url;
      videoPlayer.play();
      isLiveStream = false; // It's an MP4 file, not a live stream
      stopButton.disabled = false;
    }
  });


  // New function to fetch stream URL from API
  function fetchStreamUrlFromAPI() {
    fetch('https://5k2487aafd.execute-api.ap-south-1.amazonaws.com/dev/stream-url')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();  // Convert the response to JSON
        })
        .then(data => {
            try {
                console.log("Initial API Response:", data);
                // Since 'body' is a stringified JSON, parse it again to get the actual object
                const bodyData = JSON.parse(data.body);
                console.log("Parsed Body Data:", bodyData);
                if (bodyData.hlsUrl && typeof bodyData.hlsUrl === 'string') {
                    let hlsUrl = bodyData.hlsUrl;
                    if (hlsUrl.endsWith('/')) {
                        hlsUrl = hlsUrl.slice(0, -1);
                    }
                    setupLiveStream(hlsUrl);
                } else {
                    console.error('hlsUrl not found in the body or is not a string');
                }
            } catch (error) {
                console.error('Error parsing JSON from body:', error);
            }
        })
        .catch(error => console.error('Error fetching HLS URL:', error));
}

  // Event listener for the new button
  newStreamButton.addEventListener('click', function() {
    fetchStreamUrlFromAPI();
  });


  videoPlayer.onpause = function() {
    lastPauseTime = Date.now();
  };

  videoPlayer.onplay = function() {
    if (isLiveStream && lastPauseTime) { // Apply this logic only for live streams
      var currentTime = Date.now();
      var timeDiff = currentTime - lastPauseTime;

      if (timeDiff > 120000) { // More than 2 minutes
        console.log('Jumping to live as the pause was longer than 2 minutes.');
        if (hls.liveSyncPosition) {
          videoPlayer.currentTime = hls.liveSyncPosition;
        }
      }
    }
  };

  playButton.onclick = function() {
    if (videoPlayer.paused) {
      videoPlayer.play();
    } else {
      videoPlayer.pause();
    }
  };

  stopButton.onclick = function() {
    videoPlayer.pause();
    videoPlayer.currentTime = 0;
    playButton.textContent = 'Play';
    stopButton.disabled = true;
  };
});
