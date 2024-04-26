document.addEventListener('DOMContentLoaded', function() {
  var videoPlayer = document.getElementById('videoPlayer');
  var playButton = document.getElementById('playButton');
  var stopButton = document.getElementById('stopButton');
  var streamUrlInput = document.getElementById('streamUrl');
  var lastPauseTime = 0;

  var hls = new Hls();

  // Check if HLS is supported
  if (Hls.isSupported()) {
    hls.attachMedia(videoPlayer);
    hls.on(Hls.Events.MEDIA_ATTACHED, function () {
      console.log('Video and HLS.js are now bound together!');
      hls.on(Hls.Events.MANIFEST_PARSED, function () {
        console.log('Manifest parsed and loaded');
      });
    });
  } else if (videoPlayer.canPlayType('application/vnd.apple.mpegurl')) {
    // HLS is natively supported in Safari
    videoPlayer.addEventListener('canplay', function() {
      videoPlayer.play();
    });
  }

  streamUrlInput.addEventListener('change', function() {
    var url = streamUrlInput.value;
    if (Hls.isSupported()) {
      hls.loadSource(url);
      hls.on(Hls.Events.LEVEL_LOADED, function () {
        console.log('Level loaded');
      });
    } else {
      videoPlayer.src = url; // Directly load into video element for natively supported browsers
    }
    stopButton.disabled = false;
  });

  videoPlayer.onpause = function() {
    lastPauseTime = Date.now();
  };

  videoPlayer.onplay = function() {
    if (lastPauseTime) {
      var currentTime = Date.now();
      var timeDiff = currentTime - lastPauseTime;

      if (timeDiff > 120000) { // More than 2 minutes
        console.log('Jumping to live as the pause was longer than 2 minutes.');
        if (hls.liveSyncPosition) {
          videoPlayer.currentTime = hls.liveSyncPosition; // Set to the live sync position for HLS streams
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
