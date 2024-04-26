document.addEventListener('DOMContentLoaded', function() {
  var videoPlayer = videojs('videoPlayer');
  var playButton = document.getElementById('playButton');
  var stopButton = document.getElementById('stopButton');
  var streamUrlInput = document.getElementById('streamUrl');
  var pauseTimerValue = document.getElementById('pauseTimerValue');
  var pauseTimerInterval;
  var pauseStartTime;

  function resetPauseTimer() {
    clearInterval(pauseTimerInterval);
    pauseTimerValue.textContent = "00:00:00";
    pauseTimerInterval = null;
  }

  function updatePauseTimer() {
    var elapsed = Date.now() - pauseStartTime;
    var date = new Date(elapsed);
    var hours = date.getUTCHours().toString().padStart(2, '0');
    var minutes = date.getUTCMinutes().toString().padStart(2, '0');
    var seconds = date.getUTCSeconds().toString().padStart(2, '0');
    pauseTimerValue.textContent = `${hours}:${minutes}:${seconds}`;
  }

  // Setup event listeners
  playButton.addEventListener('click', function() {
    if (videoPlayer.paused()) {
      videoPlayer.play();
    } else {
      videoPlayer.pause();
    }
  });

  stopButton.addEventListener('click', function() {
    videoPlayer.pause();
    videoPlayer.currentTime(0);
    playButton.textContent = 'Play';
    stopButton.disabled = true;
    resetPauseTimer();
  });

  streamUrlInput.addEventListener('input', function() {
    // Enable the stop button when a stream URL is entered
    stopButton.disabled = !streamUrlInput.value;
  });

  streamUrlInput.addEventListener('keydown', function(event) {
    if (event.key === 'Enter' && streamUrlInput.value) {
      var url = streamUrlInput.value;
      var type = videoPlayer.canPlayType('application/vnd.apple.mpegurl')
? 'application/x-mpegURL' : 'application/dash+xml';
videoPlayer.src({ src: url, type: type });
videoPlayer.play();
stopButton.disabled = false;
}
});

document.getElementById('videoFile').addEventListener('change', function(event) {
var file = event.target.files[0];
var url = URL.createObjectURL(file);
videoPlayer.src({ src: url, type: 'video/mp4' });
videoPlayer.play();
stopButton.disabled = false;
});

// Player event handlers
videoPlayer.on('play', function() {
playButton.textContent = 'Pause';
resetPauseTimer();
});

videoPlayer.on('pause', function() {
playButton.textContent = 'Play';
if (!videoPlayer.seeking()) {
pauseStartTime = Date.now();
pauseTimerInterval = setInterval(updatePauseTimer, 1000);
}
});

videoPlayer.on('ended', function() {
playButton.textContent = 'Play';
stopButton.disabled = true;
resetPauseTimer();
});

videoPlayer.on('error', function(event) {
console.error('An error occurred while playing the video:', event);
resetPauseTimer();
});
});