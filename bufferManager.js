// bufferManager.js
class BufferManager {
  constructor(videoPlayer) {
    this.videoPlayer = videoPlayer;
    this.buffer = [];
    this.maxBufferLength = 1800000; // 30 minutes in milliseconds
    this.initEvents();
  }

  initEvents() {
    this.videoPlayer.on('timeupdate', () => this.manageBuffer());
    this.videoPlayer.on('pause', () => this.startTime = Date.now());
    this.videoPlayer.on('play', () => this.checkBufferStatus());
  }

  manageBuffer() {
    if (this.videoPlayer.paused()) return;
    let currentTime = this.videoPlayer.currentTime();
    this.buffer.push({ time: currentTime, blob: new Blob([this.videoPlayer.currentSegment]) });

    // Clean up the buffer to maintain the rolling 2-minute window
    while (this.buffer.length > 0 && (currentTime - this.buffer[0].time) * 1000 > this.maxBufferLength) {
      this.buffer.shift();
    }
  }

  checkBufferStatus() {
    let pauseDuration = Date.now() - this.startTime;
    if (pauseDuration > this.maxBufferLength) {
      this.jumpToLive();
    }
  }

  jumpToLive() {
    this.videoPlayer.currentTime(this.videoPlayer.duration());
    this.displayLiveIndicator();
    this.buffer = []; // Clear buffer as we are jumping to live
  }

  displayLiveIndicator() {
    let liveIndicator = document.getElementById('liveIndicator');
    liveIndicator.style.visibility = 'visible';
    setTimeout(() => {
      liveIndicator.style.visibility = 'hidden';
    }, 3000);
  }
}

// Ensure to include this script after loading script.js and instantiate BufferManager with the videoPlayer instance.
