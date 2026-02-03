const socket = io();
const band = document.getElementById('band');
const countdown = document.getElementById('countdown');
const progressBar = document.getElementById('progress-bar');
const nextBandContainer = document.getElementById('next-band');
const nextBandName = document.getElementById('next-band-name');

let totalSeconds = 0;
let currentSeconds = 0;
let warn_orange = 300;
let warn_red = 60;
let nextMode = false;

document.addEventListener("DOMContentLoaded", () => {
  fetch('/status')
    .then(response => response.json())
    .then(data => {
      if (data.status === "playing") {
        band.textContent = data.band;
        nextMode = false;
        totalSeconds = data.remaining;
        currentSeconds = data.remaining;
        let min = String(Math.floor(data.remaining / 60)).padStart(2, '0');
        let sec = String(Math.floor(data.remaining % 60)).padStart(2, '0');
        countdown.textContent = `${min}:${sec}`;
        let bar = document.getElementById("progress-bar");
        bar.style.display = "block";
        bar.style.width = "100%";
        bar.style.backgroundColor = "green";
      } else if (data.status === "finished") {
        band.textContent = "Fertig für heute";
        countdown.textContent = "";
        let barContainer = document.querySelector(".bar-container");
        if (barContainer) barContainer.remove();
      } else {
        band.textContent = "STAGETIMER";
        countdown.textContent = "";
        let barContainer = document.querySelector(".bar-container");
        if (barContainer) barContainer.remove();
      }
    });
});

socket.on('band_update', (data) => {
  band.textContent = data.band;
  nextMode = false;
  totalSeconds = data.remaining;
  currentSeconds = data.remaining;
  
  // Verstecke die "Nächste Band" Anzeige während eine Band spielt
  nextBandContainer.style.display = 'none';
});

socket.on('time_update', (data) => {
  const remaining = data.remaining;
  const minutes = Math.floor(remaining / 60);
  const seconds = Math.floor(remaining % 60);
  
  countdown.textContent = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  
  // Aktualisiere die Fortschrittsleiste
  const percentage = (remaining / totalSeconds) * 100;
  progressBar.style.width = `${percentage}%`;
  
  // Setze Warnfarben
  progressBar.classList.remove('warning', 'danger');
  if (minutes <= data.warn_red) {
    progressBar.classList.add('danger');
  } else if (minutes <= data.warn_orange) {
    progressBar.classList.add('warning');
  }
});

socket.on('next_band', (data) => {
  if (data.band) {
    nextBandName.textContent = `${data.band} (${data.start})`;
    nextBandContainer.style.display = 'block';
  } else {
    nextBandContainer.style.display = 'none';
  }
});