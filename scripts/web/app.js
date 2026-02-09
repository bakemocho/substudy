const SEEK_SECONDS = 5;
const CONTROL_TOGGLE_IDLE_MS = 2600;
const LYRIC_WHEEL_STEP_DELTA = 52;
const LYRIC_WHEEL_IDLE_RESET_MS = 260;

const state = {
  videos: [],
  sources: [],
  index: 0,
  shuffleMode: false,
  shuffleQueue: [],
  playbackHistory: [],
  historyPointer: -1,
  lyricReelActive: false,
  lyricReelIndex: -1,
  lyricWheelAccumulator: 0,
  lyricWheelResetTimer: null,
  cues: [],
  activeCueIndex: -1,
  currentTrackId: null,
  bookmarks: [],
  countdownTimer: null,
  countdownRemaining: 0,
  autoplayContinuous: localStorage.getItem("substudy.autoplay") !== "off",
  rangeStartMs: null,
  wheelLockUntil: 0,
  touchStartY: null,
  metaExpanded: false,
  controlsExpanded: localStorage.getItem("substudy.controls_expanded") === "true",
  controlsFadeTimer: null,
  jumpResults: [],
  jumpSelectedIndex: 0,
};

const elements = {
  sourceSelect: document.getElementById("sourceSelect"),
  jumpOpenBtn: document.getElementById("jumpOpenBtn"),
  autoplayToggle: document.getElementById("autoplayToggle"),
  shuffleToggle: document.getElementById("shuffleToggle"),
  videoPlayer: document.getElementById("videoPlayer"),
  phoneShell: document.getElementById("phoneShell"),
  subtitleOverlay: document.getElementById("subtitleOverlay"),
  lyricReelOverlay: document.getElementById("lyricReelOverlay"),
  lyricReelList: document.getElementById("lyricReelList"),
  countdownPanel: document.getElementById("countdownPanel"),
  countdownValue: document.getElementById("countdownValue"),
  cancelCountdownBtn: document.getElementById("cancelCountdownBtn"),
  videoMetaDrawer: document.getElementById("videoMetaDrawer"),
  metaTabBtn: document.getElementById("metaTabBtn"),
  metaPanel: document.getElementById("metaPanel"),
  metaPrimaryLine: document.getElementById("metaPrimaryLine"),
  metaSecondaryLine: document.getElementById("metaSecondaryLine"),
  controlsToggleBtn: document.getElementById("controlsToggleBtn"),
  mainControls: document.getElementById("mainControls"),
  prevBtn: document.getElementById("prevBtn"),
  seekBackBtn: document.getElementById("seekBackBtn"),
  playPauseBtn: document.getElementById("playPauseBtn"),
  seekForwardBtn: document.getElementById("seekForwardBtn"),
  nextBtn: document.getElementById("nextBtn"),
  favoriteBtn: document.getElementById("favoriteBtn"),
  muteBtn: document.getElementById("muteBtn"),
  volumeSlider: document.getElementById("volumeSlider"),
  playerActions: document.querySelector(".player-actions"),
  trackSelect: document.getElementById("trackSelect"),
  bookmarkCueBtn: document.getElementById("bookmarkCueBtn"),
  rangeStartBtn: document.getElementById("rangeStartBtn"),
  bookmarkRangeBtn: document.getElementById("bookmarkRangeBtn"),
  bookmarkNoteInput: document.getElementById("bookmarkNoteInput"),
  rangeStatus: document.getElementById("rangeStatus"),
  videoNote: document.getElementById("videoNote"),
  saveNoteBtn: document.getElementById("saveNoteBtn"),
  bookmarkList: document.getElementById("bookmarkList"),
  jumpModal: document.getElementById("jumpModal"),
  jumpCloseBtn: document.getElementById("jumpCloseBtn"),
  jumpInput: document.getElementById("jumpInput"),
  jumpResults: document.getElementById("jumpResults"),
  statusBar: document.getElementById("statusBar"),
};

function setStatus(message, tone = "info") {
  elements.statusBar.textContent = message;
  elements.statusBar.classList.remove("ok", "error");
  if (tone === "ok") {
    elements.statusBar.classList.add("ok");
  }
  if (tone === "error") {
    elements.statusBar.classList.add("error");
  }
}

function currentVideo() {
  if (!state.videos.length) {
    return null;
  }
  return state.videos[state.index] || null;
}

async function apiRequest(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  let payload = {};
  try {
    payload = await response.json();
  } catch (_error) {
    payload = {};
  }

  if (!response.ok) {
    const message = payload.error || `Request failed (${response.status})`;
    throw new Error(message);
  }
  return payload;
}

function formatDuration(durationSeconds) {
  if (typeof durationSeconds !== "number" || Number.isNaN(durationSeconds)) {
    return "";
  }
  const totalSeconds = Math.max(0, Math.round(durationSeconds));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function formatTimeMs(value) {
  const safe = Number.isFinite(value) ? Math.max(0, Math.round(value)) : 0;
  const totalSeconds = Math.floor(safe / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function updateAutoplayToggle() {
  elements.autoplayToggle.textContent = `連続再生: ${state.autoplayContinuous ? "ON" : "OFF"}`;
}

function updateShuffleToggle() {
  elements.shuffleToggle.textContent = `シャッフル: ${state.shuffleMode ? "ON" : "OFF"}`;
}

function shuffleIndices(indices) {
  const cloned = [...indices];
  for (let idx = cloned.length - 1; idx > 0; idx -= 1) {
    const randomIdx = Math.floor(Math.random() * (idx + 1));
    const tmp = cloned[idx];
    cloned[idx] = cloned[randomIdx];
    cloned[randomIdx] = tmp;
  }
  return cloned;
}

function refillShuffleQueue(currentIndex) {
  const candidates = [];
  for (let idx = 0; idx < state.videos.length; idx += 1) {
    if (idx !== currentIndex) {
      candidates.push(idx);
    }
  }
  if (!candidates.length && currentIndex >= 0) {
    candidates.push(currentIndex);
  }
  state.shuffleQueue = shuffleIndices(candidates);
}

function chooseNextShuffleIndex() {
  if (!state.videos.length) {
    return -1;
  }
  if (!state.shuffleQueue.length) {
    refillShuffleQueue(state.index);
  }
  const next = state.shuffleQueue.shift();
  if (typeof next !== "number") {
    return -1;
  }
  return next;
}

function resetPlaybackTracking(initialIndex) {
  state.playbackHistory = [initialIndex];
  state.historyPointer = 0;
  state.shuffleQueue = [];
}

function pushHistory(index) {
  if (state.historyPointer < state.playbackHistory.length - 1) {
    state.playbackHistory = state.playbackHistory.slice(0, state.historyPointer + 1);
  }
  state.playbackHistory.push(index);
  state.historyPointer = state.playbackHistory.length - 1;
}

function updatePlayPauseButton() {
  elements.playPauseBtn.textContent = elements.videoPlayer.paused ? "再生" : "停止";
}

function updateFavoriteButton() {
  const video = currentVideo();
  if (!video) {
    elements.favoriteBtn.textContent = "☆ ファボ";
    return;
  }
  elements.favoriteBtn.textContent = video.is_favorite ? "★ ファボ済み" : "☆ ファボ";
}

function updateMetaDrawerState() {
  elements.videoMetaDrawer.classList.toggle("open", state.metaExpanded);
  elements.phoneShell.classList.toggle("meta-open", state.metaExpanded);
  elements.metaTabBtn.setAttribute("aria-expanded", state.metaExpanded ? "true" : "false");
}

function updateControlsDrawerState() {
  elements.mainControls.classList.toggle("collapsed", !state.controlsExpanded);
  elements.controlsToggleBtn.setAttribute("aria-expanded", state.controlsExpanded ? "true" : "false");
  elements.controlsToggleBtn.textContent = state.controlsExpanded ? "操作を隠す" : "操作を表示";
}

function toggleControlsDrawer(forceValue = null) {
  if (typeof forceValue === "boolean") {
    state.controlsExpanded = forceValue;
  } else {
    state.controlsExpanded = !state.controlsExpanded;
  }
  localStorage.setItem("substudy.controls_expanded", state.controlsExpanded ? "true" : "false");
  updateControlsDrawerState();
  scheduleControlsToggleIdleFade();
}

function clearControlsToggleIdleFadeTimer() {
  if (state.controlsFadeTimer !== null) {
    window.clearTimeout(state.controlsFadeTimer);
    state.controlsFadeTimer = null;
  }
}

function showControlsToggleButton() {
  elements.controlsToggleBtn.classList.remove("idle-dim");
}

function scheduleControlsToggleIdleFade() {
  clearControlsToggleIdleFadeTimer();
  showControlsToggleButton();
  if (state.controlsExpanded) {
    return;
  }
  state.controlsFadeTimer = window.setTimeout(() => {
    elements.controlsToggleBtn.classList.add("idle-dim");
    state.controlsFadeTimer = null;
  }, CONTROL_TOGGLE_IDLE_MS);
}

function shortenForTab(text) {
  const value = String(text || "").trim();
  if (!value) {
    return "description";
  }
  if (value.length <= 30) {
    return value;
  }
  return `${value.slice(0, 30)}...`;
}

function setVideoMetaFallback(messagePrimary, messageSecondary = "") {
  elements.metaPrimaryLine.textContent = messagePrimary;
  elements.metaSecondaryLine.textContent = messageSecondary;
  elements.metaTabBtn.textContent = shortenForTab(messagePrimary);
}

function toggleMetaDrawer(forceValue = null) {
  if (typeof forceValue === "boolean") {
    state.metaExpanded = forceValue;
  } else {
    state.metaExpanded = !state.metaExpanded;
  }
  updateMetaDrawerState();
}

function updateMuteButtonLabel() {
  const volumePercent = Math.round((elements.videoPlayer.volume || 0) * 100);
  elements.muteBtn.textContent = elements.videoPlayer.muted ? "ミュート中" : `音量: ${volumePercent}%`;
}

function applyVolumeSettings(volumeValue, muted) {
  const normalizedVolume = Number.isFinite(volumeValue) ? Math.min(1, Math.max(0, volumeValue)) : 1;
  elements.videoPlayer.volume = normalizedVolume;
  elements.videoPlayer.muted = Boolean(muted);
  elements.volumeSlider.value = String(normalizedVolume);
  updateMuteButtonLabel();
}

function loadVolumeSettings() {
  const storedVolume = Number(localStorage.getItem("substudy.volume"));
  const storedMuted = localStorage.getItem("substudy.muted") === "true";
  const initialVolume = Number.isFinite(storedVolume) ? storedVolume : 1;
  applyVolumeSettings(initialVolume, storedMuted);
}

function parseInitialVideoSelectionFromUrl() {
  try {
    const params = new URLSearchParams(window.location.search);
    const sourceId = String(params.get("source_id") || "").trim();
    const videoId = String(params.get("video_id") || "").trim();
    return {
      sourceId,
      videoId,
    };
  } catch (_error) {
    return {
      sourceId: "",
      videoId: "",
    };
  }
}

function updateUrlVideoSelection(sourceId = "", videoId = "") {
  try {
    const url = new URL(window.location.href);
    if (sourceId) {
      url.searchParams.set("source_id", sourceId);
    } else {
      url.searchParams.delete("source_id");
    }
    if (videoId) {
      url.searchParams.set("video_id", videoId);
    } else {
      url.searchParams.delete("video_id");
    }
    const search = url.searchParams.toString();
    const nextRelativeUrl = `${url.pathname}${search ? `?${search}` : ""}${url.hash || ""}`;
    window.history.replaceState({}, "", nextRelativeUrl);
  } catch (_error) {
    return;
  }
}

function isJumpModalOpen() {
  return !elements.jumpModal.classList.contains("hidden");
}

function normalizeSearchText(value) {
  return String(value || "").trim().toLowerCase();
}

function truncateText(value, maxLength = 120) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

function parseJumpIndex(queryText) {
  const value = String(queryText || "").trim();
  if (!value) {
    return null;
  }
  const match = value.match(/^#?\s*(\d{1,5})(?:\s*\/\s*\d+)?$/);
  if (!match) {
    return null;
  }
  const index = Number(match[1]) - 1;
  if (!Number.isInteger(index) || index < 0 || index >= state.videos.length) {
    return null;
  }
  return index;
}

function extractVideoIdFromQuery(queryText) {
  const value = String(queryText || "").trim();
  if (!value) {
    return null;
  }
  const urlMatch = value.match(/\/video\/(\d{10,})/);
  if (urlMatch) {
    return urlMatch[1];
  }
  const numericMatch = value.match(/^(\d{10,})$/);
  if (numericMatch) {
    return numericMatch[1];
  }
  return null;
}

function buildJumpCandidates() {
  if (!state.videos.length) {
    return [];
  }

  const rawQuery = elements.jumpInput.value || "";
  const query = normalizeSearchText(rawQuery);
  if (!query) {
    const start = Math.max(0, state.index - 4);
    const end = Math.min(state.videos.length, start + 10);
    const fallback = [];
    for (let idx = start; idx < end; idx += 1) {
      fallback.push({
        index: idx,
        score: 1,
        reason: idx === state.index ? "現在位置" : "近くの動画",
      });
    }
    return fallback;
  }

  const parsedIndex = parseJumpIndex(rawQuery);
  const videoId = extractVideoIdFromQuery(rawQuery);
  const candidates = [];
  const seen = new Set();

  if (parsedIndex !== null) {
    candidates.push({
      index: parsedIndex,
      score: 200,
      reason: "番号ジャンプ",
    });
    seen.add(parsedIndex);
  }

  for (let idx = 0; idx < state.videos.length; idx += 1) {
    const video = state.videos[idx];
    let score = 0;
    let reason = "";
    const title = normalizeSearchText(video.title);
    const description = normalizeSearchText(video.description);
    const uploader = normalizeSearchText(video.uploader);
    const sourceId = normalizeSearchText(video.source_id);
    const indexLabel = String(idx + 1);

    if (videoId && video.video_id === videoId) {
      score = 190;
      reason = "video_id 完全一致";
    } else if (video.video_id === rawQuery.trim()) {
      score = 180;
      reason = "video_id 完全一致";
    } else if (video.video_id.includes(query)) {
      score = 110;
      reason = "video_id 部分一致";
    } else if (title.includes(query)) {
      score = 90;
      reason = "タイトル一致";
    } else if (description.includes(query)) {
      score = 80;
      reason = "description一致";
    } else if (uploader.includes(query)) {
      score = 70;
      reason = "投稿者一致";
    } else if (sourceId.includes(query)) {
      score = 60;
      reason = "source一致";
    } else if (indexLabel === query) {
      score = 120;
      reason = "番号一致";
    }

    if (score <= 0 || seen.has(idx)) {
      continue;
    }
    seen.add(idx);
    candidates.push({ index: idx, score, reason });
  }

  candidates.sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    return Math.abs(a.index - state.index) - Math.abs(b.index - state.index);
  });
  return candidates.slice(0, 14);
}

function renderJumpResults() {
  elements.jumpResults.textContent = "";
  if (!state.jumpResults.length) {
    const empty = document.createElement("p");
    empty.className = "jump-empty";
    empty.textContent = "一致する動画がありません。";
    elements.jumpResults.appendChild(empty);
    return;
  }

  for (let idx = 0; idx < state.jumpResults.length; idx += 1) {
    const result = state.jumpResults[idx];
    const video = state.videos[result.index];
    if (!video) {
      continue;
    }

    const rowButton = document.createElement("button");
    rowButton.type = "button";
    rowButton.className = "jump-result-item";
    if (idx === state.jumpSelectedIndex) {
      rowButton.classList.add("active");
    }

    const title = document.createElement("span");
    title.className = "jump-result-title";
    title.textContent = truncateText(video.description || video.title || "(説明なし)", 95);

    const meta = document.createElement("span");
    meta.className = "jump-result-meta";
    meta.textContent = `${result.index + 1}/${state.videos.length} • ${video.source_id} • ${video.video_id} • ${result.reason}`;

    rowButton.appendChild(title);
    rowButton.appendChild(meta);
    rowButton.addEventListener("click", () => {
      jumpToIndex(result.index).catch((error) => setStatus(error.message, "error"));
    });
    elements.jumpResults.appendChild(rowButton);
  }
}

function refreshJumpResults() {
  state.jumpResults = buildJumpCandidates();
  state.jumpSelectedIndex = 0;
  renderJumpResults();
}

function moveJumpSelection(delta) {
  if (!state.jumpResults.length) {
    return;
  }
  const next = state.jumpSelectedIndex + delta;
  state.jumpSelectedIndex = Math.max(0, Math.min(state.jumpResults.length - 1, next));
  renderJumpResults();
}

async function jumpToIndex(index) {
  await openVideo(index, true);
  closeJumpModal();
  setStatus(`ジャンプしました: ${index + 1}/${state.videos.length}`, "ok");
}

async function jumpToSelectedResult() {
  if (!state.jumpResults.length) {
    return;
  }
  const selected = state.jumpResults[state.jumpSelectedIndex];
  if (!selected) {
    return;
  }
  await jumpToIndex(selected.index);
}

function openJumpModal() {
  if (!state.videos.length) {
    setStatus("ジャンプ対象の動画がありません。", "error");
    return;
  }
  closeLyricReel();
  elements.jumpModal.classList.remove("hidden");
  elements.jumpModal.setAttribute("aria-hidden", "false");
  refreshJumpResults();
  elements.jumpInput.focus();
  elements.jumpInput.select();
}

function closeJumpModal() {
  elements.jumpModal.classList.add("hidden");
  elements.jumpModal.setAttribute("aria-hidden", "true");
}

function renderSourceOptions(preferredSource = "") {
  const selectedBefore = preferredSource || elements.sourceSelect.value || "";
  const options = ["", ...state.sources];
  elements.sourceSelect.textContent = "";

  for (const sourceId of options) {
    const option = document.createElement("option");
    option.value = sourceId;
    option.textContent = sourceId ? sourceId : "All Sources";
    elements.sourceSelect.appendChild(option);
  }

  const available = options.includes(selectedBefore) ? selectedBefore : "";
  elements.sourceSelect.value = available;
}

function renderVideoMeta(video) {
  const current = state.index + 1;
  const total = state.videos.length;
  const descriptionLike = (video.description || video.title || "").trim();
  const duration = formatDuration(video.duration);
  const secondaryLine = [
    `${current} / ${total}`,
    video.source_id || "",
    video.uploader || "",
    video.upload_date || "",
    duration,
  ]
    .filter(Boolean)
    .join(" • ");
  elements.metaPrimaryLine.textContent = descriptionLike || "(説明なし)";
  elements.metaSecondaryLine.textContent = secondaryLine;
  elements.metaTabBtn.textContent = shortenForTab(descriptionLike || "description");
}

function renderTrackOptions(video) {
  elements.trackSelect.textContent = "";

  if (!video.tracks.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "字幕なし";
    elements.trackSelect.appendChild(option);
    elements.trackSelect.disabled = true;
    state.currentTrackId = null;
    return;
  }

  elements.trackSelect.disabled = false;
  const stillValid = video.tracks.some((track) => track.track_id === state.currentTrackId);
  state.currentTrackId = stillValid
    ? state.currentTrackId
    : video.default_track || video.tracks[0].track_id;

  for (const track of video.tracks) {
    const option = document.createElement("option");
    option.value = track.track_id;
    option.textContent = `${track.label} (${track.kind})`;
    elements.trackSelect.appendChild(option);
  }

  elements.trackSelect.value = state.currentTrackId;
}

function clearSubtitleOverlay(message = "字幕がありません") {
  elements.subtitleOverlay.textContent = message;
}

function findActiveCueIndex(timeMs) {
  if (!state.cues.length) {
    return -1;
  }

  let left = 0;
  let right = state.cues.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const cue = state.cues[mid];
    if (timeMs < cue.start_ms) {
      right = mid - 1;
    } else if (timeMs > cue.end_ms) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  return -1;
}

function findNearestCueIndex(timeMs) {
  if (!state.cues.length) {
    return -1;
  }
  let nearest = 0;
  let nearestDistance = Number.POSITIVE_INFINITY;
  for (let idx = 0; idx < state.cues.length; idx += 1) {
    const cue = state.cues[idx];
    const distance = Math.abs(timeMs - cue.start_ms);
    if (distance < nearestDistance) {
      nearestDistance = distance;
      nearest = idx;
    }
  }
  return nearest;
}

function renderLyricReel() {
  elements.lyricReelList.textContent = "";
  if (!state.cues.length || state.lyricReelIndex < 0) {
    return;
  }

  const start = Math.max(0, state.lyricReelIndex - 4);
  const end = Math.min(state.cues.length, state.lyricReelIndex + 5);
  for (let idx = start; idx < end; idx += 1) {
    const cue = state.cues[idx];
    const line = document.createElement("p");
    line.className = "lyric-reel-line";
    if (idx === state.lyricReelIndex) {
      line.classList.add("active");
    } else if (Math.abs(idx - state.lyricReelIndex) >= 3) {
      line.dataset.distance = "far";
    }
    line.textContent = cue.text || "...";
    elements.lyricReelList.appendChild(line);
  }
}

function clearLyricWheelResetTimer() {
  if (state.lyricWheelResetTimer !== null) {
    window.clearTimeout(state.lyricWheelResetTimer);
    state.lyricWheelResetTimer = null;
  }
}

function resetLyricWheelAccumulator() {
  clearLyricWheelResetTimer();
  state.lyricWheelAccumulator = 0;
}

function scheduleLyricWheelAccumulatorReset() {
  clearLyricWheelResetTimer();
  state.lyricWheelResetTimer = window.setTimeout(() => {
    state.lyricWheelAccumulator = 0;
    state.lyricWheelResetTimer = null;
  }, LYRIC_WHEEL_IDLE_RESET_MS);
}

function closeLyricReel() {
  if (!state.lyricReelActive) {
    return;
  }
  resetLyricWheelAccumulator();
  state.lyricReelActive = false;
  elements.phoneShell.classList.remove("lyric-reel-active");
  elements.lyricReelOverlay.classList.add("hidden");
  elements.lyricReelOverlay.setAttribute("aria-hidden", "true");
}

function seekLyricReelToIndex(index) {
  if (!state.cues.length) {
    return;
  }
  const nextIndex = Math.max(0, Math.min(state.cues.length - 1, index));
  const cue = state.cues[nextIndex];
  state.lyricReelIndex = nextIndex;
  state.activeCueIndex = nextIndex;
  elements.videoPlayer.currentTime = cue.start_ms / 1000;
  elements.subtitleOverlay.textContent = cue.text || "...";
  renderLyricReel();
}

function openLyricReel() {
  if (!state.cues.length) {
    return false;
  }
  const currentMs = Math.round((elements.videoPlayer.currentTime || 0) * 1000);
  const activeIndex = findActiveCueIndex(currentMs);
  const initialIndex = activeIndex >= 0 ? activeIndex : findNearestCueIndex(currentMs);

  state.lyricReelActive = true;
  resetLyricWheelAccumulator();
  elements.videoPlayer.pause();
  elements.phoneShell.classList.add("lyric-reel-active");
  elements.lyricReelOverlay.classList.remove("hidden");
  elements.lyricReelOverlay.setAttribute("aria-hidden", "false");
  seekLyricReelToIndex(initialIndex >= 0 ? initialIndex : 0);
  return true;
}

function stepLyricReel(step) {
  if (!state.lyricReelActive) {
    if (!openLyricReel()) {
      return false;
    }
  }
  const nextIndex = state.lyricReelIndex + step;
  seekLyricReelToIndex(nextIndex);
  return true;
}

function updateSubtitleFromPlayback() {
  if (!state.cues.length) {
    return;
  }
  const currentMs = Math.round(elements.videoPlayer.currentTime * 1000);
  const index = findActiveCueIndex(currentMs);
  if (index === state.activeCueIndex) {
    return;
  }

  state.activeCueIndex = index;
  if (index < 0) {
    elements.subtitleOverlay.textContent = "...";
    return;
  }
  elements.subtitleOverlay.textContent = state.cues[index].text;
}

function clearCountdown() {
  if (state.countdownTimer !== null) {
    window.clearInterval(state.countdownTimer);
    state.countdownTimer = null;
  }
  state.countdownRemaining = 0;
  elements.countdownPanel.classList.add("hidden");
}

function startCountdown() {
  clearCountdown();
  state.countdownRemaining = 3;
  elements.countdownValue.textContent = String(state.countdownRemaining);
  elements.countdownPanel.classList.remove("hidden");

  state.countdownTimer = window.setInterval(() => {
    state.countdownRemaining -= 1;
    if (state.countdownRemaining <= 0) {
      clearCountdown();
      nextVideo().catch((error) => setStatus(error.message, "error"));
      return;
    }
    elements.countdownValue.textContent = String(state.countdownRemaining);
  }, 1000);
}

async function loadTrackCues() {
  closeLyricReel();
  const video = currentVideo();
  if (!video || !state.currentTrackId) {
    state.cues = [];
    state.activeCueIndex = -1;
    state.lyricReelIndex = -1;
    clearSubtitleOverlay("字幕トラックがありません");
    return;
  }

  const params = new URLSearchParams({
    source_id: video.source_id,
    video_id: video.video_id,
    track: state.currentTrackId,
  });

  const payload = await apiRequest(`/api/subtitles?${params.toString()}`);
  state.cues = Array.isArray(payload.cues) ? payload.cues : [];
  state.activeCueIndex = -1;
  state.lyricReelIndex = -1;

  if (!state.cues.length) {
    clearSubtitleOverlay("字幕が見つかりません");
    return;
  }

  clearSubtitleOverlay(state.cues[0].text || "字幕あり");
  updateSubtitleFromPlayback();
}

async function loadBookmarks() {
  const video = currentVideo();
  if (!video) {
    state.bookmarks = [];
    renderBookmarkList();
    return;
  }

  const params = new URLSearchParams({
    source_id: video.source_id,
    video_id: video.video_id,
    limit: "300",
  });
  const payload = await apiRequest(`/api/bookmarks?${params.toString()}`);
  state.bookmarks = Array.isArray(payload.bookmarks) ? payload.bookmarks : [];
  renderBookmarkList();
}

function renderBookmarkList() {
  elements.bookmarkList.textContent = "";

  if (!state.bookmarks.length) {
    const empty = document.createElement("p");
    empty.className = "hint";
    empty.textContent = "まだ保存されたブックマークはありません。";
    elements.bookmarkList.appendChild(empty);
    return;
  }

  for (const bookmark of state.bookmarks) {
    const item = document.createElement("article");
    item.className = "bookmark-item";

    const time = document.createElement("p");
    time.className = "time";
    const startLabel = bookmark.start_label || formatTimeMs(bookmark.start_ms);
    const endLabel = bookmark.end_label || formatTimeMs(bookmark.end_ms);
    time.textContent = `${startLabel} - ${endLabel}`;

    const text = document.createElement("p");
    text.textContent = bookmark.text || "(字幕テキストなし)";

    const meta = document.createElement("p");
    meta.className = "meta";
    meta.textContent = `${bookmark.track || "track: none"} • ${bookmark.created_at || ""}`;

    const noteInput = document.createElement("textarea");
    noteInput.rows = 2;
    noteInput.value = bookmark.note || "";
    noteInput.placeholder = "ブックマークメモ";

    const actions = document.createElement("div");
    actions.className = "actions";

    const saveBtn = document.createElement("button");
    saveBtn.type = "button";
    saveBtn.textContent = "メモ保存";
    saveBtn.addEventListener("click", () => updateBookmarkNote(bookmark.id, noteInput.value));

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "delete";
    deleteBtn.textContent = "削除";
    deleteBtn.addEventListener("click", () => removeBookmark(bookmark.id));

    actions.appendChild(saveBtn);
    actions.appendChild(deleteBtn);

    item.appendChild(time);
    item.appendChild(text);
    item.appendChild(meta);
    item.appendChild(noteInput);
    item.appendChild(actions);

    elements.bookmarkList.appendChild(item);
  }
}

async function updateBookmarkNote(bookmarkId, note) {
  await apiRequest(`/api/bookmarks/${bookmarkId}/note`, {
    method: "POST",
    body: JSON.stringify({ note }),
  });
  await loadBookmarks();
  setStatus("ブックマークメモを更新しました。", "ok");
}

async function removeBookmark(bookmarkId) {
  await apiRequest(`/api/bookmarks/${bookmarkId}`, { method: "DELETE" });
  await loadBookmarks();
  setStatus("ブックマークを削除しました。", "ok");
}

async function openVideo(index, autoplay = true, historyMode = "push") {
  if (!state.videos.length) {
    return;
  }

  clearCountdown();
  closeLyricReel();
  state.index = Math.max(0, Math.min(index, state.videos.length - 1));
  if (historyMode === "push") {
    pushHistory(state.index);
  }

  const video = currentVideo();
  elements.videoPlayer.src = video.media_url;
  elements.videoPlayer.load();
  updateUrlVideoSelection(video.source_id, video.video_id);

  renderVideoMeta(video);
  updateFavoriteButton();
  elements.videoNote.value = video.note || "";
  renderTrackOptions(video);

  await loadTrackCues();
  await loadBookmarks();

  if (autoplay) {
    try {
      await elements.videoPlayer.play();
    } catch (_error) {
      setStatus("自動再生がブロックされました。再生ボタンを押してください。", "error");
    }
  }

  updateMuteButtonLabel();
  updatePlayPauseButton();
  setStatus(`動画を表示中: ${video.source_id} (${state.index + 1}/${state.videos.length})`);
}

async function loadFeed(sourceId = "", startRandom = false, preferredVideoId = "") {
  closeJumpModal();
  closeLyricReel();
  const params = new URLSearchParams({ limit: "900", offset: "0" });
  if (sourceId) {
    params.set("source_id", sourceId);
  }

  setStatus("フィードを読み込み中...");
  const payload = await apiRequest(`/api/feed?${params.toString()}`);

  state.videos = Array.isArray(payload.videos) ? payload.videos : [];
  state.sources = Array.isArray(payload.sources) ? payload.sources : [];
  state.index = 0;
  state.rangeStartMs = null;
  state.metaExpanded = false;
  updateMetaDrawerState();
  elements.rangeStatus.textContent = "範囲ブックマークは未開始です。";
  renderSourceOptions(sourceId);

  if (!state.videos.length) {
    elements.videoPlayer.removeAttribute("src");
    elements.videoPlayer.load();
    updateUrlVideoSelection(sourceId, "");
    state.playbackHistory = [];
    state.historyPointer = -1;
    state.shuffleQueue = [];
    state.lyricReelIndex = -1;
    clearSubtitleOverlay("動画がありません");
    setVideoMetaFallback("動画が見つかりません", "0 / 0");
    state.bookmarks = [];
    renderBookmarkList();
    setStatus("条件に一致する動画がありません。", "error");
    return;
  }

  let initialIndex = -1;
  if (preferredVideoId) {
    initialIndex = state.videos.findIndex((video) => video.video_id === preferredVideoId);
  }
  if (initialIndex < 0) {
    initialIndex = startRandom
      ? Math.floor(Math.random() * state.videos.length)
      : 0;
  }
  resetPlaybackTracking(initialIndex);
  await openVideo(initialIndex, true, "keep");
  setStatus(`${state.videos.length}件の動画を読み込みました。`, "ok");
}

async function nextVideo() {
  if (!state.videos.length) {
    return;
  }
  if (state.shuffleMode) {
    if (state.historyPointer < state.playbackHistory.length - 1) {
      const forwardIndex = state.playbackHistory[state.historyPointer + 1];
      state.historyPointer += 1;
      await openVideo(forwardIndex, true, "keep");
      return;
    }
    const nextShuffleIndex = chooseNextShuffleIndex();
    if (nextShuffleIndex < 0) {
      setStatus("次の動画が見つかりません。", "info");
      return;
    }
    await openVideo(nextShuffleIndex, true, "push");
    return;
  }

  if (state.index >= state.videos.length - 1) {
    setStatus("最後の動画です。", "info");
    return;
  }
  await openVideo(state.index + 1, true, "push");
}

async function prevVideo() {
  if (!state.videos.length) {
    return;
  }
  if (state.shuffleMode) {
    if (state.historyPointer <= 0) {
      setStatus("これより前の履歴はありません。", "info");
      return;
    }
    state.historyPointer -= 1;
    const previousIndex = state.playbackHistory[state.historyPointer];
    await openVideo(previousIndex, true, "keep");
    return;
  }

  if (state.index <= 0) {
    setStatus("最初の動画です。", "info");
    return;
  }
  await openVideo(state.index - 1, true, "push");
}

async function toggleFavorite() {
  const video = currentVideo();
  if (!video) {
    return;
  }

  const payload = await apiRequest("/api/favorites/toggle", {
    method: "POST",
    body: JSON.stringify({
      source_id: video.source_id,
      video_id: video.video_id,
    }),
  });

  video.is_favorite = Boolean(payload.is_favorite);
  updateFavoriteButton();
  setStatus(video.is_favorite ? "動画をファボしました。" : "ファボを解除しました。", "ok");
}

async function saveVideoNote() {
  const video = currentVideo();
  if (!video) {
    return;
  }

  const payload = await apiRequest("/api/video-note", {
    method: "POST",
    body: JSON.stringify({
      source_id: video.source_id,
      video_id: video.video_id,
      note: elements.videoNote.value,
    }),
  });

  video.note = payload.note || "";
  setStatus("動画メモを保存しました。", "ok");
}

function currentCueOrFallback() {
  if (state.activeCueIndex >= 0 && state.activeCueIndex < state.cues.length) {
    return state.cues[state.activeCueIndex];
  }

  const nowMs = Math.round(elements.videoPlayer.currentTime * 1000);
  const fallbackText = elements.subtitleOverlay.textContent.trim();
  return {
    start_ms: Math.max(0, nowMs - 1200),
    end_ms: nowMs + 1200,
    text: fallbackText === "..." ? "" : fallbackText,
  };
}

function gatherRangeText(startMs, endMs) {
  const parts = [];
  const seen = new Set();
  for (const cue of state.cues) {
    if (cue.end_ms < startMs || cue.start_ms > endMs) {
      continue;
    }
    const value = String(cue.text || "").trim();
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    parts.push(value);
  }
  return parts.join(" ").trim();
}

async function createBookmark(startMs, endMs, text) {
  const video = currentVideo();
  if (!video) {
    return;
  }

  await apiRequest("/api/bookmarks", {
    method: "POST",
    body: JSON.stringify({
      source_id: video.source_id,
      video_id: video.video_id,
      track: state.currentTrackId,
      start_ms: Math.round(startMs),
      end_ms: Math.round(endMs),
      text,
      note: elements.bookmarkNoteInput.value,
    }),
  });

  await loadBookmarks();
  setStatus("ブックマークを保存しました。", "ok");
}

async function bookmarkCurrentCue() {
  const cue = currentCueOrFallback();
  await createBookmark(cue.start_ms, cue.end_ms, cue.text || "");
}

function markRangeStart() {
  state.rangeStartMs = Math.round(elements.videoPlayer.currentTime * 1000);
  elements.rangeStatus.textContent = `開始位置: ${formatTimeMs(state.rangeStartMs)} (Tで保存)`;
  setStatus("範囲開始を設定しました。", "ok");
}

async function bookmarkCurrentRange() {
  if (state.rangeStartMs === null) {
    setStatus("先に範囲開始 (R) を設定してください。", "error");
    return;
  }

  const nowMs = Math.round(elements.videoPlayer.currentTime * 1000);
  const startMs = Math.min(state.rangeStartMs, nowMs);
  const endMs = Math.max(state.rangeStartMs, nowMs);
  const text = gatherRangeText(startMs, endMs) || elements.subtitleOverlay.textContent.trim();

  await createBookmark(startMs, endMs, text);
  state.rangeStartMs = null;
  elements.rangeStatus.textContent = "範囲ブックマークは未開始です。";
}

function togglePlayPause() {
  if (!elements.videoPlayer.src) {
    return;
  }
  if (elements.videoPlayer.paused) {
    elements.videoPlayer.play().catch(() => {});
  } else {
    elements.videoPlayer.pause();
  }
}

function seekBySeconds(deltaSeconds) {
  if (!elements.videoPlayer.src) {
    return;
  }
  const duration = Number.isFinite(elements.videoPlayer.duration)
    ? elements.videoPlayer.duration
    : null;
  const current = elements.videoPlayer.currentTime || 0;
  let next = current + deltaSeconds;
  if (duration !== null) {
    next = Math.min(duration, Math.max(0, next));
  } else {
    next = Math.max(0, next);
  }
  elements.videoPlayer.currentTime = next;
  if (state.lyricReelActive && state.cues.length) {
    const nearestIndex = findNearestCueIndex(Math.round(next * 1000));
    if (nearestIndex >= 0) {
      state.lyricReelIndex = nearestIndex;
      renderLyricReel();
    }
  }
  updateSubtitleFromPlayback();
}

function toggleMute() {
  elements.videoPlayer.muted = !elements.videoPlayer.muted;
  localStorage.setItem("substudy.muted", elements.videoPlayer.muted ? "true" : "false");
  updateMuteButtonLabel();
}

function toggleShuffleMode() {
  state.shuffleMode = !state.shuffleMode;
  if (!state.shuffleMode) {
    state.shuffleQueue = [];
  } else {
    refillShuffleQueue(state.index);
  }
  updateShuffleToggle();
  setStatus(`シャッフルを${state.shuffleMode ? "ON" : "OFF"}にしました。`, "ok");
}

function handleWheel(event) {
  const canUseLyricReel = state.lyricReelActive || state.cues.length > 0;
  if (canUseLyricReel) {
    event.preventDefault();
    event.stopPropagation();
    if (!state.lyricReelActive) {
      const opened = openLyricReel();
      if (!opened) {
        return;
      }
    }
    state.lyricWheelAccumulator += event.deltaY;
    scheduleLyricWheelAccumulatorReset();

    const steps = Math.floor(Math.abs(state.lyricWheelAccumulator) / LYRIC_WHEEL_STEP_DELTA);
    if (steps <= 0) {
      return;
    }
    const direction = state.lyricWheelAccumulator > 0 ? 1 : -1;
    for (let idx = 0; idx < steps; idx += 1) {
      seekLyricReelToIndex(state.lyricReelIndex + direction);
    }
    state.lyricWheelAccumulator -= direction * steps * LYRIC_WHEEL_STEP_DELTA;
    return;
  }

  const now = Date.now();
  if (now < state.wheelLockUntil) {
    event.preventDefault();
    return;
  }

  if (Math.abs(event.deltaY) < 18) {
    return;
  }

  const direction = event.deltaY > 0 ? 1 : -1;
  state.wheelLockUntil = now + 420;
  if (direction > 0) {
    nextVideo().catch((error) => setStatus(error.message, "error"));
  } else {
    prevVideo().catch((error) => setStatus(error.message, "error"));
  }
  event.preventDefault();
}

function handleKeydown(event) {
  if (isJumpModalOpen()) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeJumpModal();
    }
    return;
  }

  const key = event.key.toLowerCase();
  if (state.lyricReelActive) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeLyricReel();
      return;
    }
    if (event.key === "ArrowDown" || key === "j") {
      event.preventDefault();
      stepLyricReel(1);
      return;
    }
    if (event.key === "ArrowUp" || key === "k") {
      event.preventDefault();
      stepLyricReel(-1);
      return;
    }
  }

  const activeTag = document.activeElement ? document.activeElement.tagName : "";
  const isTyping = ["INPUT", "TEXTAREA", "SELECT"].includes(activeTag);
  if (isTyping) {
    return;
  }

  if (event.key === " ") {
    event.preventDefault();
    togglePlayPause();
    return;
  }

  if (event.key === "ArrowDown" || key === "j") {
    event.preventDefault();
    nextVideo().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (event.key === "ArrowUp" || key === "k") {
    event.preventDefault();
    prevVideo().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (event.key === "ArrowLeft") {
    event.preventDefault();
    seekBySeconds(-SEEK_SECONDS);
    return;
  }

  if (event.key === "ArrowRight") {
    event.preventDefault();
    seekBySeconds(SEEK_SECONDS);
    return;
  }

  if (key === "f") {
    event.preventDefault();
    toggleFavorite().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (key === "g") {
    event.preventDefault();
    openJumpModal();
    return;
  }

  if (key === "b") {
    event.preventDefault();
    bookmarkCurrentCue().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (key === "r") {
    event.preventDefault();
    markRangeStart();
    return;
  }

  if (key === "t") {
    event.preventDefault();
    bookmarkCurrentRange().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (key === "a") {
    event.preventDefault();
    state.autoplayContinuous = !state.autoplayContinuous;
    localStorage.setItem("substudy.autoplay", state.autoplayContinuous ? "on" : "off");
    updateAutoplayToggle();
    setStatus(`連続再生を${state.autoplayContinuous ? "ON" : "OFF"}にしました。`, "ok");
    return;
  }

  if (key === "m") {
    event.preventDefault();
    toggleMute();
    return;
  }

  if (key === "c") {
    event.preventDefault();
    clearCountdown();
    setStatus("自動遷移をキャンセルしました。", "ok");
  }
}

function bindTouchNavigation() {
  elements.phoneShell.addEventListener(
    "touchstart",
    (event) => {
      if (event.changedTouches.length === 0) {
        return;
      }
      state.touchStartY = event.changedTouches[0].clientY;
    },
    { passive: true }
  );

  elements.phoneShell.addEventListener(
    "touchend",
    (event) => {
      if (state.touchStartY === null || event.changedTouches.length === 0) {
        state.touchStartY = null;
        return;
      }
      const diff = event.changedTouches[0].clientY - state.touchStartY;
      state.touchStartY = null;
      if (Math.abs(diff) < 56) {
        return;
      }
      if (diff < 0) {
        nextVideo().catch((error) => setStatus(error.message, "error"));
      } else {
        prevVideo().catch((error) => setStatus(error.message, "error"));
      }
    },
    { passive: true }
  );
}

function bindEvents() {
  const resetControlsToggleFade = () => scheduleControlsToggleIdleFade();

  elements.jumpOpenBtn.addEventListener("click", () => openJumpModal());
  elements.jumpCloseBtn.addEventListener("click", () => closeJumpModal());
  elements.jumpModal.addEventListener("click", (event) => {
    if (event.target === elements.jumpModal) {
      closeJumpModal();
    }
  });
  elements.jumpInput.addEventListener("input", () => refreshJumpResults());
  elements.jumpInput.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      moveJumpSelection(1);
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      moveJumpSelection(-1);
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      jumpToSelectedResult().catch((error) => setStatus(error.message, "error"));
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      closeJumpModal();
    }
  });
  elements.lyricReelOverlay.addEventListener("click", () => closeLyricReel());

  elements.sourceSelect.addEventListener("change", () => {
    loadFeed(elements.sourceSelect.value).catch((error) => {
      setStatus(error.message, "error");
    });
    resetControlsToggleFade();
  });

  elements.autoplayToggle.addEventListener("click", () => {
    state.autoplayContinuous = !state.autoplayContinuous;
    localStorage.setItem("substudy.autoplay", state.autoplayContinuous ? "on" : "off");
    updateAutoplayToggle();
    setStatus(`連続再生を${state.autoplayContinuous ? "ON" : "OFF"}にしました。`, "ok");
    resetControlsToggleFade();
  });
  elements.shuffleToggle.addEventListener("click", () => {
    toggleShuffleMode();
    resetControlsToggleFade();
  });
  elements.controlsToggleBtn.addEventListener("click", () => toggleControlsDrawer());
  elements.controlsToggleBtn.addEventListener("pointerenter", () => showControlsToggleButton());
  elements.controlsToggleBtn.addEventListener("focus", () => showControlsToggleButton());
  elements.controlsToggleBtn.addEventListener("blur", () => resetControlsToggleFade());

  elements.prevBtn.addEventListener("click", () => {
    prevVideo().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.seekBackBtn.addEventListener("click", () => {
    seekBySeconds(-SEEK_SECONDS);
    resetControlsToggleFade();
  });
  elements.nextBtn.addEventListener("click", () => {
    nextVideo().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.seekForwardBtn.addEventListener("click", () => {
    seekBySeconds(SEEK_SECONDS);
    resetControlsToggleFade();
  });
  elements.playPauseBtn.addEventListener("click", () => {
    togglePlayPause();
    resetControlsToggleFade();
  });
  elements.favoriteBtn.addEventListener("click", () => {
    toggleFavorite().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.metaTabBtn.addEventListener("click", () => toggleMetaDrawer());

  elements.muteBtn.addEventListener("click", () => {
    toggleMute();
    resetControlsToggleFade();
  });
  elements.volumeSlider.addEventListener("input", () => {
    const volume = Number(elements.volumeSlider.value);
    const safeVolume = Number.isFinite(volume) ? Math.min(1, Math.max(0, volume)) : 1;
    elements.videoPlayer.volume = safeVolume;
    if (safeVolume > 0) {
      elements.videoPlayer.muted = false;
    }
    localStorage.setItem("substudy.volume", String(safeVolume));
    localStorage.setItem("substudy.muted", elements.videoPlayer.muted ? "true" : "false");
    updateMuteButtonLabel();
    resetControlsToggleFade();
  });

  elements.trackSelect.addEventListener("change", () => {
    state.currentTrackId = elements.trackSelect.value || null;
    loadTrackCues().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });

  elements.bookmarkCueBtn.addEventListener("click", () => {
    bookmarkCurrentCue().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.rangeStartBtn.addEventListener("click", () => {
    markRangeStart();
    resetControlsToggleFade();
  });
  elements.bookmarkRangeBtn.addEventListener("click", () => {
    bookmarkCurrentRange().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });

  elements.saveNoteBtn.addEventListener("click", () => {
    saveVideoNote().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });

  elements.cancelCountdownBtn.addEventListener("click", () => {
    clearCountdown();
    setStatus("自動遷移をキャンセルしました。", "ok");
    resetControlsToggleFade();
  });

  elements.videoPlayer.addEventListener("timeupdate", () => updateSubtitleFromPlayback());
  elements.videoPlayer.addEventListener("play", () => {
    closeLyricReel();
    updatePlayPauseButton();
  });
  elements.videoPlayer.addEventListener("pause", () => updatePlayPauseButton());
  elements.videoPlayer.addEventListener("volumechange", () => {
    localStorage.setItem("substudy.volume", String(elements.videoPlayer.volume || 0));
    localStorage.setItem("substudy.muted", elements.videoPlayer.muted ? "true" : "false");
    updateMuteButtonLabel();
    resetControlsToggleFade();
  });
  elements.videoPlayer.addEventListener("ended", () => {
    if (!state.autoplayContinuous) {
      return;
    }
    if (state.index >= state.videos.length - 1) {
      setStatus("最後の動画に到達しました。", "info");
      return;
    }
    startCountdown();
  });

  elements.phoneShell.addEventListener("wheel", handleWheel, { passive: false });
  elements.phoneShell.addEventListener("pointermove", resetControlsToggleFade, { passive: true });
  if (elements.playerActions) {
    elements.playerActions.addEventListener("pointermove", resetControlsToggleFade, { passive: true });
  }
  bindTouchNavigation();
  document.addEventListener("keydown", handleKeydown);
  document.addEventListener("keydown", resetControlsToggleFade);
}

async function initialize() {
  updateMetaDrawerState();
  updateControlsDrawerState();
  scheduleControlsToggleIdleFade();
  updateAutoplayToggle();
  updateShuffleToggle();
  loadVolumeSettings();
  updatePlayPauseButton();
  bindEvents();

  try {
    const initialSelection = parseInitialVideoSelectionFromUrl();
    await loadFeed(
      initialSelection.sourceId,
      false,
      initialSelection.videoId
    );
    setStatus("準備完了。動画上スクロールで時間同期歌詞リール、Gでジャンプできます。", "ok");
  } catch (error) {
    setStatus(error.message, "error");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initialize();
});
