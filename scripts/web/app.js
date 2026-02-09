const SEEK_SECONDS = 5;
const CONTROL_TOGGLE_IDLE_MS = 2600;
const LYRIC_WHEEL_STEP_DELTA = 52;
const LYRIC_REEL_AUTO_CLOSE_MS = 620;
const LYRIC_REEL_INERTIA_FACTOR = 0.04;
const LYRIC_REEL_INERTIA_DECAY = 0.88;
const LYRIC_REEL_INERTIA_MIN = 0.0007;
const DICT_POPUP_HIDE_DELAY_MS = 160;
const DICT_HOVER_LOOP_MIN_MS = 900;
const DICT_CONTEXT_MAX_CANDIDATES = 9;
const DICT_CONTEXT_PER_TERM_LIMIT = 4;
const DICT_CONTEXT_TOTAL_LIMIT = 12;
const DICT_CONTEXT_CORE_MIN_RESULTS = 2;
const SUBTITLE_WORD_PATTERN = /[A-Za-z]+(?:['’][A-Za-z]+)*/g;
const DICT_COLLAPSE_PARTICLE_WORDS = new Set([
  "back",
  "up",
  "out",
  "off",
  "on",
  "in",
  "away",
  "down",
  "over",
  "around",
  "through",
  "apart",
  "along",
  "across",
  "by",
  "about",
  "into",
]);
const DICT_IRREGULAR_BASE_FORMS = new Map([
  ["made", "make"],
  ["gone", "go"],
  ["went", "go"],
  ["gave", "give"],
  ["given", "give"],
  ["took", "take"],
  ["taken", "take"],
  ["came", "come"],
  ["did", "do"],
  ["done", "do"],
  ["was", "be"],
  ["were", "be"],
  ["been", "be"],
  ["saw", "see"],
  ["seen", "see"],
]);

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
  lyricReelTargetPosition: -1,
  lyricReelVisualPosition: -1,
  lyricReelAnimationFrame: null,
  lyricReelAnimationPrevTs: 0,
  lyricReelInertiaVelocity: 0,
  lyricReelAutoCloseTimer: null,
  lyricReelResumeOnClose: false,
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
  normalizationEnabled: localStorage.getItem("substudy.volume_normalization") !== "off",
  userVolume: 1,
  userMuted: false,
  dictLookupCache: new Map(),
  dictPopupHideTimer: null,
  dictLookupRequestId: 0,
  dictPopupWord: "",
  dictPopupCueStartMs: null,
  dictPopupCueEndMs: null,
  dictHoverLoopEnabled: localStorage.getItem("substudy.dict_hover_loop") !== "off",
  dictHoverLoopActive: false,
  dictHoverLoopPauseOnStop: false,
  dictBatchApiAvailable: null,
};

const elements = {
  sourceSelect: document.getElementById("sourceSelect"),
  jumpOpenBtn: document.getElementById("jumpOpenBtn"),
  autoplayToggle: document.getElementById("autoplayToggle"),
  shuffleToggle: document.getElementById("shuffleToggle"),
  normalizationToggle: document.getElementById("normalizationToggle"),
  dictHoverLoopToggle: document.getElementById("dictHoverLoopToggle"),
  videoPlayer: document.getElementById("videoPlayer"),
  phoneShell: document.getElementById("phoneShell"),
  subtitleOverlay: document.getElementById("subtitleOverlay"),
  subtitleDictPopup: document.getElementById("subtitleDictPopup"),
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

function updateNormalizationToggle() {
  if (!elements.normalizationToggle) {
    return;
  }
  elements.normalizationToggle.textContent = `音量正規化: ${state.normalizationEnabled ? "ON" : "OFF"}`;
}

function updateDictHoverLoopToggle() {
  if (!elements.dictHoverLoopToggle) {
    return;
  }
  elements.dictHoverLoopToggle.textContent = `辞書ループ: ${state.dictHoverLoopEnabled ? "ON" : "OFF"}`;
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
  if (state.userMuted) {
    elements.muteBtn.textContent = "ミュート中";
    return;
  }
  const volumePercent = Math.round(state.userVolume * 100);
  if (!state.normalizationEnabled) {
    elements.muteBtn.textContent = `音量: ${volumePercent}%`;
    return;
  }

  const video = currentVideo();
  const gainDbRaw = Number(video?.audio_gain_db);
  const gainDb = Number.isFinite(gainDbRaw) ? gainDbRaw : 0;
  if (Math.abs(gainDb) < 0.05) {
    elements.muteBtn.textContent = `音量: ${volumePercent}%`;
    return;
  }
  const gainPrefix = gainDb > 0 ? "+" : "";
  elements.muteBtn.textContent = `音量: ${volumePercent}% (${gainPrefix}${gainDb.toFixed(1)}dB)`;
}

function currentVideoNormalizationGain() {
  if (!state.normalizationEnabled) {
    return 1;
  }
  const video = currentVideo();
  if (!video) {
    return 1;
  }
  const gainDbRaw = Number(video.audio_gain_db);
  if (!Number.isFinite(gainDbRaw)) {
    return 1;
  }
  const gain = Math.pow(10, gainDbRaw / 20);
  if (!Number.isFinite(gain)) {
    return 1;
  }
  return Math.min(4, Math.max(0, gain));
}

function applyOutputVolume() {
  const gain = currentVideoNormalizationGain();
  const effectiveVolume = Math.min(1, Math.max(0, state.userVolume * gain));
  elements.videoPlayer.volume = effectiveVolume;
  elements.videoPlayer.muted = state.userMuted;
  elements.volumeSlider.value = String(state.userVolume);
  updateMuteButtonLabel();
}

function applyVolumeSettings(volumeValue, muted) {
  const normalizedVolume = Number.isFinite(volumeValue) ? Math.min(1, Math.max(0, volumeValue)) : 1;
  state.userVolume = normalizedVolume;
  state.userMuted = Boolean(muted);
  applyOutputVolume();
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

function normalizeDictionaryTerm(value) {
  return String(value || "")
    .replace(/[’‘]/g, "'")
    .replace(/[`]/g, "'")
    .toLowerCase()
    .trim()
    .replace(/^[^a-z0-9]+|[^a-z0-9]+$/g, "");
}

function clearDictionaryPopupHideTimer() {
  if (state.dictPopupHideTimer !== null) {
    window.clearTimeout(state.dictPopupHideTimer);
    state.dictPopupHideTimer = null;
  }
}

function updateDictionaryPopupCueRange() {
  const cue = currentCueOrFallback();
  const cueStartMs = Number(cue.start_ms);
  const cueEndMs = Number(cue.end_ms);
  if (!Number.isFinite(cueStartMs) || !Number.isFinite(cueEndMs)) {
    state.dictPopupCueStartMs = null;
    state.dictPopupCueEndMs = null;
    return;
  }
  const startMs = Math.max(0, Math.round(cueStartMs));
  const endMs = Math.max(startMs + DICT_HOVER_LOOP_MIN_MS, Math.round(cueEndMs));
  state.dictPopupCueStartMs = startMs;
  state.dictPopupCueEndMs = endMs;
}

function stopDictionaryHoverLoop() {
  if (!state.dictHoverLoopActive) {
    return;
  }
  const pauseOnStop = state.dictHoverLoopPauseOnStop;
  state.dictHoverLoopActive = false;
  state.dictHoverLoopPauseOnStop = false;
  if (pauseOnStop && !elements.videoPlayer.paused) {
    elements.videoPlayer.pause();
  }
}

function startDictionaryHoverLoop() {
  if (!state.dictHoverLoopEnabled || state.lyricReelActive || !elements.videoPlayer.src) {
    return;
  }
  const startMs = Number(state.dictPopupCueStartMs);
  const endMs = Number(state.dictPopupCueEndMs);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return;
  }
  if (!state.dictHoverLoopActive) {
    state.dictHoverLoopPauseOnStop = elements.videoPlayer.paused;
  }
  state.dictHoverLoopActive = true;
  const nowMs = Math.round((elements.videoPlayer.currentTime || 0) * 1000);
  if (nowMs < startMs || nowMs > endMs) {
    elements.videoPlayer.currentTime = startMs / 1000;
  }
  if (elements.videoPlayer.paused) {
    elements.videoPlayer.play().catch(() => {});
  }
}

function enforceDictionaryHoverLoop() {
  if (!state.dictHoverLoopActive || state.lyricReelActive || !elements.videoPlayer.src) {
    return;
  }
  const startMs = Number(state.dictPopupCueStartMs);
  const endMs = Number(state.dictPopupCueEndMs);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    stopDictionaryHoverLoop();
    return;
  }
  const nowMs = Math.round((elements.videoPlayer.currentTime || 0) * 1000);
  if (nowMs >= endMs || nowMs < startMs - 80) {
    elements.videoPlayer.currentTime = startMs / 1000;
    if (elements.videoPlayer.paused) {
      elements.videoPlayer.play().catch(() => {});
    }
  }
}

function hideSubtitleDictionaryPopup() {
  clearDictionaryPopupHideTimer();
  stopDictionaryHoverLoop();
  state.dictPopupWord = "";
  state.dictPopupCueStartMs = null;
  state.dictPopupCueEndMs = null;
  elements.subtitleDictPopup.classList.add("hidden");
  elements.subtitleDictPopup.setAttribute("aria-hidden", "true");
  const activeWord = elements.subtitleOverlay.querySelector(".subtitle-word.active");
  if (activeWord) {
    activeWord.classList.remove("active");
  }
}

function scheduleHideSubtitleDictionaryPopup() {
  clearDictionaryPopupHideTimer();
  state.dictPopupHideTimer = window.setTimeout(() => {
    hideSubtitleDictionaryPopup();
  }, DICT_POPUP_HIDE_DELAY_MS);
}

function positionSubtitleDictionaryPopup(anchorEl) {
  if (!anchorEl || !elements.subtitleDictPopup || elements.subtitleDictPopup.classList.contains("hidden")) {
    return;
  }
  const shellRect = elements.phoneShell.getBoundingClientRect();
  const anchorRect = anchorEl.getBoundingClientRect();
  const popupRect = elements.subtitleDictPopup.getBoundingClientRect();

  const centerX = anchorRect.left - shellRect.left + (anchorRect.width / 2);
  const minCenterX = (popupRect.width / 2) + 8;
  const maxCenterX = shellRect.width - (popupRect.width / 2) - 8;
  const clampedCenterX = Math.max(minCenterX, Math.min(maxCenterX, centerX));

  const spaceAbove = anchorRect.top - shellRect.top;
  const placeBelow = spaceAbove < (popupRect.height + 16);
  const top = placeBelow
    ? (anchorRect.bottom - shellRect.top + 10)
    : (anchorRect.top - shellRect.top - 10);
  const translateY = placeBelow ? "0%" : "-100%";

  elements.subtitleDictPopup.style.left = `${clampedCenterX.toFixed(1)}px`;
  elements.subtitleDictPopup.style.top = `${top.toFixed(1)}px`;
  elements.subtitleDictPopup.style.transform = `translate(-50%, ${translateY})`;
}

function groupDictionaryRows(rows) {
  const groups = [];
  const groupMap = new Map();

  for (const row of rows) {
    const rowTermNorm = normalizeDictionaryTerm(row?.term_norm || row?.term || "");
    const key = rowTermNorm || dictionaryResultKey(row);
    let group = groupMap.get(key);
    if (!group) {
      group = {
        key,
        term: String(row?.term || row?.term_norm || "").trim(),
        rowTermNorm,
        entries: [],
        entryKeys: new Set(),
      };
      groupMap.set(key, group);
      groups.push(group);
    }

    const lookupTerm = String(row?.lookup_term || "").trim();
    const definition = String(row?.definition || "").trim();
    const entryKey = `${normalizeDictionaryTerm(lookupTerm)}\u0000${definition}`;
    if (group.entryKeys.has(entryKey)) {
      continue;
    }
    group.entryKeys.add(entryKey);
    group.entries.push({
      lookupTerm,
      definition,
    });
  }

  return groups;
}

function renderSubtitleDictionaryPopup(term, rows, anchorEl) {
  elements.subtitleDictPopup.textContent = "";
  const title = document.createElement("p");
  title.className = "subtitle-dict-title";
  title.textContent = `Dictionary: ${term}`;
  elements.subtitleDictPopup.appendChild(title);

  const groups = groupDictionaryRows(rows);
  if (!groups.length) {
    const empty = document.createElement("p");
    empty.className = "subtitle-dict-empty";
    empty.textContent = "辞書エントリが見つかりません。";
    elements.subtitleDictPopup.appendChild(empty);
  } else {
    const list = document.createElement("div");
    list.className = "subtitle-dict-list";
    for (const group of groups) {
      const item = document.createElement("article");
      item.className = "subtitle-dict-item";

      const label = document.createElement("strong");
      label.textContent = group.term || term;
      item.appendChild(label);

      const fromTerms = new Set();
      for (const entry of group.entries) {
        const normalizedLookupTerm = normalizeDictionaryTerm(entry.lookupTerm);
        if (normalizedLookupTerm && normalizedLookupTerm !== group.rowTermNorm) {
          fromTerms.add(entry.lookupTerm);
        }
      }

      if (fromTerms.size === 1) {
        const [singleFrom] = Array.from(fromTerms);
        const match = document.createElement("small");
        match.className = "subtitle-dict-match";
        match.textContent = `from: ${singleFrom}`;
        item.appendChild(match);
      }

      const defs = document.createElement("ul");
      defs.className = "subtitle-dict-def-list";
      for (const entry of group.entries) {
        const defItem = document.createElement("li");
        defItem.className = "subtitle-dict-def-item";

        const detail = document.createElement("span");
        const rawDefinition = String(entry.definition || "").trim();
        detail.textContent = rawDefinition.length > 260
          ? `${rawDefinition.slice(0, 260)}...`
          : rawDefinition;
        defItem.appendChild(detail);

        if (fromTerms.size > 1) {
          const normalizedLookupTerm = normalizeDictionaryTerm(entry.lookupTerm);
          if (normalizedLookupTerm && normalizedLookupTerm !== group.rowTermNorm) {
            const from = document.createElement("small");
            from.className = "subtitle-dict-from";
            from.textContent = `from: ${entry.lookupTerm}`;
            defItem.appendChild(from);
          }
        }
        defs.appendChild(defItem);
      }

      item.appendChild(defs);
      list.appendChild(item);
    }
    elements.subtitleDictPopup.appendChild(list);
  }

  elements.subtitleDictPopup.classList.remove("hidden");
  elements.subtitleDictPopup.setAttribute("aria-hidden", "false");
  requestAnimationFrame(() => positionSubtitleDictionaryPopup(anchorEl));
}

function renderSubtitleDictionaryPopupLoading(term, anchorEl) {
  elements.subtitleDictPopup.textContent = "";
  const title = document.createElement("p");
  title.className = "subtitle-dict-title";
  title.textContent = `Dictionary: ${term}`;
  const loading = document.createElement("p");
  loading.className = "subtitle-dict-empty";
  loading.textContent = "辞書を検索中...";
  elements.subtitleDictPopup.appendChild(title);
  elements.subtitleDictPopup.appendChild(loading);
  elements.subtitleDictPopup.classList.remove("hidden");
  elements.subtitleDictPopup.setAttribute("aria-hidden", "false");
  requestAnimationFrame(() => positionSubtitleDictionaryPopup(anchorEl));
}

async function lookupDictionary(term) {
  const normalized = normalizeDictionaryTerm(term);
  if (!normalized) {
    return {
      term,
      normalized: "",
      results: [],
    };
  }
  const cached = state.dictLookupCache.get(normalized);
  if (cached) {
    return cached;
  }
  const params = new URLSearchParams({
    term,
    limit: "6",
  });
  const payload = await apiRequest(`/api/dictionary?${params.toString()}`);
  state.dictLookupCache.set(normalized, payload);
  return payload;
}

async function lookupDictionaryBatch(terms, limit = DICT_CONTEXT_PER_TERM_LIMIT, exactOnly = false, ftsMode = "all") {
  if (state.dictBatchApiAvailable === false) {
    throw new Error("Dictionary batch API is unavailable");
  }
  const cleanedTerms = [];
  const seenTerms = new Set();
  for (const rawTerm of terms) {
    const value = String(rawTerm || "").trim();
    const normalized = normalizeDictionaryTerm(value);
    if (!value || !normalized || seenTerms.has(normalized)) {
      continue;
    }
    seenTerms.add(normalized);
    cleanedTerms.push(value);
  }
  if (!cleanedTerms.length) {
    return [];
  }

  const params = new URLSearchParams();
  for (const term of cleanedTerms) {
    params.append("term", term);
  }
  params.set("limit", String(Math.max(1, Math.min(20, Number(limit) || DICT_CONTEXT_PER_TERM_LIMIT))));
  if (exactOnly) {
    params.set("exact_only", "1");
  }
  if (ftsMode && ftsMode !== "all") {
    params.set("fts_mode", String(ftsMode));
  }

  let payload;
  try {
    payload = await apiRequest(`/api/dictionary/batch?${params.toString()}`);
    state.dictBatchApiAvailable = true;
  } catch (error) {
    const message = String(error?.message || "");
    if (message.includes("Not found") || message.includes("404")) {
      state.dictBatchApiAvailable = false;
    }
    throw error;
  }
  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!exactOnly) {
    for (const item of items) {
      const normalized = normalizeDictionaryTerm(item?.normalized || item?.term || "");
      if (!normalized) {
        continue;
      }
      state.dictLookupCache.set(normalized, item);
    }
  }
  return items;
}

function splitNormalizedWords(value) {
  const normalized = normalizeDictionaryTerm(value);
  if (!normalized) {
    return [];
  }
  return normalized.split(/\s+/).filter(Boolean);
}

function deriveDictionaryCoreTerms(baseTerm) {
  const normalizedBase = normalizeDictionaryTerm(baseTerm);
  if (!normalizedBase) {
    return [];
  }

  const terms = [];
  const seen = new Set();
  const add = (value) => {
    const normalized = normalizeDictionaryTerm(value);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    terms.push(normalized);
  };

  add(normalizedBase);
  add(DICT_IRREGULAR_BASE_FORMS.get(normalizedBase) || "");

  if (normalizedBase.endsWith("'s")) {
    add(normalizedBase.slice(0, -2));
  }
  if (normalizedBase.endsWith("ies") && normalizedBase.length > 4) {
    add(`${normalizedBase.slice(0, -3)}y`);
  }
  if (normalizedBase.endsWith("ing") && normalizedBase.length > 5) {
    const stem = normalizedBase.slice(0, -3);
    add(stem);
    add(`${stem}e`);
  }
  if (normalizedBase.endsWith("ed") && normalizedBase.length > 4) {
    const stem = normalizedBase.slice(0, -2);
    add(stem);
    add(`${stem}e`);
  }
  if (normalizedBase.endsWith("es") && normalizedBase.length > 4) {
    add(normalizedBase.slice(0, -2));
  }
  if (normalizedBase.endsWith("s") && normalizedBase.length > 3) {
    add(normalizedBase.slice(0, -1));
  }

  return terms.slice(0, 3);
}

function buildDictionaryLookupTerms(wordEl, baseTerm) {
  const terms = [];
  const seen = new Set();
  const wordNodes = Array.from(elements.subtitleOverlay.querySelectorAll(".subtitle-word"));
  const index = wordNodes.indexOf(wordEl);
  const words = wordNodes.map((node) => String(node.dataset.dictTerm || node.textContent || "").trim());
  const coreTerms = deriveDictionaryCoreTerms(baseTerm);

  const addTerm = (rawTerm) => {
    const value = String(rawTerm || "").trim();
    if (!value) {
      return;
    }
    const normalized = normalizeDictionaryTerm(value);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    terms.push(normalized);
  };

  const addRange = (startIndex, endIndex) => {
    if (startIndex < 0 || endIndex < startIndex || endIndex >= words.length) {
      return;
    }
    const phrase = words
      .slice(startIndex, endIndex + 1)
      .map((word) => String(word || "").trim())
      .filter(Boolean)
      .join(" ");
    addTerm(phrase);
  };

  const wordAt = (wordIndex) => normalizeDictionaryTerm(words[wordIndex] || "");

  const addCollapsedForwardPhrasalCandidate = () => {
    if (index < 0 || index >= words.length) {
      return;
    }
    const head = wordAt(index);
    if (!head) {
      return;
    }
    const maxTail = Math.min(words.length - 1, index + 4);
    for (let tailIndex = index + 2; tailIndex <= maxTail; tailIndex += 1) {
      const tail = wordAt(tailIndex);
      if (!tail || !DICT_COLLAPSE_PARTICLE_WORDS.has(tail)) {
        continue;
      }
      addTerm(`${head} ${tail}`);
    }
  };

  const addCollapsedBackwardPhrasalCandidate = () => {
    if (index < 0 || index >= words.length) {
      return;
    }
    const tail = wordAt(index);
    if (!tail || !DICT_COLLAPSE_PARTICLE_WORDS.has(tail)) {
      return;
    }
    const minHead = Math.max(0, index - 4);
    for (let headIndex = index - 1; headIndex >= minHead; headIndex -= 1) {
      const head = wordAt(headIndex);
      if (!head || DICT_COLLAPSE_PARTICLE_WORDS.has(head)) {
        continue;
      }
      addTerm(`${head} ${tail}`);
      break;
    }
  };

  const addCoreHeadPhraseVariants = () => {
    if (!coreTerms.length) {
      return;
    }
    const primaryCore = coreTerms[0];
    const alternateHeads = coreTerms.slice(1).filter(Boolean);
    if (!primaryCore || !alternateHeads.length) {
      return;
    }
    const snapshot = [...terms];
    for (const term of snapshot) {
      const tokens = splitNormalizedWords(term);
      if (tokens.length < 2 || tokens[0] !== primaryCore) {
        continue;
      }
      const tail = tokens.slice(1).join(" ");
      for (const altHead of alternateHeads) {
        addTerm(`${altHead} ${tail}`);
      }
    }
  };

  if (index >= 0 && words.length > 0) {
    addRange(index, index + 1);
    addRange(index, index + 2);
    addRange(index, index + 3);
    addCollapsedForwardPhrasalCandidate();
    addCollapsedBackwardPhrasalCandidate();
  }
  addCoreHeadPhraseVariants();

  for (const coreTerm of coreTerms) {
    addTerm(coreTerm);
  }

  let lookupTerms = terms;
  if (terms.length > DICT_CONTEXT_MAX_CANDIDATES) {
    lookupTerms = terms.slice(0, DICT_CONTEXT_MAX_CANDIDATES);
    let replaceIndex = lookupTerms.length - 1;
    for (const coreTerm of coreTerms) {
      if (lookupTerms.includes(coreTerm)) {
        continue;
      }
      if (replaceIndex < 0) {
        break;
      }
      lookupTerms[replaceIndex] = coreTerm;
      replaceIndex -= 1;
    }
  }

  const contextWordSet = new Set();
  if (index >= 0 && words.length > 0) {
    const startIndex = Math.max(0, index - 3);
    const endIndex = Math.min(words.length - 1, index + 5);
    for (let wordIndex = startIndex; wordIndex <= endIndex; wordIndex += 1) {
      const tokens = splitNormalizedWords(words[wordIndex]);
      for (const token of tokens) {
        contextWordSet.add(token);
      }
    }
  }
  for (const coreTerm of coreTerms) {
    for (const token of splitNormalizedWords(coreTerm)) {
      contextWordSet.add(token);
    }
  }

  return {
    lookupTerms,
    coreTerms,
    contextWordSet,
  };
}

function dictionaryResultKey(row) {
  const id = Number(row?.id);
  if (Number.isFinite(id) && id > 0) {
    return `id:${id}`;
  }
  return [
    String(row?.source_name || ""),
    String(row?.term_norm || ""),
    String(row?.definition || ""),
  ].join("\u0000");
}

function scoreDictionaryCandidate(row, lookupTerm, contextWordSet, coreTermSet) {
  const lookupNormalized = normalizeDictionaryTerm(lookupTerm);
  const rowTermNormalized = normalizeDictionaryTerm(row?.term_norm || row?.term || "");
  const lookupWords = splitNormalizedWords(lookupNormalized);
  const rowWords = splitNormalizedWords(rowTermNormalized);

  let overlapCount = 0;
  for (const word of rowWords) {
    if (contextWordSet.has(word)) {
      overlapCount += 1;
    }
  }

  const isCoreLookup = coreTermSet.has(lookupNormalized);
  const isCoreEntry = isCoreLookup && rowTermNormalized === lookupNormalized;
  const isExactLookup = lookupNormalized && rowTermNormalized === lookupNormalized;
  const isPrefixLookup = (
    !isExactLookup
    && lookupNormalized
    && rowTermNormalized.startsWith(`${lookupNormalized} `)
  );

  let score = 0;
  score += overlapCount * 100;
  score += lookupWords.length * 18;
  score += rowWords.length * 3;
  if (isExactLookup) {
    score += 36;
  }
  if (isPrefixLookup) {
    score += 14;
  }
  if (isCoreLookup) {
    score += 10;
  }
  if (!isCoreLookup && lookupWords.length <= 2 && overlapCount <= 2) {
    score -= 40;
  }

  return {
    score,
    lookupNormalized,
    rowTermNormalized,
    isCoreEntry,
    isCoreLookup,
    overlapCount,
  };
}

async function lookupDictionaryWithContext(wordEl, baseTerm) {
  const context = buildDictionaryLookupTerms(wordEl, baseTerm);
  const lookupTerms = context.lookupTerms;
  const coreTermSet = new Set(context.coreTerms);
  const baseNormalized = context.coreTerms[0] || normalizeDictionaryTerm(baseTerm);
  const contextTerms = lookupTerms.filter((term) => normalizeDictionaryTerm(term) !== baseNormalized);
  const basePromise = lookupDictionary(baseNormalized || baseTerm);
  const contextPromise = (async () => {
    if (!contextTerms.length) {
      return new Map();
    }

    const exactTerms = [];
    const broadTerms = [];
    for (const term of contextTerms) {
      if (splitNormalizedWords(term).length >= 3) {
        broadTerms.push(term);
      } else {
        exactTerms.push(term);
      }
    }

    // Fast path: exact for short terms, broader search for longer phrase terms.
    try {
      const mapped = new Map();
      const tasks = [];
      if (exactTerms.length) {
        tasks.push(
          lookupDictionaryBatch(exactTerms, DICT_CONTEXT_PER_TERM_LIMIT, true).then((items) => {
            for (const item of items) {
              const key = normalizeDictionaryTerm(item?.term || item?.normalized || "");
              if (!key || mapped.has(key)) {
                continue;
              }
              mapped.set(key, item);
            }
          })
        );
      }
      if (broadTerms.length) {
        tasks.push(
          lookupDictionaryBatch(broadTerms, DICT_CONTEXT_PER_TERM_LIMIT, false, "term").then((items) => {
            for (const item of items) {
              const key = normalizeDictionaryTerm(item?.term || item?.normalized || "");
              if (!key || mapped.has(key)) {
                continue;
              }
              mapped.set(key, item);
            }
          })
        );
      }
      if (tasks.length) {
        await Promise.all(tasks);
      }
      return mapped;
    } catch (_error) {
      // Backward compatible fallback for older running servers.
      const mapped = new Map();
      for (const term of contextTerms) {
        const payload = await lookupDictionary(term);
        mapped.set(normalizeDictionaryTerm(term), payload);
      }
      return mapped;
    }
  })();

  const [basePayload, contextPayloadByTerm] = await Promise.all([basePromise, contextPromise]);
  const candidates = [];
  const seenRows = new Set();

  for (const lookupTerm of lookupTerms) {
    const lookupNormalized = normalizeDictionaryTerm(lookupTerm);
    const payload = lookupNormalized === baseNormalized
      ? basePayload
      : contextPayloadByTerm.get(lookupNormalized);
    const rows = Array.isArray(payload?.results) ? payload.results : [];
    if (!rows.length) {
      continue;
    }

    let insertedForTerm = 0;
    for (const row of rows) {
      const key = dictionaryResultKey(row);
      if (seenRows.has(key)) {
        continue;
      }
      seenRows.add(key);

      const scored = scoreDictionaryCandidate(
        row,
        lookupTerm,
        context.contextWordSet,
        coreTermSet,
      );
      const lookupTokens = splitNormalizedWords(scored.lookupNormalized);
      if (!scored.isCoreLookup && lookupTokens.length >= 3) {
        const rowTokens = splitNormalizedWords(scored.rowTermNormalized);
        const hasDirectTokenOverlap = rowTokens.some((token) => lookupTokens.includes(token));
        if (!hasDirectTokenOverlap) {
          continue;
        }
      }
      if (
        scored.isCoreLookup
        && !scored.isCoreEntry
        && splitNormalizedWords(scored.rowTermNormalized).length <= 1
        && !coreTermSet.has(scored.rowTermNormalized)
        && scored.overlapCount <= 0
      ) {
        continue;
      }
      candidates.push({
        ...row,
        lookup_term: lookupTerm,
        _row_key: key,
        _score: scored.score,
        _lookup_normalized: scored.lookupNormalized,
        _row_term_normalized: scored.rowTermNormalized,
        _is_core_entry: scored.isCoreEntry,
      });
      insertedForTerm += 1;
      if (insertedForTerm >= DICT_CONTEXT_PER_TERM_LIMIT) {
        break;
      }
    }
  }

  candidates.sort((left, right) => {
    if (right._score !== left._score) {
      return right._score - left._score;
    }
    const leftLookupWords = splitNormalizedWords(left._lookup_normalized).length;
    const rightLookupWords = splitNormalizedWords(right._lookup_normalized).length;
    if (rightLookupWords !== leftLookupWords) {
      return rightLookupWords - leftLookupWords;
    }
    return 0;
  });

  const selected = candidates.slice(0, DICT_CONTEXT_TOTAL_LIMIT);
  let selectedCoreCount = selected.filter((row) => row._is_core_entry).length;
  if (selectedCoreCount < DICT_CONTEXT_CORE_MIN_RESULTS) {
    const selectedKeys = new Set(selected.map((row) => row._row_key));
    for (const candidate of candidates) {
      if (!candidate._is_core_entry || selectedKeys.has(candidate._row_key)) {
        continue;
      }
      let replaceIndex = selected.length - 1;
      while (replaceIndex >= 0 && selected[replaceIndex]._is_core_entry) {
        replaceIndex -= 1;
      }
      if (replaceIndex < 0) {
        break;
      }
      selectedKeys.delete(selected[replaceIndex]._row_key);
      selected[replaceIndex] = candidate;
      selectedKeys.add(candidate._row_key);
      selectedCoreCount += 1;
      if (selectedCoreCount >= DICT_CONTEXT_CORE_MIN_RESULTS) {
        break;
      }
    }
  }

  selected.sort((left, right) => right._score - left._score);
  const mergedRows = selected.map((row) => {
    const result = { ...row };
    delete result._row_key;
    delete result._score;
    delete result._lookup_normalized;
    delete result._row_term_normalized;
    delete result._is_core_entry;
    return result;
  });

  return {
    term: baseTerm,
    results: mergedRows,
  };
}

async function showSubtitleDictionaryForWord(wordEl) {
  if (!wordEl) {
    return;
  }
  const term = String(wordEl.dataset.dictTerm || "").trim();
  if (!term) {
    return;
  }
  clearDictionaryPopupHideTimer();
  state.dictPopupWord = term;
  updateDictionaryPopupCueRange();
  const previousActive = elements.subtitleOverlay.querySelector(".subtitle-word.active");
  if (previousActive && previousActive !== wordEl) {
    previousActive.classList.remove("active");
  }
  wordEl.classList.add("active");
  renderSubtitleDictionaryPopupLoading(term, wordEl);

  const requestId = state.dictLookupRequestId + 1;
  state.dictLookupRequestId = requestId;
  try {
    const payload = await lookupDictionaryWithContext(wordEl, term);
    if (requestId !== state.dictLookupRequestId || state.dictPopupWord !== term) {
      return;
    }
    const rows = Array.isArray(payload.results) ? payload.results : [];
    renderSubtitleDictionaryPopup(term, rows, wordEl);
  } catch (error) {
    if (requestId !== state.dictLookupRequestId || state.dictPopupWord !== term) {
      return;
    }
    renderSubtitleDictionaryPopup(term, [], wordEl);
    setStatus(error.message, "error");
  }
}

function handleSubtitleOverlayPointerOver(event) {
  const target = event.target instanceof Element ? event.target.closest(".subtitle-word") : null;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  showSubtitleDictionaryForWord(target).catch((error) => setStatus(error.message, "error"));
}

function handleSubtitleOverlayPointerOut(event) {
  const fromWord = event.target instanceof Element ? event.target.closest(".subtitle-word") : null;
  if (!fromWord) {
    return;
  }
  const nextElement = event.relatedTarget instanceof Element ? event.relatedTarget : null;
  if (nextElement && (nextElement.closest(".subtitle-word") || nextElement.closest("#subtitleDictPopup"))) {
    return;
  }
  scheduleHideSubtitleDictionaryPopup();
}

function renderSubtitleOverlayText(text) {
  const value = String(text || "").trim();
  hideSubtitleDictionaryPopup();
  elements.subtitleOverlay.textContent = "";
  if (!value) {
    return;
  }
  SUBTITLE_WORD_PATTERN.lastIndex = 0;
  const fragment = document.createDocumentFragment();
  let cursor = 0;
  let hasWord = false;
  let match = SUBTITLE_WORD_PATTERN.exec(value);
  while (match) {
    const [word] = match;
    const start = match.index;
    const end = start + word.length;
    if (start > cursor) {
      fragment.appendChild(document.createTextNode(value.slice(cursor, start)));
    }
    const span = document.createElement("span");
    span.className = "subtitle-word";
    span.dataset.dictTerm = word;
    span.textContent = word;
    fragment.appendChild(span);
    cursor = end;
    hasWord = true;
    match = SUBTITLE_WORD_PATTERN.exec(value);
  }
  if (cursor < value.length) {
    fragment.appendChild(document.createTextNode(value.slice(cursor)));
  }
  if (!hasWord) {
    elements.subtitleOverlay.textContent = value;
    return;
  }
  elements.subtitleOverlay.appendChild(fragment);
}

function clearSubtitleOverlay(message = "字幕がありません") {
  renderSubtitleOverlayText(message);
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

function clampLyricReelPosition(position) {
  if (!state.cues.length) {
    return -1;
  }
  return Math.max(0, Math.min(state.cues.length - 1, position));
}

function clearLyricReelAnimationFrame() {
  if (state.lyricReelAnimationFrame !== null) {
    window.cancelAnimationFrame(state.lyricReelAnimationFrame);
    state.lyricReelAnimationFrame = null;
  }
  state.lyricReelAnimationPrevTs = 0;
}

function getCueStartMsAtLyricPosition(position) {
  const clamped = clampLyricReelPosition(position);
  if (clamped < 0) {
    return 0;
  }
  const lower = Math.floor(clamped);
  const upper = Math.min(state.cues.length - 1, Math.ceil(clamped));
  if (lower === upper) {
    return state.cues[lower].start_ms;
  }
  const ratio = clamped - lower;
  const lowerStart = state.cues[lower].start_ms;
  const upperStart = state.cues[upper].start_ms;
  return lowerStart + ((upperStart - lowerStart) * ratio);
}

function renderLyricReelAtPosition(position) {
  elements.lyricReelList.textContent = "";
  if (!state.cues.length || position < 0) {
    return;
  }

  const center = Math.round(position);
  const start = Math.max(0, center - 5);
  const end = Math.min(state.cues.length, center + 6);

  for (let idx = start; idx < end; idx += 1) {
    const cue = state.cues[idx];
    const distance = idx - position;
    const absDistance = Math.abs(distance);
    const line = document.createElement("p");
    line.className = "lyric-reel-line";
    if (absDistance < 0.42) {
      line.classList.add("active");
    } else if (absDistance >= 3) {
      line.dataset.distance = "far";
    }

    const translateY = distance * 44;
    const opacity = Math.max(0.16, 1 - (absDistance * 0.22));
    const scale = absDistance < 0.42
      ? 1.035
      : Math.max(0.82, 1 - (absDistance * 0.06));
    line.style.transform = `translateY(${translateY.toFixed(1)}px) scale(${scale.toFixed(3)})`;
    line.style.opacity = opacity.toFixed(3);
    line.style.zIndex = String(100 - Math.round(absDistance * 10));
    line.textContent = cue.text || "...";
    elements.lyricReelList.appendChild(line);
  }
}

function applyLyricReelVisualPosition(position) {
  const clamped = clampLyricReelPosition(position);
  if (clamped < 0) {
    return;
  }
  state.lyricReelVisualPosition = clamped;
  state.lyricReelIndex = Math.max(0, Math.min(state.cues.length - 1, Math.round(clamped)));
  state.activeCueIndex = state.lyricReelIndex;

  const cueStartMs = getCueStartMsAtLyricPosition(clamped);
  elements.videoPlayer.currentTime = cueStartMs / 1000;
  renderSubtitleOverlayText(state.cues[state.lyricReelIndex]?.text || "...");
  renderLyricReelAtPosition(clamped);
}

function animateLyricReel(frameTs = 0) {
  if (!state.lyricReelActive) {
    clearLyricReelAnimationFrame();
    return;
  }

  let frameScale = 1;
  if (state.lyricReelAnimationPrevTs > 0 && frameTs > state.lyricReelAnimationPrevTs) {
    const deltaMs = Math.min(64, frameTs - state.lyricReelAnimationPrevTs);
    frameScale = Math.max(0.5, deltaMs / 16.666);
  }
  state.lyricReelAnimationPrevTs = frameTs > 0 ? frameTs : performance.now();

  if (Math.abs(state.lyricReelInertiaVelocity) > LYRIC_REEL_INERTIA_MIN) {
    const rawTarget = state.lyricReelTargetPosition + (state.lyricReelInertiaVelocity * frameScale);
    const clampedTarget = clampLyricReelPosition(rawTarget);
    state.lyricReelTargetPosition = clampedTarget;
    if (Math.abs(clampedTarget - rawTarget) > 0.0001) {
      state.lyricReelInertiaVelocity = 0;
    } else {
      state.lyricReelInertiaVelocity *= Math.pow(LYRIC_REEL_INERTIA_DECAY, frameScale);
    }
  } else {
    state.lyricReelInertiaVelocity = 0;
  }

  const diff = state.lyricReelTargetPosition - state.lyricReelVisualPosition;
  if (Math.abs(diff) < 0.002 && state.lyricReelInertiaVelocity === 0) {
    applyLyricReelVisualPosition(state.lyricReelTargetPosition);
    clearLyricReelAnimationFrame();
    return;
  }

  const ease = Math.min(0.34, 0.22 + ((frameScale - 1) * 0.05));
  const nextPosition = state.lyricReelVisualPosition + (diff * ease);
  applyLyricReelVisualPosition(nextPosition);
  state.lyricReelAnimationFrame = window.requestAnimationFrame(animateLyricReel);
}

function setLyricReelTargetPosition(position, immediate = false) {
  const clamped = clampLyricReelPosition(position);
  if (clamped < 0) {
    return;
  }
  state.lyricReelTargetPosition = clamped;
  if (immediate || state.lyricReelVisualPosition < 0) {
    clearLyricReelAnimationFrame();
    applyLyricReelVisualPosition(clamped);
    return;
  }
  if (state.lyricReelInertiaVelocity === 0 && Math.abs(clamped - state.lyricReelVisualPosition) < 0.002) {
    return;
  }
  if (state.lyricReelAnimationFrame === null) {
    state.lyricReelAnimationFrame = window.requestAnimationFrame(animateLyricReel);
  }
}

function clearLyricReelAutoCloseTimer() {
  if (state.lyricReelAutoCloseTimer !== null) {
    window.clearTimeout(state.lyricReelAutoCloseTimer);
    state.lyricReelAutoCloseTimer = null;
  }
}

function scheduleLyricReelAutoClose() {
  clearLyricReelAutoCloseTimer();
  state.lyricReelAutoCloseTimer = window.setTimeout(() => {
    closeLyricReel({ resumePlayback: true });
  }, LYRIC_REEL_AUTO_CLOSE_MS);
}

function closeLyricReel(options = {}) {
  const { resumePlayback = false } = options;
  if (!state.lyricReelActive) {
    return;
  }
  clearLyricReelAutoCloseTimer();
  clearLyricReelAnimationFrame();
  state.lyricReelActive = false;
  state.lyricReelTargetPosition = -1;
  state.lyricReelVisualPosition = -1;
  state.lyricReelInertiaVelocity = 0;
  elements.phoneShell.classList.remove("lyric-reel-active");
  elements.lyricReelOverlay.classList.add("hidden");
  elements.lyricReelOverlay.setAttribute("aria-hidden", "true");
  if (resumePlayback && state.lyricReelResumeOnClose) {
    elements.videoPlayer.play().catch(() => {});
  }
  state.lyricReelResumeOnClose = false;
}

function openLyricReel() {
  if (!state.cues.length) {
    return false;
  }
  hideSubtitleDictionaryPopup();
  const wasPlaying = !elements.videoPlayer.paused;
  const currentMs = Math.round((elements.videoPlayer.currentTime || 0) * 1000);
  const activeIndex = findActiveCueIndex(currentMs);
  const initialIndex = activeIndex >= 0 ? activeIndex : findNearestCueIndex(currentMs);

  state.lyricReelActive = true;
  state.lyricReelResumeOnClose = wasPlaying;
  clearLyricReelAnimationFrame();
  state.lyricReelInertiaVelocity = 0;
  elements.videoPlayer.pause();
  elements.phoneShell.classList.add("lyric-reel-active");
  elements.lyricReelOverlay.classList.remove("hidden");
  elements.lyricReelOverlay.setAttribute("aria-hidden", "false");
  setLyricReelTargetPosition(initialIndex >= 0 ? initialIndex : 0, true);
  return true;
}

function stepLyricReel(step) {
  if (!state.lyricReelActive) {
    if (!openLyricReel()) {
      return false;
    }
  }
  scheduleLyricReelAutoClose();
  state.lyricReelInertiaVelocity = 0;
  const base = state.lyricReelTargetPosition >= 0
    ? state.lyricReelTargetPosition
    : state.lyricReelIndex;
  setLyricReelTargetPosition(base + step, false);
  return true;
}

function isNodeInsideDictionaryPopup(node) {
  let current = node;
  while (current) {
    if (current === elements.subtitleDictPopup) {
      return true;
    }
    current = current.parentNode;
  }
  return false;
}

function hasDictionarySelection() {
  const selection = window.getSelection();
  if (!selection || selection.isCollapsed || !selection.toString()) {
    return false;
  }
  return isNodeInsideDictionaryPopup(selection.anchorNode) || isNodeInsideDictionaryPopup(selection.focusNode);
}

function stepSubtitleCue(step) {
  if (!state.cues.length || !elements.videoPlayer.src) {
    return false;
  }
  const direction = step >= 0 ? 1 : -1;
  let baseIndex = state.activeCueIndex;
  if (baseIndex < 0 || baseIndex >= state.cues.length) {
    const currentMs = Math.round((elements.videoPlayer.currentTime || 0) * 1000);
    const active = findActiveCueIndex(currentMs);
    baseIndex = active >= 0 ? active : findNearestCueIndex(currentMs);
  }
  if (baseIndex < 0) {
    baseIndex = 0;
  }
  const targetIndex = Math.max(0, Math.min(state.cues.length - 1, baseIndex + direction));
  const cue = state.cues[targetIndex];
  if (!cue) {
    return false;
  }
  state.activeCueIndex = targetIndex;
  elements.videoPlayer.currentTime = Math.max(0, cue.start_ms / 1000);
  renderSubtitleOverlayText(cue.text || "...");
  return true;
}

function updateSubtitleFromPlayback() {
  if (state.lyricReelActive) {
    return;
  }
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
    renderSubtitleOverlayText("...");
    return;
  }
  renderSubtitleOverlayText(state.cues[index].text);
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
  applyOutputVolume();

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
      setLyricReelTargetPosition(nearestIndex, true);
    }
  }
  updateSubtitleFromPlayback();
}

function toggleMute() {
  state.userMuted = !state.userMuted;
  localStorage.setItem("substudy.muted", state.userMuted ? "true" : "false");
  applyOutputVolume();
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

function toggleNormalizationMode() {
  state.normalizationEnabled = !state.normalizationEnabled;
  localStorage.setItem("substudy.volume_normalization", state.normalizationEnabled ? "on" : "off");
  updateNormalizationToggle();
  applyOutputVolume();
  setStatus(`音量正規化を${state.normalizationEnabled ? "ON" : "OFF"}にしました。`, "ok");
}

function toggleDictionaryHoverLoopMode() {
  state.dictHoverLoopEnabled = !state.dictHoverLoopEnabled;
  localStorage.setItem("substudy.dict_hover_loop", state.dictHoverLoopEnabled ? "on" : "off");
  if (!state.dictHoverLoopEnabled) {
    stopDictionaryHoverLoop();
  }
  updateDictHoverLoopToggle();
  setStatus(`辞書ループを${state.dictHoverLoopEnabled ? "ON" : "OFF"}にしました。`, "ok");
}

function handleWheel(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (target && target.closest("#subtitleDictPopup")) {
    clearDictionaryPopupHideTimer();
    return;
  }

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
    scheduleLyricReelAutoClose();
    const currentTarget = state.lyricReelTargetPosition >= 0
      ? state.lyricReelTargetPosition
      : state.lyricReelVisualPosition;
    const deltaLines = event.deltaY / LYRIC_WHEEL_STEP_DELTA;
    const velocityDelta = deltaLines * LYRIC_REEL_INERTIA_FACTOR;
    state.lyricReelInertiaVelocity = Math.max(
      -1.05,
      Math.min(1.05, state.lyricReelInertiaVelocity + velocityDelta)
    );
    setLyricReelTargetPosition(currentTarget + deltaLines, false);
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
  if (event.metaKey || event.ctrlKey || event.altKey) {
    return;
  }
  if (
    event.shiftKey &&
    (event.key === "ArrowUp" || event.key === "ArrowDown") &&
    hasDictionarySelection()
  ) {
    return;
  }
  if (state.lyricReelActive) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeLyricReel({ resumePlayback: true });
      return;
    }
    if (event.key === "ArrowDown" || key === "j" || event.key === "ArrowRight") {
      event.preventDefault();
      stepLyricReel(1);
      return;
    }
    if (event.key === "ArrowUp" || key === "k" || event.key === "ArrowLeft") {
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

  if (event.shiftKey && event.key === "ArrowDown") {
    event.preventDefault();
    nextVideo().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (event.shiftKey && event.key === "ArrowUp") {
    event.preventDefault();
    prevVideo().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (event.key === "ArrowDown") {
    event.preventDefault();
    stepSubtitleCue(1);
    return;
  }

  if (event.key === "ArrowUp") {
    event.preventDefault();
    stepSubtitleCue(-1);
    return;
  }

  if (key === "j") {
    event.preventDefault();
    nextVideo().catch((error) => setStatus(error.message, "error"));
    return;
  }

  if (key === "k") {
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

  if (key === "n") {
    event.preventDefault();
    toggleNormalizationMode();
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

  elements.phoneShell.addEventListener("click", (event) => {
    const clickedElement = event.target instanceof Element ? event.target : null;
    if (
      clickedElement &&
      clickedElement.closest("button, input, select, textarea, a, label, .subtitle-word, .subtitle-dict-popup")
    ) {
      return;
    }
    if (state.lyricReelActive) {
      closeLyricReel({ resumePlayback: true });
      return;
    }
    togglePlayPause();
  });
  elements.subtitleOverlay.addEventListener("pointerover", handleSubtitleOverlayPointerOver);
  elements.subtitleOverlay.addEventListener("pointerout", handleSubtitleOverlayPointerOut);
  elements.subtitleOverlay.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target.closest(".subtitle-word") : null;
    if (target) {
      event.stopPropagation();
    }
  });
  elements.subtitleDictPopup.addEventListener("pointerenter", () => {
    clearDictionaryPopupHideTimer();
    startDictionaryHoverLoop();
  });
  elements.subtitleDictPopup.addEventListener("pointerleave", () => {
    stopDictionaryHoverLoop();
    scheduleHideSubtitleDictionaryPopup();
  });
  elements.subtitleDictPopup.addEventListener("click", (event) => event.stopPropagation());
  elements.subtitleDictPopup.addEventListener(
    "wheel",
    (event) => {
      event.stopPropagation();
      clearDictionaryPopupHideTimer();
    },
    { passive: true }
  );

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
  elements.lyricReelOverlay.addEventListener("click", (event) => {
    event.stopPropagation();
    closeLyricReel({ resumePlayback: true });
  });

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
  if (elements.normalizationToggle) {
    elements.normalizationToggle.addEventListener("click", () => {
      toggleNormalizationMode();
      resetControlsToggleFade();
    });
  }
  if (elements.dictHoverLoopToggle) {
    elements.dictHoverLoopToggle.addEventListener("click", () => {
      toggleDictionaryHoverLoopMode();
      resetControlsToggleFade();
    });
  }
  elements.controlsToggleBtn.addEventListener("click", () => toggleControlsDrawer());
  elements.controlsToggleBtn.addEventListener("pointerenter", () => showControlsToggleButton());
  elements.controlsToggleBtn.addEventListener("focus", () => showControlsToggleButton());
  elements.controlsToggleBtn.addEventListener("blur", () => resetControlsToggleFade());

  elements.prevBtn.addEventListener("click", () => {
    if (state.lyricReelActive) {
      stepLyricReel(-1);
      resetControlsToggleFade();
      return;
    }
    prevVideo().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.seekBackBtn.addEventListener("click", () => {
    if (state.lyricReelActive) {
      stepLyricReel(-1);
      resetControlsToggleFade();
      return;
    }
    seekBySeconds(-SEEK_SECONDS);
    resetControlsToggleFade();
  });
  elements.nextBtn.addEventListener("click", () => {
    if (state.lyricReelActive) {
      stepLyricReel(1);
      resetControlsToggleFade();
      return;
    }
    nextVideo().catch((error) => setStatus(error.message, "error"));
    resetControlsToggleFade();
  });
  elements.seekForwardBtn.addEventListener("click", () => {
    if (state.lyricReelActive) {
      stepLyricReel(1);
      resetControlsToggleFade();
      return;
    }
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
    state.userVolume = safeVolume;
    if (safeVolume > 0) {
      state.userMuted = false;
    }
    localStorage.setItem("substudy.volume", String(state.userVolume));
    localStorage.setItem("substudy.muted", state.userMuted ? "true" : "false");
    applyOutputVolume();
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

  elements.videoPlayer.addEventListener("timeupdate", () => {
    enforceDictionaryHoverLoop();
    updateSubtitleFromPlayback();
  });
  elements.videoPlayer.addEventListener("play", () => {
    closeLyricReel();
    updatePlayPauseButton();
  });
  elements.videoPlayer.addEventListener("pause", () => {
    stopDictionaryHoverLoop();
    updatePlayPauseButton();
  });
  elements.videoPlayer.addEventListener("volumechange", () => {
    updateMuteButtonLabel();
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
  updateNormalizationToggle();
  updateDictHoverLoopToggle();
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
    setStatus("準備完了。字幕の英単語ホバーで辞書表示、Gでジャンプできます。", "ok");
  } catch (error) {
    setStatus(error.message, "error");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initialize();
});
