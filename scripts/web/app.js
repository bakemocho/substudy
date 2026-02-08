const SEEK_SECONDS = 5;

const state = {
  videos: [],
  sources: [],
  index: 0,
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
};

const elements = {
  sourceSelect: document.getElementById("sourceSelect"),
  autoplayToggle: document.getElementById("autoplayToggle"),
  videoPlayer: document.getElementById("videoPlayer"),
  phoneShell: document.getElementById("phoneShell"),
  subtitleOverlay: document.getElementById("subtitleOverlay"),
  countdownPanel: document.getElementById("countdownPanel"),
  countdownValue: document.getElementById("countdownValue"),
  cancelCountdownBtn: document.getElementById("cancelCountdownBtn"),
  videoMetaDrawer: document.getElementById("videoMetaDrawer"),
  metaTabBtn: document.getElementById("metaTabBtn"),
  metaPanel: document.getElementById("metaPanel"),
  metaPrimaryLine: document.getElementById("metaPrimaryLine"),
  metaSecondaryLine: document.getElementById("metaSecondaryLine"),
  prevBtn: document.getElementById("prevBtn"),
  seekBackBtn: document.getElementById("seekBackBtn"),
  playPauseBtn: document.getElementById("playPauseBtn"),
  seekForwardBtn: document.getElementById("seekForwardBtn"),
  nextBtn: document.getElementById("nextBtn"),
  favoriteBtn: document.getElementById("favoriteBtn"),
  muteBtn: document.getElementById("muteBtn"),
  volumeSlider: document.getElementById("volumeSlider"),
  trackSelect: document.getElementById("trackSelect"),
  bookmarkCueBtn: document.getElementById("bookmarkCueBtn"),
  rangeStartBtn: document.getElementById("rangeStartBtn"),
  bookmarkRangeBtn: document.getElementById("bookmarkRangeBtn"),
  bookmarkNoteInput: document.getElementById("bookmarkNoteInput"),
  rangeStatus: document.getElementById("rangeStatus"),
  videoNote: document.getElementById("videoNote"),
  saveNoteBtn: document.getElementById("saveNoteBtn"),
  bookmarkList: document.getElementById("bookmarkList"),
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
  const video = currentVideo();
  if (!video || !state.currentTrackId) {
    state.cues = [];
    state.activeCueIndex = -1;
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

async function openVideo(index, autoplay = true) {
  if (!state.videos.length) {
    return;
  }

  clearCountdown();
  state.index = Math.max(0, Math.min(index, state.videos.length - 1));

  const video = currentVideo();
  elements.videoPlayer.src = video.media_url;
  elements.videoPlayer.load();

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

async function loadFeed(sourceId = "") {
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
    clearSubtitleOverlay("動画がありません");
    setVideoMetaFallback("動画が見つかりません", "0 / 0");
    state.bookmarks = [];
    renderBookmarkList();
    setStatus("条件に一致する動画がありません。", "error");
    return;
  }

  await openVideo(0, true);
  setStatus(`${state.videos.length}件の動画を読み込みました。`, "ok");
}

async function nextVideo() {
  if (!state.videos.length) {
    return;
  }
  if (state.index >= state.videos.length - 1) {
    setStatus("最後の動画です。", "info");
    return;
  }
  await openVideo(state.index + 1, true);
}

async function prevVideo() {
  if (!state.videos.length) {
    return;
  }
  if (state.index <= 0) {
    setStatus("最初の動画です。", "info");
    return;
  }
  await openVideo(state.index - 1, true);
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
  updateSubtitleFromPlayback();
}

function toggleMute() {
  elements.videoPlayer.muted = !elements.videoPlayer.muted;
  localStorage.setItem("substudy.muted", elements.videoPlayer.muted ? "true" : "false");
  updateMuteButtonLabel();
}

function handleWheel(event) {
  const now = Date.now();
  if (now < state.wheelLockUntil) {
    return;
  }

  if (Math.abs(event.deltaY) < 36) {
    return;
  }

  state.wheelLockUntil = now + 420;
  if (event.deltaY > 0) {
    nextVideo().catch((error) => setStatus(error.message, "error"));
  } else {
    prevVideo().catch((error) => setStatus(error.message, "error"));
  }
}

function handleKeydown(event) {
  const activeTag = document.activeElement ? document.activeElement.tagName : "";
  const isTyping = ["INPUT", "TEXTAREA", "SELECT"].includes(activeTag);
  if (isTyping) {
    return;
  }

  const key = event.key.toLowerCase();
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
  elements.sourceSelect.addEventListener("change", () => {
    loadFeed(elements.sourceSelect.value).catch((error) => {
      setStatus(error.message, "error");
    });
  });

  elements.autoplayToggle.addEventListener("click", () => {
    state.autoplayContinuous = !state.autoplayContinuous;
    localStorage.setItem("substudy.autoplay", state.autoplayContinuous ? "on" : "off");
    updateAutoplayToggle();
    setStatus(`連続再生を${state.autoplayContinuous ? "ON" : "OFF"}にしました。`, "ok");
  });

  elements.prevBtn.addEventListener("click", () => {
    prevVideo().catch((error) => setStatus(error.message, "error"));
  });
  elements.seekBackBtn.addEventListener("click", () => seekBySeconds(-SEEK_SECONDS));
  elements.nextBtn.addEventListener("click", () => {
    nextVideo().catch((error) => setStatus(error.message, "error"));
  });
  elements.seekForwardBtn.addEventListener("click", () => seekBySeconds(SEEK_SECONDS));
  elements.playPauseBtn.addEventListener("click", () => togglePlayPause());
  elements.favoriteBtn.addEventListener("click", () => {
    toggleFavorite().catch((error) => setStatus(error.message, "error"));
  });
  elements.metaTabBtn.addEventListener("click", () => toggleMetaDrawer());

  elements.muteBtn.addEventListener("click", () => toggleMute());
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
  });

  elements.trackSelect.addEventListener("change", () => {
    state.currentTrackId = elements.trackSelect.value || null;
    loadTrackCues().catch((error) => setStatus(error.message, "error"));
  });

  elements.bookmarkCueBtn.addEventListener("click", () => {
    bookmarkCurrentCue().catch((error) => setStatus(error.message, "error"));
  });
  elements.rangeStartBtn.addEventListener("click", () => markRangeStart());
  elements.bookmarkRangeBtn.addEventListener("click", () => {
    bookmarkCurrentRange().catch((error) => setStatus(error.message, "error"));
  });

  elements.saveNoteBtn.addEventListener("click", () => {
    saveVideoNote().catch((error) => setStatus(error.message, "error"));
  });

  elements.cancelCountdownBtn.addEventListener("click", () => {
    clearCountdown();
    setStatus("自動遷移をキャンセルしました。", "ok");
  });

  elements.videoPlayer.addEventListener("timeupdate", () => updateSubtitleFromPlayback());
  elements.videoPlayer.addEventListener("play", () => updatePlayPauseButton());
  elements.videoPlayer.addEventListener("pause", () => updatePlayPauseButton());
  elements.videoPlayer.addEventListener("volumechange", () => {
    localStorage.setItem("substudy.volume", String(elements.videoPlayer.volume || 0));
    localStorage.setItem("substudy.muted", elements.videoPlayer.muted ? "true" : "false");
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

  elements.phoneShell.addEventListener("wheel", handleWheel, { passive: true });
  bindTouchNavigation();
  document.addEventListener("keydown", handleKeydown);
}

async function initialize() {
  updateMetaDrawerState();
  updateAutoplayToggle();
  loadVolumeSettings();
  updatePlayPauseButton();
  bindEvents();

  try {
    await loadFeed("");
    setStatus("準備完了。上下移動と左右5秒シークで操作できます。", "ok");
  } catch (error) {
    setStatus(error.message, "error");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initialize();
});
