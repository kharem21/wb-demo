/* global deck, maplibregl */
(async function main() {
	const map = new maplibregl.Map({
		container: 'map',
		style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
		center: [0, 20],
		zoom: 1.6,
		pitch: 0,
		bearing: 0
	});

	map.addControl(new maplibregl.NavigationControl({ showCompass: false }));

	// Try to load latest 24h directly from API; fall back to local NDJSON if unavailable
	function coerceFloat(value) {
		if (value == null) return null;
		if (typeof value === 'number' && Number.isFinite(value)) return value;
		if (typeof value === 'string') {
			const m = value.trim().match(/[-+]?\d+(?:\.\d+)?/);
			if (m) {
				const n = parseFloat(m[0]);
				return Number.isFinite(n) ? n : null;
			}
		}
		return null;
	}
	function normalizeKey(name) {
		let k = String(name || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
		k = k.replace('longitude', 'lon').replace('long', 'lon').replace('lng', 'lon');
		k = k.replace('latitude', 'lat');
		k = k.replace('altitude', 'alt');
		k = k.replace('updatedat', 'time').replace('lastseen', 'time').replace('fixtime', 'time');
		k = k.replace('timestampms', 'timestamp').replace('timeunix', 'timestamp');
		return k;
	}
	function flattenObject(obj, prefix = '', out = {}) {
		if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
			for (const [k, v] of Object.entries(obj)) {
				const key = normalizeKey(prefix ? `${prefix}.${k}` : k);
				if (v && typeof v === 'object' && !Array.isArray(v)) {
					flattenObject(v, prefix ? `${prefix}.${k}` : k, out);
				} else {
					out[key] = v;
				}
			}
		}
		return out;
	}
	function extractLatLon(flat) {
		let lat = null, lon = null;
		if (flat.late7 != null) {
			const v = coerceFloat(flat.late7);
			if (v != null) lat = v / 1e7;
		}
		if (flat.lone7 != null) {
			const v = coerceFloat(flat.lone7);
			if (v != null) lon = v / 1e7;
		}
		if (flat.lnge7 != null) {
			const v = coerceFloat(flat.lnge7);
			if (v != null) lon = v / 1e7;
		}
		const latKeys = ['lat','latitude','gpslat','positionlat'];
		const lonKeys = ['lon','longitude','gpslon','positionlon'];
		if (lat == null) for (const k of latKeys) { if (flat[k] != null) { const v = coerceFloat(flat[k]); if (v != null) { lat = v; break; } } }
		if (lon == null) for (const k of lonKeys) { if (flat[k] != null) { const v = coerceFloat(flat[k]); if (v != null) { lon = v; break; } } }
		if (lat != null && (lat < -90 || lat > 90)) lat = null;
		if (lon != null && (lon < -180 || lon > 180)) lon = null;
		return [lat, lon];
	}
	function extractAltM(flat) {
		if (flat.altkm != null) { const v = coerceFloat(flat.altkm); if (v != null) return v * 1000; }
		for (const k of ['altm','altitudem','altitude','alt','msl','baroaltitude','elevation','height','agl']) {
			if (flat[k] != null) { const v = coerceFloat(flat[k]); if (v != null) return v; }
		}
		for (const k of ['altft','altitudeft','altfeet','feet','altftmsl']) {
			if (flat[k] != null) { const v = coerceFloat(flat[k]); if (v != null) return v * 0.3048; }
		}
		for (const [k, vv] of Object.entries(flat)) {
			if (typeof vv === 'string') {
				const m = vv.trim().toLowerCase().match(/([-+]?\d+(?:\.\d+)?)\s*(m|meter|meters|km|kilometer|kilometers|ft|feet)\b/);
				if (m) {
					const num = parseFloat(m[1]);
					const unit = m[2];
					if (unit.startsWith('km')) return num * 1000;
					if (unit.startsWith('ft') || unit.startsWith('feet')) return num * 0.3048;
					return num;
				}
			}
		}
		return null;
	}
	function parseTimeToIso(flat, sourceHour) {
		const candidates = ['time','timestamp','ts','datetime','date','lastseen','updated','updatedat','fixtime'];
		let val = null;
		for (const k of candidates) { if (flat[k] != null) { val = flat[k]; break; } }
		try {
			if (typeof val === 'number' && Number.isFinite(val)) {
				let sec = val;
				if (sec > 1e12) sec /= 1000; else if (sec > 1e10) sec /= 1000;
				return new Date(sec * 1000).toISOString();
			}
			if (typeof val === 'string' && val.trim()) {
				if (/^[-+]?\d+(?:\.\d+)?$/.test(val.trim())) {
					let sec = parseFloat(val.trim());
					if (sec > 1e12) sec /= 1000; else if (sec > 1e10) sec /= 1000;
					return new Date(sec * 1000).toISOString();
				}
				const d = new Date(val);
				if (!isNaN(d.getTime())) return new Date(d.getTime()).toISOString();
			}
		} catch {}
		// fallback: snapshot hour approximation
		const approx = new Date(Date.now() - sourceHour * 3600000);
		approx.setUTCMinutes(0, 0, 0);
		return approx.toISOString();
	}
	function extractId(parentKey, flat) {
		for (const k of ['id','balloonid','flightid','deviceid','serial','name','identifier','uuid']) {
			if (flat[k]) return String(flat[k]);
		}
		return parentKey ? String(parentKey) : null;
	}
	function enumerateRecords(obj) {
		const out = [];
		if (obj == null) return out;
		if (Array.isArray(obj)) {
			for (const rec of obj) {
				if (rec && typeof rec === 'object') out.push([null, rec]);
				else if (Array.isArray(rec) && rec.length >= 2) {
					const [lat, lon, altkm] = rec;
					const r = { lat, lon };
					if (altkm != null) r.altkm = altkm;
					out.push([null, r]);
				}
			}
			return out;
		}
		if (obj && typeof obj === 'object') {
			const lists = Object.values(obj).filter(v => Array.isArray(v));
			if (lists.length) {
				// Prefer arrays whose elements are objects or coordinate tuples over primitive arrays
				const scored = lists.map(arr => {
					let score = 0;
					if (arr.length) {
						const first = arr[0];
						if (first && typeof first === 'object' && !Array.isArray(first)) score = 3;
						else if (Array.isArray(first) && first.length >= 2) score = 2;
						else score = 1; // primitive array
					}
					return { arr, score };
				});
				const best = scored.sort((x, y) => (y.score - x.score) || (y.arr.length - x.arr.length))[0].arr;
				for (const rec of best) {
					if (rec && typeof rec === 'object' && !Array.isArray(rec)) {
						out.push([null, rec]);
					} else if (Array.isArray(rec) && rec.length >= 2) {
						const [lat, lon, altkm] = rec;
						const r = { lat, lon };
						if (altkm != null) r.altkm = altkm;
						out.push([null, r]);
					}
				}
				return out;
			}
			const allDicts = Object.values(obj).every(v => v && typeof v === 'object' && !Array.isArray(v));
			if (allDicts) { for (const [k, v] of Object.entries(obj)) out.push([k, v]); return out; }
			out.push([null, obj]);
			return out;
		}
		return out;
	}
	function safeJsonParse(text) {
		if (typeof text !== 'string') return null;
		let s = text.replace(/^[\uFEFF]/, '');
		// Remove obvious HTML wrappers by trimming to first bracketed block if present
		let startIdx = s.search(/[\[{]/);
		if (startIdx > 0) s = s.slice(startIdx);
		// Remove JS-style comments
		s = s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/.*$/gm, '$1');
		// Remove trailing commas before closing } or ]
		s = s.replace(/,\s*([\]\}])/g, '$1');
		try { return JSON.parse(s); } catch {}
		// Try to extract outermost balanced JSON and parse again
		try {
			const start = s.search(/[\[{]/);
			if (start >= 0) {
				let balance = 0, end = -1;
				for (let i = start; i < s.length; i++) {
					const ch = s[i];
					if (ch === '{' || ch === '[') balance++;
					if (ch === '}' || ch === ']') { balance--; if (balance === 0) { end = i + 1; break; } }
				}
				if (end > start) {
					const frag = s.slice(start, end);
					try { return JSON.parse(frag); } catch {}
					// As a last resort, try quoting simple unquoted keys
					const quoted = frag.replace(/([\{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:/g, '$1"$2":');
					try { return JSON.parse(quoted); } catch {}
				}
			}
		} catch {}
		return null;
	}
	const API_BASE = (window && window.WB_API_BASE) ? String(window.WB_API_BASE).replace(/\/$/, '') : 'https://a.windbornesystems.com/treasure';
	const DISABLE_API = typeof window !== 'undefined' && window.WB_DISABLE_API === true;
	async function fetchHour(hour) {
		const url = `${API_BASE}/${hour.toString().padStart(2,'0')}.json`;
		const ctrl = new AbortController();
		const t = setTimeout(() => ctrl.abort(), 15000);
		try {
			const res = await fetch(url, { cache: 'no-cache', signal: ctrl.signal });
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const text = await res.text();
			const json = safeJsonParse(text);
			return [hour, json, null];
		} catch (e) {
			return [hour, null, String(e && e.message || e)];
		} finally {
			clearTimeout(t);
		}
	}
	function standardize(parentKey, rec, sourceHour, rawIndex) {
		const flat = flattenObject(rec);
		const [lat, lon] = extractLatLon(flat);
		if (lat == null || lon == null) return null;
		const alt_m = extractAltM(flat);
		const time_iso = parseTimeToIso(flat, sourceHour);
		const id = extractId(parentKey, flat);
		return { id, lat, lon, alt_m, time_iso, source_hour: sourceHour, raw_index: rawIndex };
	}
	async function loadFromApi() {
		const statusEl = document.getElementById('dataStatus');
		if (statusEl) statusEl.textContent = 'Data: Fetching…';
		const hours = [...Array(24).keys()];
		const results = await Promise.all(hours.map(h => fetchHour(h)));
		results.sort((a,b) => a[0]-b[0]);
		const standardized = [];
		const seen = new Set();
		let okHours = 0;
		for (const [hour, data, err] of results) {
			if (!data) continue; else okHours++;
			let idx = 0;
			for (const [parentKey, rec] of enumerateRecords(data)) {
				const s = standardize(parentKey, rec, hour, idx++);
				if (!s) continue;
				const key = `${s.id || ''}|${s.time_iso}|${Math.round((s.lat||0)*1e6)}|${Math.round((s.lon||0)*1e6)}`;
				if (seen.has(key)) continue;
				seen.add(key);
				standardized.push(s);
			}
		}
		const feats = standardized.map(obj => ({
			type: 'Feature',
			geometry: { type: 'Point', coordinates: [obj.lon, obj.lat] },
			properties: {
				id: obj.id ?? null,
				time_iso: obj.time_iso ?? null,
				time_ms: obj.time_iso ? Date.parse(obj.time_iso) : null,
				alt_m: typeof obj.alt_m === 'number' ? obj.alt_m : null,
				source_hour: obj.source_hour ?? null
			}
		}));
		if (statusEl) statusEl.textContent = `Data: ${feats.length.toLocaleString('en-US')} pts`;
		return { type: 'FeatureCollection', features: feats };
	}
	async function loadFromNdjson() {
		try {
			const response = await fetch('../out/constellation.ndjson', { cache: 'no-cache' });
			if (!response.ok) throw new Error('Failed to load constellation.ndjson');
			const text = await response.text();
			const feats = [];
			for (const line of text.split('\n')) {
				if (!line.trim()) continue;
				try {
					const obj = JSON.parse(line);
					if (typeof obj.lon !== 'number' || typeof obj.lat !== 'number') continue;
					const t = obj.time_iso ? Date.parse(obj.time_iso) : null;
					feats.push({
						type: 'Feature',
						geometry: { type: 'Point', coordinates: [obj.lon, obj.lat] },
						properties: {
							id: obj.id ?? null,
							time_iso: obj.time_iso ?? null,
							time_ms: t,
							alt_m: typeof obj.alt_m === 'number' ? obj.alt_m : null,
							source_hour: obj.source_hour ?? null
						}
					});
				} catch {}
			}
			return { type: 'FeatureCollection', features: feats };
		} catch (e) {
			console.error(String(e));
			return { type: 'FeatureCollection', features: [] };
		}
	}
	async function loadConstellation() {
		const statusEl = document.getElementById('dataStatus');
		if (DISABLE_API) {
			const fc = await loadFromNdjson();
			if (statusEl) statusEl.textContent = `Data: ${(fc.features || []).length.toLocaleString('en-US')} pts`;
			return fc;
		}
		let fc = await loadFromApi();
		if (fc && Array.isArray(fc.features) && fc.features.length > 0) {
			return fc;
		}
		console.warn('API fetch failed or returned no data; falling back to local NDJSON');
		if (statusEl) statusEl.textContent = 'Data: API failed (CORS or parse). Using local…';
		fc = await loadFromNdjson();
		if (statusEl) statusEl.textContent = `Data: ${(fc.features || []).length.toLocaleString('en-US')} pts`;
		return fc;
	}
	const { type, features } = await loadConstellation();
	const data = { type, features };
	if (!Array.isArray(features) || features.length === 0) {
		// Nothing to render; leave map with no overlay and exit
		console.warn('No constellation data available');
		return;
	}

	// Timeline domain
	const times = features.map(f => f.properties.time_ms).filter(v => typeof v === 'number');
	const tMin = Math.min(...times);
	const tMax = Math.max(...times);
	// Default window 1h focused on the latest timestamp
	let windowMs = 1 * 60 * 60 * 1000; // default 1h
	let cursor = tMax;

	// UI wiring
	const playBtn = document.getElementById('play');
	const range = document.getElementById('time');
	const label = document.getElementById('timeLabel');
	const windowSel = document.getElementById('window');
	const liveToggle = document.getElementById('liveToggle');
	const liveStatusEl = document.getElementById('liveStatus');
	// Sidebar stats elements
	const statWindowEl = document.getElementById('stat-window');
	const statCountEl = document.getElementById('stat-count');
	// removed unique balloons stat
	const statAltMinEl = document.getElementById('stat-alt-min');
	const statInViewEl = document.getElementById('stat-in-view');
	const statAltP25El = document.getElementById('stat-alt-p25');
	const statAltMedEl = document.getElementById('stat-alt-med');
	const statAltP75El = document.getElementById('stat-alt-p75');
	const statAltMaxEl = document.getElementById('stat-alt-max');
	const statAltMeanEl = document.getElementById('stat-alt-mean');
	const statLatestEl = document.getElementById('stat-latest');
	// Distance modal elements
	const openDistancesBtn = document.getElementById('openDistances');
	const modalOverlay = document.getElementById('modalOverlay');
	const modalCloseBtn = document.getElementById('modalClose');
	const distModalCanvas = document.getElementById('distModalCanvas');
	const distModalMeta = document.getElementById('distModalMeta');
	const planeDistCanvas = document.getElementById('planeDistCanvas');
	const planeDistMeta = document.getElementById('planeDistMeta');
	function fmt(ts) {
		if (!Number.isFinite(ts)) return '—';
		return new Date(ts).toISOString().replace('T', ' ').replace(/\..+/, '');
	}
	function setLabel() {
		// Always display a 24h range next to the slider
		const displayStart = cursor - 24 * 60 * 60 * 1000;
		label.textContent = `${fmt(displayStart)} → ${fmt(cursor)}  (24h)`;
		// Sidebar shows the actual selected window
		if (statWindowEl) {
			const start = cursor - windowMs;
			statWindowEl.textContent = `${fmt(start)} → ${fmt(cursor)}  (${Math.round(windowMs/3600000)}h)`;
		}
	}
	const minFor24h = Number.isFinite(tMax) && Number.isFinite(tMin) ? Math.max(tMin, tMax - 24 * 60 * 60 * 1000) : tMin;
	range.min = `${minFor24h}`;
	range.max = `${tMax}`;
	range.step = '60000';
	range.value = `${cursor}`;
	// Ensure the UI reflects the 1h default selection
	if (windowSel) windowSel.value = '3600000';
	setLabel();
	windowSel.addEventListener('change', () => {
		windowMs = parseInt(windowSel.value, 10);
		updateLayer();
		setLabel();
		// If modal is open, update both histograms immediately
		if (modalOverlay && modalOverlay.style.display === 'flex') {
			const start = cursor - windowMs;
			const windowFeatures = features.filter(f => {
				const t = f.properties.time_ms;
				return typeof t === 'number' && t >= start && t <= cursor;
			});
			updateDistanceHistogram(windowFeatures, distModalCanvas, distModalMeta);
			updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
		}
	});
	range.addEventListener('input', () => {
		cursor = parseInt(range.value, 10);
		updateLayer();
		setLabel();
		if (modalOverlay && modalOverlay.style.display === 'flex') {
			const start = cursor - windowMs;
			const windowFeatures = features.filter(f => {
				const t = f.properties.time_ms;
				return typeof t === 'number' && t >= start && t <= cursor;
			});
			updateDistanceHistogram(windowFeatures, distModalCanvas, distModalMeta);
			updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
		}
	});

	function percentile(sorted, p) {
		if (!sorted.length) return null;
		const idx = (sorted.length - 1) * p;
		const lo = Math.floor(idx);
		const hi = Math.ceil(idx);
		if (lo === hi) return sorted[lo];
		const w = idx - lo;
		return sorted[lo] * (1 - w) + sorted[hi] * w;
	}

	function formatNum(n) {
		if (!Number.isFinite(n)) return '—';
		return n.toLocaleString('en-US');
	}

	function updateStats() {
		if (!statCountEl) return; // sidebar not present
		const start = cursor - windowMs;
		const windowFeatures = features.filter(f => {
			const t = f.properties.time_ms;
			return typeof t === 'number' && t >= start && t <= cursor;
		});
		statCountEl.textContent = formatNum(windowFeatures.length);
		// Compute balloons in current map view using latest-hour logic (same as histogram)
		if (statInViewEl && map && typeof map.getBounds === 'function') {
			let maxTs = null;
			for (const f of windowFeatures) {
				const t = f.properties.time_ms;
				if (typeof t === 'number' && (maxTs == null || t > maxTs)) maxTs = t;
			}
			let countInView = 0;
			if (maxTs != null) {
				const hourMs = 3600000;
				const bucketStart = Math.floor(maxTs / hourMs) * hourMs;
				const bucketEnd = bucketStart + hourMs;
				const hourFeatures = windowFeatures.filter(f => {
					const t = f.properties.time_ms;
					return typeof t === 'number' && t >= bucketStart && t < bucketEnd;
				});
				// Latest position per id within the hour
				const latestById = new Map();
				for (const f of hourFeatures) {
					const id = f.properties.id || null;
					const t = f.properties.time_ms;
					if (!id || !Number.isFinite(t)) continue;
					const prev = latestById.get(id);
					if (!prev || t > prev.properties.time_ms) latestById.set(id, f);
				}
				let latest = Array.from(latestById.values());
				if (!latest.length) {
					let latestTs = null;
					for (const f of hourFeatures) {
						const t = f.properties.time_ms;
						if (typeof t === 'number' && (latestTs == null || t > latestTs)) latestTs = t;
					}
					if (latestTs != null) latest = hourFeatures.filter(f => f.properties.time_ms === latestTs);
				}
				const b = map.getBounds();
				const south = b.getSouth();
				const north = b.getNorth();
				let west = b.getWest();
				let east = b.getEast();
				let width = east - west;
				if (width < 0) width += 360; // dateline wrap
				const inBounds = (lat, lon) => {
					const latOk = lat >= south && lat <= north;
					let lonOk;
					if (east >= west) lonOk = lon >= west && lon <= east; else lonOk = lon >= west || lon <= east;
					return latOk && lonOk;
				};
				countInView = latest
					.map(f => f.geometry.coordinates)
					.filter(coords => Array.isArray(coords) && coords.length >= 2 && inBounds(coords[1], coords[0]))
					.length;
			}
			statInViewEl.textContent = formatNum(countInView);
		}
		// unique balloons stat removed
		const alts = windowFeatures
			.map(f => (typeof f.properties.alt_m === 'number' ? f.properties.alt_m : NaN))
			.filter(v => Number.isFinite(v))
			.sort((a, b) => a - b);
		const altMin = alts.length ? alts[0] : null;
		const altMax = alts.length ? alts[alts.length - 1] : null;
		const altMean = alts.length ? (alts.reduce((s, v) => s + v, 0) / alts.length) : null;
		const altP25 = percentile(alts, 0.25);
		const altMed = percentile(alts, 0.5);
		const altP75 = percentile(alts, 0.75);
		const fmtAlt = v => (v == null ? '—' : `${Math.round(v)} m`);
		statAltMinEl.textContent = fmtAlt(altMin);
		statAltMaxEl.textContent = fmtAlt(altMax);
		statAltMeanEl.textContent = altMean == null ? '—' : `${Math.round(altMean)} m`;
		statAltP25El.textContent = fmtAlt(altP25);
		statAltMedEl.textContent = fmtAlt(altMed);
		statAltP75El.textContent = fmtAlt(altP75);
		let latestTs = null;
		for (const f of windowFeatures) {
			const t = f.properties.time_ms;
			if (typeof t === 'number' && (latestTs == null || t > latestTs)) latestTs = t;
		}
		statLatestEl.textContent = latestTs == null ? '—' : fmt(latestTs);

		// If modal is open, refresh its histograms
		if (modalOverlay && modalOverlay.style.display === 'flex') {
			updateDistanceHistogram(windowFeatures, distModalCanvas, distModalMeta);
			updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
		}
	}

	function updateDistanceHistogram(windowFeatures, canvas, metaEl) {
		if (!canvas || !metaEl) return;
		// Use ONLY the latest hour bucket within the current window
		let maxTs = null;
		for (const f of windowFeatures) {
			const t = f.properties.time_ms;
			if (typeof t === 'number' && (maxTs == null || t > maxTs)) maxTs = t;
		}
		if (maxTs == null) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'No data in window';
			return;
		}
		const hourMs = 3600000;
		const bucketStart = Math.floor(maxTs / hourMs) * hourMs;
		const bucketEnd = bucketStart + hourMs;
		const hourFeatures = windowFeatures.filter(f => {
			const t = f.properties.time_ms;
			return typeof t === 'number' && t >= bucketStart && t < bucketEnd;
		});
		// Build latest position per balloon id within this hour
		const latestById = new Map();
		for (const f of hourFeatures) {
			const id = f.properties.id || null;
			const t = f.properties.time_ms;
			if (!id || !Number.isFinite(t)) continue;
			const prev = latestById.get(id);
			if (!prev || t > prev.properties.time_ms) latestById.set(id, f);
		}
		let latest = Array.from(latestById.values());
		// Fallback: if IDs are missing, use all features from the latest timestamp within the hour
		if (!latest.length) {
			let latestTs = null;
			for (const f of hourFeatures) {
				const t = f.properties.time_ms;
				if (typeof t === 'number' && (latestTs == null || t > latestTs)) latestTs = t;
			}
			if (latestTs != null) {
				latest = hourFeatures.filter(f => f.properties.time_ms === latestTs);
			}
		}
		// Filter to current map viewport
		// Guard: map might not be ready
		if (!map || typeof map.getBounds !== 'function') {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'Map not ready';
			return;
		}
		const b = map.getBounds();
		const south = b.getSouth();
		const north = b.getNorth();
		let west = b.getWest();
		let east = b.getEast();
		let width = east - west;
		if (width < 0) width += 360; // dateline wrap
		const inBounds = (lat, lon) => {
			const latOk = lat >= south && lat <= north;
			let lonOk;
			if (east >= west) lonOk = lon >= west && lon <= east; else lonOk = lon >= west || lon <= east;
			return latOk && lonOk;
		};
		const positions = latest
			.map(f => f.geometry.coordinates)
			.filter(coords => Array.isArray(coords) && coords.length >= 2 && inBounds(coords[1], coords[0]));
		if (!positions.length) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'No balloons in view';
			return;
		}
		// Balloon-to-balloon pairwise distances within viewport
		if (positions.length < 2) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = `${positions.length} balloon(s) in view — not enough for pair distances`;
			return;
		}
		const distances = [];
		for (let i = 0; i < positions.length; i++) {
			const [lonA, latA] = positions[i];
			for (let j = i + 1; j < positions.length; j++) {
				const [lonB, latB] = positions[j];
				const d = haversineKm(latA, lonA, latB, lonB);
				if (Number.isFinite(d)) distances.push(d);
			}
		}
		const maxDist = distances.length ? Math.max(...distances) : 0;
		const binSize = chooseBinSize(maxDist);
		const binCount = Math.max(10, Math.min(60, Math.ceil((maxDist || 1) / binSize)));
		const bins = new Array(binCount).fill(0);
		for (const d of distances) {
			const idx = Math.min(binCount - 1, Math.floor(d / binSize));
			bins[idx] += 1;
		}
		renderHistogram(canvas, bins, binSize);
		const mean = distances.length ? (distances.reduce((s, v) => s + v, 0) / distances.length) : 0;
		const med = percentile([...distances].sort((a,b)=>a-b), 0.5);
		metaEl.textContent = `${positions.length} balloons in view, ${distances.length} pair distances, mean=${mean.toFixed(1)} km, median=${med ? med.toFixed(1) : '—'} km, bin=${binSize} km`;
	}

	function updatePlaneDistanceHistogram(windowFeatures, canvas, metaEl) {
		if (!canvas || !metaEl) return;
		if (!liveEnabled) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'Live aircraft disabled';
			return;
		}
		// Use ONLY the latest hour bucket within the current window for balloons
		let maxTs = null;
		for (const f of windowFeatures) {
			const t = f.properties.time_ms;
			if (typeof t === 'number' && (maxTs == null || t > maxTs)) maxTs = t;
		}
		if (maxTs == null) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'No data in window';
			return;
		}
		const hourMs = 3600000;
		const bucketStart = Math.floor(maxTs / hourMs) * hourMs;
		const bucketEnd = bucketStart + hourMs;
		const hourFeatures = windowFeatures.filter(f => {
			const t = f.properties.time_ms;
			return typeof t === 'number' && t >= bucketStart && t < bucketEnd;
		});
		// Build latest position per balloon id within this hour
		const latestById = new Map();
		for (const f of hourFeatures) {
			const id = f.properties.id || null;
			const t = f.properties.time_ms;
			if (!id || !Number.isFinite(t)) continue;
			const prev = latestById.get(id);
			if (!prev || t > prev.properties.time_ms) latestById.set(id, f);
		}
		let latest = Array.from(latestById.values());
		if (!latest.length) {
			let latestTs = null;
			for (const f of hourFeatures) {
				const t = f.properties.time_ms;
				if (typeof t === 'number' && (latestTs == null || t > latestTs)) latestTs = t;
			}
			if (latestTs != null) {
				latest = hourFeatures.filter(f => f.properties.time_ms === latestTs);
			}
		}
		// Filter to current map viewport and ensure zoomed-in area similar to live fetch guard
		if (!map || typeof map.getBounds !== 'function') {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'Map not ready';
			return;
		}
		const b = map.getBounds();
		const south = b.getSouth();
		const north = b.getNorth();
		let west = b.getWest();
		let east = b.getEast();
		let width = east - west;
		if (width < 0) width += 360; // dateline wrap
		const lamin = Math.max(-90, Math.min(90, south));
		const lamax = Math.max(-90, Math.min(90, north));
		const lomin = Math.max(-180, Math.min(180, west));
		const lomax = Math.max(-180, Math.min(180, east));
		const latSpan = Math.abs(lamax - lamin);
		const lonSpan = Math.abs(lomax - lomin);
		if (latSpan * lonSpan > 1000) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'Zoom in to enable live (area too large)';
			return;
		}
		const inBounds = (lat, lon) => {
			const latOk = lat >= south && lat <= north;
			let lonOk;
			if (east >= west) lonOk = lon >= west && lon <= east; else lonOk = lon >= west || lon <= east;
			return latOk && lonOk;
		};
		const balloonPositions = latest
			.map(f => f.geometry.coordinates)
			.filter(coords => Array.isArray(coords) && coords.length >= 2 && inBounds(coords[1], coords[0]));
		const aircraftPositions = (liveAircraft || [])
			.filter(a => Number.isFinite(a.lat) && Number.isFinite(a.lon) && inBounds(a.lat, a.lon))
			.map(a => [a.lon, a.lat]);
		if (!balloonPositions.length) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'No balloons in view';
			return;
		}
		if (!aircraftPositions.length) {
			renderHistogram(canvas, [], 1);
			metaEl.textContent = 'No aircraft in view (zoom in or wait)';
			return;
		}
		const distances = [];
		for (let i = 0; i < balloonPositions.length; i++) {
			const [lonB, latB] = balloonPositions[i];
			for (let j = 0; j < aircraftPositions.length; j++) {
				const [lonA, latA] = aircraftPositions[j];
				const d = haversineKm(latB, lonB, latA, lonA);
				if (Number.isFinite(d)) distances.push(d);
			}
		}
		const maxDist = distances.length ? Math.max(...distances) : 0;
		const binSize = chooseBinSize(maxDist);
		const binCount = Math.max(10, Math.min(60, Math.ceil((maxDist || 1) / binSize)));
		const bins = new Array(binCount).fill(0);
		for (const d of distances) {
			const idx = Math.min(binCount - 1, Math.floor(d / binSize));
			bins[idx] += 1;
		}
		renderHistogram(canvas, bins, binSize);
		const mean = distances.length ? (distances.reduce((s, v) => s + v, 0) / distances.length) : 0;
		const med = percentile([...distances].sort((a,b)=>a-b), 0.5);
		metaEl.textContent = `${balloonPositions.length} balloons × ${aircraftPositions.length} aircraft in view, ${distances.length} distances, mean=${mean.toFixed(1)} km, median=${med ? med.toFixed(1) : '—'} km, bin=${binSize} km`;
	}

	function chooseBinSize(maxDist) {
		if (!Number.isFinite(maxDist) || maxDist <= 0) return 1;
		const scales = [0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000];
		for (const s of scales) { if (maxDist / s <= 40) return s; }
		return 2000;
	}

	function renderHistogram(canvas, bins, binSize) {
		const ctx = canvas.getContext('2d');
		const rect = canvas.getBoundingClientRect();
		// Keep canvas internal size in sync with CSS size for crisp rendering
		if (Math.floor(rect.width) !== canvas.width || Math.floor(rect.height) !== canvas.height) {
			canvas.width = Math.floor(rect.width);
			canvas.height = Math.floor(rect.height);
		}
		const width = canvas.width;
		const height = canvas.height;
		ctx.clearRect(0, 0, width, height);
		const padL = 24, padR = 8, padT = 8, padB = 18;
		const w = width - padL - padR;
		const h = height - padT - padB;
		const maxBin = bins.length ? Math.max(...bins) : 0;
		if (!maxBin) {
			ctx.fillStyle = '#999';
			ctx.font = '12px system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif';
			ctx.fillText('No data', padL, padT + 14);
			return;
		}
		// Bars
		const barW = w / bins.length;
		for (let i = 0; i < bins.length; i++) {
			const v = bins[i];
			const barH = Math.max(1, Math.round((v / maxBin) * h));
			const x = padL + i * barW;
			const y = padT + (h - barH);
			ctx.fillStyle = '#3b82f6';
			ctx.fillRect(Math.round(x)+0.5, Math.round(y)+0.5, Math.max(1, Math.floor(barW - 1)), barH);
		}
		// X-axis ticks (every ~5 bins)
		ctx.fillStyle = '#444';
		ctx.font = '10px system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif';
		const step = Math.max(1, Math.round(bins.length / 5));
		ctx.textAlign = 'center';
		for (let i = 0; i < bins.length; i += step) {
			const x = padL + i * barW;
			const label = `${Math.round(i * binSize)}`;
			ctx.fillText(label, Math.round(x), height - 4);
		}
		// Axis label (place above ticks, right-aligned)
		ctx.fillStyle = '#666';
		ctx.textAlign = 'right';
		ctx.fillText('distance (km)', width - padR, height - 18);
		ctx.textAlign = 'start';
	}

	function haversineKm(lat1, lon1, lat2, lon2) {
		const R = 6371.0088; // km
		const toRad = d => d * Math.PI / 180;
		const dLat = toRad(lat2 - lat1);
		const dLon = toRad(lon2 - lon1);
		const a = Math.sin(dLat/2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon/2) ** 2;
		return 2 * R * Math.asin(Math.sqrt(a));
	}

	let playing = false;
	let rafId = null;
	playBtn.addEventListener('click', () => {
		playing = !playing;
		playBtn.textContent = playing ? '⏸' : '▶';
		if (playing) tick(); else cancelAnimationFrame(rafId);
	});

	// High-contrast palette (turbo-like), locked as the only ramp
	const currentColorRamp = [
		{ t: 0.0,  c: [48, 18, 59] },   // #30123b
		{ t: 0.2,  c: [0, 87, 255] },   // #0057ff
		{ t: 0.4,  c: [0, 215, 229] },  // #00d7e5
		{ t: 0.6,  c: [122, 245, 44] }, // #7af52c
		{ t: 0.8,  c: [255, 209, 0] },  // #ffd100
		{ t: 1.0,  c: [255, 56, 0] }    // #ff3800
	];

	const ALT_MIN = 0;
	const ALT_MAX = 22000;

	function lerp(a, b, t) { return a + (b - a) * t; }
	function sampleRamp(v) {
		const t = Math.max(0, Math.min(1, (v - ALT_MIN) / (ALT_MAX - ALT_MIN)));
		for (let i = 0; i < currentColorRamp.length - 1; i++) {
			const left = currentColorRamp[i];
			const right = currentColorRamp[i + 1];
			if (t <= right.t) {
				const local = (t - left.t) / (right.t - left.t);
				return [
					Math.round(lerp(left.c[0], right.c[0], local)),
					Math.round(lerp(left.c[1], right.c[1], local)),
					Math.round(lerp(left.c[2], right.c[2], local)),
					180
				];
			}
		}
		return [...currentColorRamp[currentColorRamp.length - 1].c, 180];
	}

	function rgbToHex(r, g, b) {
		const toHex = (n) => n.toString(16).padStart(2, '0');
		return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
	}

	function rampToCssGradient(ramp) {
		const stops = ramp.map(stop => `${rgbToHex(stop.c[0], stop.c[1], stop.c[2])} ${Math.round(stop.t * 100)}%`).join(', ');
		return `linear-gradient(90deg, ${stops})`;
	}

	function computeAlpha(f) {
		const t = f.properties.time_ms;
		if (typeof t !== 'number') return 0;
		const start = cursor - windowMs;
		if (t > cursor || t < start) return 0; // outside window → hidden
		const p = (t - start) / (cursor - start || 1);
		return Math.round(60 + 140 * p); // 60..200 within window
	}

	// Live aircraft state
	let liveEnabled = false;
	let liveTimerId = null;
	let liveDebounceId = null;
	let liveAbort = null;
	let lastLiveFetchMs = 0;
	const LIVE_MIN_INTERVAL_MS = 15000; // be gentle: >=10s recommended for anonymous usage
	let liveAircraft = [];

	function getLiveLayer() {
		if (!liveEnabled) return null;
		// Build a simple diamond SVG as a data URL and render with IconLayer
		const makeDiamondDataUrl = () => {
			const svg = [
				'<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32">',
				'<g fill="white">',
				// diamond centered at (16,16)
				'<path d="M16 2 L30 16 L16 30 L2 16 Z"/>',
				'</g>',
				'</svg>'
			].join('');
			return 'data:image/svg+xml;utf8,' + encodeURIComponent(svg);
		};
		const iconUrl = makeDiamondDataUrl();
		return new deck.IconLayer({
			id: 'live-aircraft',
			data: liveAircraft,
			pickable: true,
			opacity: 0.95,
			parameters: { depthTest: false },
			getPosition: d => [d.lon, d.lat],
			getIcon: () => ({
				url: iconUrl,
				width: 32,
				height: 32,
				anchorX: 16,
				anchorY: 16,
				mask: true
			}),
			// Slightly smaller; scale modestly with speed
			getSize: d => 12 + Math.min(6, (d.velocity ?? 0) / 80),
			sizeUnits: 'pixels',
			sizeMinPixels: 6,
			sizeMaxPixels: 22,
			getAngle: d => Number.isFinite(d.track) ? d.track : 0,
			getColor: d => {
				const col = sampleRamp(d.alt_m ?? 0);
				col[3] = 200; // slightly higher alpha for visibility
				return col;
			},
			onHover: ({ object, x, y }) => {
				const el = document.getElementById('tooltip');
				if (!el) return;
				if (object) {
					el.style.display = 'block';
					el.style.left = `${x + 12}px`;
					el.style.top = `${y + 12}px`;
					const id = object.callsign || object.icao24 || 'unknown';
					const alt = Number.isFinite(object.alt_m) ? `${Math.round(object.alt_m)} m` : 'n/a';
					el.innerHTML = `<strong>${id}</strong><br/>alt: ${alt}`;
				} else {
					el.style.display = 'none';
				}
			}
		});
	}

	function buildLayers() {
		const base = new deck.GeoJsonLayer({
			id: 'constellation-points',
			data,
			pointType: 'circle',
			pointRadiusUnits: 'pixels',
			extensions: [new deck.DataFilterExtension({ filterSize: 1 })],
			getFilterValue: f => (typeof f.properties.time_ms === 'number' ? f.properties.time_ms : -Infinity),
			filterRange: [cursor - windowMs, cursor],
			getPointRadius: f => {
				const alt = f.properties.alt_m ?? 0;
				return 2 + 4 * Math.max(0, Math.min(1, alt / ALT_MAX));
			},
			getFillColor: f => {
				const baseCol = sampleRamp(f.properties.alt_m ?? 0);
				baseCol[3] = computeAlpha(f);
				return baseCol;
			},
			stroked: true,
			getLineColor: [0, 0, 0, 60],
			lineWidthMinPixels: 1,
			pickable: true,
			autoHighlight: true,
			onHover: ({ object, x, y }) => {
				const el = document.getElementById('tooltip');
				if (!el) return;
				if (object) {
					el.style.display = 'block';
					el.style.left = `${x + 12}px`;
					el.style.top = `${y + 12}px`;
					const id = object.properties.id ?? 'unknown';
					const alt = object.properties.alt_m ?? 'n/a';
					const t = object.properties.time_iso ?? '';
					el.innerHTML = `<strong>${id}</strong><br/>alt: ${alt} m<br/>${t}`;
				} else {
					el.style.display = 'none';
				}
			},
			updateTriggers: {
				getFillColor: [cursor, windowMs],
				getFilterValue: [cursor, windowMs]
			}
		});
		const layers = [base];
		const live = getLiveLayer();
		if (live) layers.push(live);
		return layers;
	}

	function setLiveStatus(msg) {
		if (!liveStatusEl) return;
		liveStatusEl.textContent = msg || '';
	}

	async function fetchLiveOnce(force) {
		if (!liveEnabled) return;
		const now = Date.now();
		if (!force && now - lastLiveFetchMs < LIVE_MIN_INTERVAL_MS) return;
		if (liveAbort) {
			try { liveAbort.abort(); } catch {}
		}
		// Require reasonable bounds (avoid huge global queries)
		const b = map.getBounds();
		const lamin = Math.max(-90, Math.min(90, b.getSouth()));
		const lamax = Math.max(-90, Math.min(90, b.getNorth()));
		const lomin = Math.max(-180, Math.min(180, b.getWest()));
		const lomax = Math.max(-180, Math.min(180, b.getEast()));
		const latSpan = Math.abs(lamax - lamin);
		const lonSpan = Math.abs(lomax - lomin);
		if (latSpan * lonSpan > 1000) {
			setLiveStatus('Zoom in to enable live (area too large)');
			return;
		}
		const url = `https://opensky-network.org/api/states/all?lamin=${lamin.toFixed(4)}&lomin=${lomin.toFixed(4)}&lamax=${lamax.toFixed(4)}&lomax=${lomax.toFixed(4)}`;
		liveAbort = new AbortController();
		setLiveStatus('Fetching…');
		try {
			const res = await fetch(url, { signal: liveAbort.signal, cache: 'no-cache' });
			if (!res.ok) {
				setLiveStatus(`Error ${res.status}`);
				return;
			}
			const json = await res.json();
			const states = Array.isArray(json.states) ? json.states : [];
			const parsed = [];
			for (const s of states) {
				// indices based on OpenSky docs
				const icao24 = s[0] || null;
				const callsign = (s[1] || '').trim();
				const lon = typeof s[5] === 'number' ? s[5] : null;
				const lat = typeof s[6] === 'number' ? s[6] : null;
				if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
				const geoAlt = typeof s[13] === 'number' ? s[13] : null;
				const baroAlt = typeof s[7] === 'number' ? s[7] : null;
				const alt_m = (geoAlt ?? baroAlt) != null ? Math.round((geoAlt ?? baroAlt)) : null;
				const velocity = typeof s[9] === 'number' ? s[9] : null; // m/s
				const track = typeof s[10] === 'number' ? s[10] : null; // degrees, 0..360
				parsed.push({ icao24, callsign, lat, lon, alt_m, velocity, track });
			}
			liveAircraft = parsed;
			lastLiveFetchMs = now;
			setLiveStatus(`Live: ${parsed.length} aircraft`);
			// refresh layers
			overlay.setProps({ layers: buildLayers() });
			// If modal is open, refresh plane histogram too
			if (modalOverlay && modalOverlay.style.display === 'flex') {
				const start = cursor - windowMs;
				const windowFeatures = features.filter(f => {
					const t = f.properties.time_ms;
					return typeof t === 'number' && t >= start && t <= cursor;
				});
				updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
			}
		} catch (err) {
			if (err && err.name === 'AbortError') return;
			setLiveStatus('Fetch failed');
		}
	}

	function startLive() {
		if (liveEnabled) return;
		liveEnabled = true;
		setLiveStatus('Starting…');
		// immediate fetch
		fetchLiveOnce(true);
		// periodic refresh respecting min interval
		liveTimerId = setInterval(() => fetchLiveOnce(false), LIVE_MIN_INTERVAL_MS);
		// debounce map move events
		const schedule = () => {
			if (liveDebounceId) clearTimeout(liveDebounceId);
			liveDebounceId = setTimeout(() => fetchLiveOnce(false), 1200);
		};
		map.on('moveend', schedule);
		map.on('zoomend', schedule);
		// update layers to show empty live layer immediately
		overlay.setProps({ layers: buildLayers() });
	}

	function stopLive() {
		if (!liveEnabled) return;
		liveEnabled = false;
		if (liveTimerId) { clearInterval(liveTimerId); liveTimerId = null; }
		if (liveDebounceId) { clearTimeout(liveDebounceId); liveDebounceId = null; }
		if (liveAbort) { try { liveAbort.abort(); } catch {} liveAbort = null; }
		liveAircraft = [];
		setLiveStatus('');
		map.off('moveend', null);
		map.off('zoomend', null);
		// remove live layer
		overlay.setProps({ layers: buildLayers() });
	}

	if (liveToggle) {
		liveToggle.addEventListener('change', () => {
			if (liveToggle.checked) startLive(); else stopLive();
		});
	}

	function updateLayer() {
		const start = cursor - windowMs;
		overlay.setProps({ layers: buildLayers() });
		updateStats();
	}

	function tick() {
		cursor += 60 * 1000; // advance 1 minute per frame
		// Wrap within the last 24h window ending at tMax
		const wrapMin = Math.max(tMin, tMax - 24 * 60 * 60 * 1000);
		if (cursor > tMax) cursor = wrapMin;
		range.value = `${cursor}`;
		updateLayer();
		setLabel();
		rafId = requestAnimationFrame(tick);
	}

	const overlay = new deck.MapboxOverlay({
		interleaved: true,
		layers: []
	});
	map.addControl(overlay);
	updateLayer();

	// Initialize legend gradient to current palette
	const legendBar = document.querySelector('#legend .bar');
	if (legendBar) legendBar.style.background = rampToCssGradient(currentColorRamp);

	// Modal wiring
	function openDistances() {
		if (!modalOverlay) return;
		modalOverlay.style.display = 'flex';
		// Compute with current window
		const start = cursor - windowMs;
		const windowFeatures = features.filter(f => {
			const t = f.properties.time_ms;
			return typeof t === 'number' && t >= start && t <= cursor;
		});
		updateDistanceHistogram(windowFeatures, distModalCanvas, distModalMeta);
		updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
	}
	function closeDistances() {
		if (!modalOverlay) return;
		modalOverlay.style.display = 'none';
	}
	if (openDistancesBtn) openDistancesBtn.addEventListener('click', openDistances);
	if (modalCloseBtn) modalCloseBtn.addEventListener('click', closeDistances);
	if (modalOverlay) modalOverlay.addEventListener('click', (e) => { if (e.target === modalOverlay) closeDistances(); });
	// Update histogram when map view changes while modal is open
	map.on('moveend', () => {
		// Always refresh sidebar stats (includes in-view count)
		updateStats();
		if (modalOverlay && modalOverlay.style.display === 'flex') {
			const start = cursor - windowMs;
			const windowFeatures = features.filter(f => {
				const t = f.properties.time_ms;
				return typeof t === 'number' && t >= start && t <= cursor;
			});
			updateDistanceHistogram(windowFeatures, distModalCanvas, distModalMeta);
			updatePlaneDistanceHistogram(windowFeatures, planeDistCanvas, planeDistMeta);
		}
	});

	// Simple tooltip element
	const tip = document.createElement('div');
	tip.id = 'tooltip';
	tip.style.position = 'absolute';
	tip.style.pointerEvents = 'none';
	tip.style.background = 'rgba(0,0,0,0.75)';
	tip.style.color = '#fff';
	tip.style.padding = '6px 8px';
	tip.style.borderRadius = '4px';
	tip.style.font = '12px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif';
	tip.style.display = 'none';
	document.body.appendChild(tip);
})();


