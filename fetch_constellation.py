#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import math
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import httpx
import json5
from dateutil import parser as dtparser

BASE_URL = "https://a.windbornesystems.com/treasure/{:02d}.json"


def ensure_dir(path: str) -> None:
	os.makedirs(path, exist_ok=True)


def normalize_key(name: str) -> str:
	k = name.strip().lower()
	k = re.sub(r"[^a-z0-9]", "", k)
	# common synonyms
	k = k.replace("longitude", "lon").replace("long", "lon").replace("lng", "lon")
	k = k.replace("latitude", "lat")
	k = k.replace("altitude", "alt")
	k = k.replace("updatedat", "time").replace("lastseen", "time").replace("fixtime", "time")
	k = k.replace("timestampms", "timestamp").replace("timeunix", "timestamp")
	return k


def try_json_loads(text: str) -> Any:
	# Fast path: strict JSON
	try:
		return json.loads(text)
	except Exception:
		pass
	# Try JSON5 for trailing commas, comments, etc.
	try:
		return json5.loads(text)
	except Exception:
		pass
	# Try simple repairs: remove trailing commas before ] or }
	repaired = re.sub(r",\s*([\]\}])", r"\1", text)
	# Trim to first bracket to last bracket to drop accidental HTML wrappers
	m_open = re.search(r"[\[\{]", repaired)
	m_close = None
	if m_open:
		# find last closing bracket of matching type
		closing_candidates = [m.start() for m in re.finditer(r"[\]\}]", repaired)]
		if closing_candidates:
			end = closing_candidates[-1] + 1
			repaired = repaired[m_open.start():end]
	try:
		return json5.loads(repaired)
	except Exception:
		# Last resort: attempt to extract top-level array/object by balancing braces
		return try_extract_json_fragment(repaired)


def try_extract_json_fragment(text: str) -> Any:
	# Attempt to find a balanced JSON array or object
	for opener, closer in [("[", "]"), ("{", "}")]:
		start_idx = text.find(opener)
		if start_idx == -1:
			continue
		balance = 0
		for i in range(start_idx, len(text)):
			ch = text[i]
			if ch == opener:
				balance += 1
			elif ch == closer:
				balance -= 1
				if balance == 0:
					fragment = text[start_idx:i+1]
					try:
						return json5.loads(fragment)
					except Exception:
						break
	return None


def flatten_dict(d: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
	if out is None:
		out = {}
	if isinstance(d, dict):
		for k, v in d.items():
			key = normalize_key(f"{prefix}.{k}" if prefix else str(k))
			if isinstance(v, dict):
				flatten_dict(v, f"{prefix}.{k}" if prefix else str(k), out)
			else:
				out[key] = v
	return out


def enumerate_records(obj: Any) -> Iterable[Tuple[Optional[str], Dict[str, Any]]]:
	# Returns (parent_key_if_any, record_dict)
	if obj is None:
		return []
	if isinstance(obj, list):
		for rec in obj:
			if isinstance(rec, dict):
				yield None, rec
			elif isinstance(rec, (list, tuple)) and len(rec) >= 2:
				# Heuristic: [lat, lon, alt_km?]
				lat_val = rec[0]
				lon_val = rec[1]
				alt_km_val = rec[2] if len(rec) >= 3 else None
				try:
					lat_num = float(lat_val)
					lon_num = float(lon_val)
				except Exception:
					continue
				record: Dict[str, Any] = {"lat": lat_num, "lon": lon_num}
				if alt_km_val is not None:
					try:
						record["altkm"] = float(alt_km_val)
					except Exception:
						pass
				yield None, record
		return
	if isinstance(obj, dict):
		# Common wrappers
		candidate_lists = [v for v in obj.values() if isinstance(v, list) and (not v or isinstance(v[0], dict))]
		if candidate_lists:
			best = max(candidate_lists, key=lambda x: len(x))
			for rec in best:
				yield None, rec
			return
		# Keyed by id map
		if all(isinstance(v, dict) for v in obj.values()):
			for k, v in obj.items():
				yield str(k), v
			return
		# Fallback: treat object as a single record
		yield None, obj
		return
	return []


def coerce_float(value: Any) -> Optional[float]:
	if value is None:
		return None
	if isinstance(value, (int, float)):
		if isinstance(value, bool):
			return None
		try:
			return float(value)
		except Exception:
			return None
	if isinstance(value, str):
		s = value.strip()
		m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
		if not m:
			return None
		try:
			return float(m.group(0))
		except Exception:
			return None
	return None


def extract_lat_lon(flat: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
	lat_keys = ["lat", "late7", "latitude", "gpslat", "positionlat"]
	lon_keys = ["lon", "lnge7", "lone7", "longitude", "gpslon", "positionlon"]
	lat = None
	lon = None
	# E7 integer support
	if "late7" in flat and flat.get("late7") is not None:
		lat = coerce_float(flat.get("late7"))
		if lat is not None:
			lat = lat / 1e7
	if "lone7" in flat and flat.get("lone7") is not None:
		lon = coerce_float(flat.get("lone7"))
		if lon is not None:
			lon = lon / 1e7
	if "lnge7" in flat and flat.get("lnge7") is not None:
		lon = coerce_float(flat.get("lnge7"))
		if lon is not None:
			lon = lon / 1e7
	if lat is None:
		for k in lat_keys:
			if k in flat and flat[k] is not None:
				lat = coerce_float(flat[k])
				break
	if lon is None:
		for k in lon_keys:
			if k in flat and flat[k] is not None:
				lon = coerce_float(flat[k])
				break
	if lat is not None and (-90 > lat or lat > 90):
		lat = None
	if lon is not None and (-180 > lon or lon > 180):
		lon = None
	return lat, lon


def feet_to_meters(feet: float) -> float:
	return feet * 0.3048


def extract_alt_m(flat: Dict[str, Any]) -> Optional[float]:
	# Altitude in kilometers explicitly
	if "altkm" in flat and flat.get("altkm") is not None:
		val = coerce_float(flat.get("altkm"))
		if val is not None:
			return val * 1000.0
	# Prefer explicit meters
	meter_keys = [
		"altm", "altitudem", "altitude", "alt", "msl", "baroaltitude", "elevation", "height", "agl"
	]
	for k in meter_keys:
		if k in flat and flat[k] is not None:
			val = coerce_float(flat[k])
			if val is not None:
				return val
	# Feet-based fields
	feet_keys = ["altft", "altitudeft", "altfeet", "feet", "altftmsl"]
	for k in feet_keys:
		if k in flat and flat[k] is not None:
			val = coerce_float(flat[k])
			if val is not None:
				return feet_to_meters(val)
	# String with units
	for k, v in flat.items():
		if isinstance(v, str):
			s = v.strip().lower()
			m = re.match(r"([-+]?\d+(?:\.\d+)?)\s*(m|meter|meters|km|kilometer|kilometers|ft|feet)\b", s)
			if m:
				num = float(m.group(1))
				unit = m.group(2)
				if unit.startswith("m") and unit != "meters":
					return num
				if unit.startswith("km"):
					return num * 1000.0
				return num if unit.startswith("m") else feet_to_meters(num)
	return None


def parse_time_to_iso(flat: Dict[str, Any]) -> Optional[str]:
	candidates = [
		"time", "timestamp", "ts", "datetime", "date", "lastseen", "updated", "updatedat", "fixtime"
	]
	val: Optional[Union[str, int, float]] = None
	for k in candidates:
		if k in flat and flat[k] is not None:
			val = flat[k]
			break
	if val is None:
		return None
	try:
		if isinstance(val, (int, float)) and not isinstance(val, bool):
			sec = float(val)
			# Heuristic: milliseconds if very large
			if sec > 1e12:
				sec /= 1000.0
			elif sec > 1e10:  # also treat as ms
				sec /= 1000.0
			dt = datetime.fromtimestamp(sec, tz=timezone.utc)
			return dt.isoformat()
		if isinstance(val, str):
			# If it's a bare number string
			m = re.fullmatch(r"[-+]?\d+(?:\.\d+)?", val.strip())
			if m:
				sec = float(val)
				if sec > 1e12:
					sec /= 1000.0
				elif sec > 1e10:
					sec /= 1000.0
				dt = datetime.fromtimestamp(sec, tz=timezone.utc)
				return dt.isoformat()
			dt = dtparser.parse(val)
			if not dt.tzinfo:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt.astimezone(timezone.utc).isoformat()
	except Exception:
		return None


def extract_id(parent_key: Optional[str], flat: Dict[str, Any]) -> Optional[str]:
	candidates = [
		"id", "balloonid", "flightid", "deviceid", "serial", "name", "identifier", "uuid"
	]
	for k in candidates:
		if k in flat and flat[k]:
			return str(flat[k])
	# Use parent map key as fallback
	if parent_key:
		return str(parent_key)
	return None


def standardize_record(parent_key: Optional[str], rec: Dict[str, Any], source_hour: int, raw_index: int) -> Optional[Dict[str, Any]]:
	flat = flatten_dict(rec)
	lat, lon = extract_lat_lon(flat)
	if lat is None or lon is None:
		return None
	alt_m = extract_alt_m(flat)
	time_iso = parse_time_to_iso(flat)
	if time_iso is None:
		# Approximate snapshot time based on source hour offset
		approx_dt = datetime.now(timezone.utc) - timedelta(hours=source_hour)
		approx_dt = approx_dt.replace(minute=0, second=0, microsecond=0)
		time_iso = approx_dt.isoformat()
	id_value = extract_id(parent_key, flat)
	return {
		"id": id_value,
		"lat": lat,
		"lon": lon,
		"alt_m": alt_m,
		"time_iso": time_iso,
		"source_hour": source_hour,
		"raw_index": raw_index,
	}


async def fetch_one(client: httpx.AsyncClient, hour: int, timeout: float, save_raw_dir: Optional[str]) -> Tuple[int, Optional[Any], Optional[str]]:
	url = BASE_URL.format(hour)
	try:
		resp = await client.get(url, timeout=timeout)
		resp.raise_for_status()
		text = resp.text
		if save_raw_dir:
			ensure_dir(save_raw_dir)
			with open(os.path.join(save_raw_dir, f"{hour:02d}.json"), "w", encoding="utf-8") as f:
				f.write(text)
		data = try_json_loads(text)
		return hour, data, None
	except Exception as e:
		return hour, None, f"{type(e).__name__}: {e}"


async def fetch_all(hours: int, timeout: float, max_concurrency: int, save_raw_dir: Optional[str]) -> Tuple[List[Tuple[int, Any]], List[Tuple[int, str]]]:
	sema = asyncio.Semaphore(max_concurrency)
	results: List[Tuple[int, Any]] = []
	errors: List[Tuple[int, str]] = []

	async def task(hour: int) -> None:
		async with sema:
			async with httpx.AsyncClient(http2=True, headers={"user-agent": "wb-constellation-fetcher/1.0"}) as client:
				h, data, err = await fetch_one(client, hour, timeout, save_raw_dir)
				if err is None and data is not None:
					results.append((h, data))
				else:
					errors.append((h, err or "unknown error"))

	await asyncio.gather(*(task(h) for h in range(hours)))
	results.sort(key=lambda x: x[0])
	errors.sort(key=lambda x: x[0])
	return results, errors


def write_outputs(records: List[Dict[str, Any]], out_dir: str) -> Tuple[str, str]:
	ensure_dir(out_dir)
	ndjson_path = os.path.join(out_dir, "constellation.ndjson")
	csv_path = os.path.join(out_dir, "constellation.csv")
	with open(ndjson_path, "w", encoding="utf-8") as fnd:
		for rec in records:
			fnd.write(json.dumps(rec, ensure_ascii=False) + "\n")
	with open(csv_path, "w", encoding="utf-8", newline="") as fcsv:
		writer = csv.writer(fcsv)
		writer.writerow(["id", "time_iso", "lat", "lon", "alt_m", "source_hour", "raw_index"])
		for rec in records:
			writer.writerow([
				rec.get("id"),
				rec.get("time_iso"),
				rec.get("lat"),
				rec.get("lon"),
				rec.get("alt_m"),
				rec.get("source_hour"),
				rec.get("raw_index"),
			])
	return ndjson_path, csv_path


def build_records(fetched: List[Tuple[int, Any]]) -> List[Dict[str, Any]]:
	standardized: List[Dict[str, Any]] = []
	seen: set = set()
	for hour, data in fetched:
		for idx, (parent_key, rec) in enumerate(enumerate_records(data)):
			s = standardize_record(parent_key, rec, hour, idx)
			if s is None:
				continue
			key = (s.get("id"), s.get("time_iso"), round(s.get("lat", 0), 6), round(s.get("lon", 0), 6))
			if key in seen:
				continue
			seen.add(key)
			standardized.append(s)
	return standardized


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
	p = argparse.ArgumentParser(description="Fetch Windborne constellation history and write NDJSON/CSV")
	p.add_argument("--out-dir", default="out", help="Output directory (default: ./out)")
	p.add_argument("--hours", type=int, default=24, help="How many hours back to fetch (default: 24; max 24)")
	p.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout seconds")
	p.add_argument("--max-concurrency", type=int, default=8, help="Max parallel requests")
	p.add_argument("--save-raw", action="store_true", help="Save raw responses to out/raw")
	p.add_argument("--verbose", action="store_true", help="Verbose logging")
	return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
	args = parse_args(argv)
	hours = max(1, min(24, args.hours))
	out_dir = os.path.abspath(args.out_dir)
	raw_dir = os.path.join(out_dir, "raw") if args.save_raw else None
	if args.verbose:
		print(f"Fetching {hours} hours from {BASE_URL.format(0)} .. {BASE_URL.format(hours-1)}", file=sys.stderr)

	fetched, errors = asyncio.run(fetch_all(hours, args.timeout, args.max_concurrency, raw_dir))
	if args.verbose and errors:
		for h, e in errors:
			print(f"Hour {h:02d}: ERROR {e}", file=sys.stderr)

	records = build_records(fetched)
	ndjson_path, csv_path = write_outputs(records, out_dir)

	print(f"Wrote {len(records)} records")
	print(f"NDJSON: {ndjson_path}")
	print(f"CSV: {csv_path}")
	if errors:
		print(f"Had {len(errors)} fetch/parse errors", file=sys.stderr)
	return 0


if __name__ == "__main__":
	sys.exit(main())
