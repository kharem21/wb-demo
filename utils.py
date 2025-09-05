#!/usr/bin/env python3
import csv
import os
from typing import Dict, List, Optional


def ensure_dir(path: str) -> None:
	os.makedirs(path, exist_ok=True)


def read_constellation_csv(path: str, balloon_id: Optional[str]) -> List[Dict[str, Optional[float]]]:
	rows: List[Dict[str, Optional[float]]] = []
	with open(path, "r", encoding="utf-8") as f:
		reader = csv.DictReader(f)
		for r in reader:
			if balloon_id is not None and (r.get("id") or "") != balloon_id:
				continue
			try:
				lat = float(r["lat"]) if r.get("lat") not in (None, "") else None
				lon = float(r["lon"]) if r.get("lon") not in (None, "") else None
				alt_m = float(r["alt_m"]) if r.get("alt_m") not in (None, "") else None
			except Exception:
				continue
			rows.append({
				"id": r.get("id"),
				"time_iso": r.get("time_iso"),
				"lat": lat,
				"lon": lon,
				"alt_m": alt_m,
			})
	return rows


