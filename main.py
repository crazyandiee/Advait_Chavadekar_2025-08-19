"""
LOOP STORE MONITORING — FASTAPI STARTER (single-file)
====================================================

This is a self‑contained FastAPI app that solves the take‑home:
- Ingest the three CSVs into SQLite
- /trigger_report starts generation
- /get_report?report_id=... returns "Running" or the CSV when complete
- Computes uptime/downtime inside business hours using UTC<->local conversion

Quick Start (Mac/Windows/Linux)
-------------------------------
1) Python 3.10+ recommended.
2) Create a virtual env and install deps:
   python -m venv .venv && source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
   pip install -r requirements.txt  (see bottom of file for list)
   (If you don't have requirements.txt, just: pip install fastapi uvicorn sqlalchemy python-multipart pydantic)

3) Download the zip from the prompt, extract to ./data/ so you have:
   ./data/status.csv               (columns: store_id,timestamp_utc,status)
   ./data/business_hours.csv       (columns: store_id,dayOfWeek,start_time_local,end_time_local)
   ./data/timezones.csv            (columns: store_id,timezone_str)

   Notes:
   - All timestamps in status.csv are in UTC ISO format.
   - dayOfWeek: 0=Monday ... 6=Sunday
   - If a store has NO rows in business_hours.csv, we assume 24x7.
   - If timezone missing for a store, default America/Chicago.

4) Load data to DB and run server (first run auto‑ingests if DB empty):
   uvicorn main:app --reload

5) Trigger a report:
   curl -X POST http://127.0.0.1:8000/trigger_report
   -> {"report_id":"<id>"}

6) Poll for completion:
   curl "http://127.0.0.1:8000/get_report?report_id=<id>"
   If running: {"status":"Running"}
   If done: returns CSV file (text/csv) with columns:
   store_id,uptime_last_hour(d_mins),uptime_last_day(h_hrs),uptime_last_week(h_hrs),downtime_last_hour(d_mins),downtime_last_day(h_hrs),downtime_last_week(h_hrs)

CSV is also saved to ./reports/<id>.csv

Design & Assumptions (documented in code below)
-----------------------------------------------
- Polls are roughly hourly; to convert sparse polls to continuous time we use a step function:
  "last observation carried forward (LOCF)" between poll timestamps.
- If there is no observation at/before a subinterval start, we assume INACTIVE until the first observation.
- Business hours windows are computed in local store time, then converted to UTC and intersected with the report windows.
- If a store has at least one business hours row, times outside those rows are considered CLOSED (ignored). If the store has no rows, it is open 24x7.
- "Current time" is fixed to the max timestamp present in status.csv, as requested.

Ideas to Improve (for README)
-----------------------------
- Replace LOCF with a gap‑aware model (e.g., cap carry‑forward at 2h; beyond that mark unknown).
- Add unit/integration tests; property tests for tricky DST days.
- Batch computations with SQL window functions for speed.
- Support S3/GCS ingestion; CDC to keep DB warm.
- Paginated CSV or Parquet output; signed URL download.
- Observability: metrics for data gaps, per‑store health, processing time.

"""
from __future__ import annotations

import csv
import os
import uuid
from datetime import datetime, timedelta, time, date
from typing import List, Tuple, Optional, Dict

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.responses import PlainTextResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Time, ForeignKey, Text, func, select, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from dateutil import parser
from zoneinfo import ZoneInfo


DB_URL = os.getenv("DB_URL", "sqlite:///./store_monitoring.db")
DATA_DIR = os.getenv("DATA_DIR", "./data")
REPORTS_DIR = os.getenv("REPORTS_DIR", "./reports")
DEFAULT_TZ = "America/Chicago"

engine = create_engine(DB_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

# ----------------------
# Database Models
# ----------------------
class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True)
    store_id = Column(String, index=True, nullable=False)
    timestamp_utc = Column(DateTime, index=True, nullable=False)
    status = Column(String, nullable=False)  # "active" or "inactive"

class BusinessHours(Base):
    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True)
    store_id = Column(String, index=True, nullable=False)
    day_of_week = Column(Integer, nullable=False)  # 0=Mon .. 6=Sun
    start_time_local = Column(Time, nullable=False)
    end_time_local = Column(Time, nullable=False)

class StoreTimezone(Base):
    __tablename__ = "store_timezone"
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, nullable=False)

class Report(Base):
    __tablename__ = "report"
    id = Column(String, primary_key=True)  # report_id
    status = Column(String, nullable=False, default="Running")  # Running/Complete
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    csv_path = Column(Text, nullable=True)

# ----------------------
# Utilities
# ----------------------

def ensure_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)


def parse_iso_utc(ts: str) -> datetime:
    return parser.parse(ts)


def read_csv_detect(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k.strip(): v.strip() for k, v in r.items()})
    return rows


def ingest_if_needed():
    """Load CSVs into DB if tables are empty."""
    with SessionLocal() as s:
        has_status = s.execute(select(func.count(StoreStatus.id))).scalar_one()
        if has_status:
            return  # already ingested

        # Heuristic file names (allow a few common variants)
        status_files = [
            os.path.join(DATA_DIR, name)
            for name in ["status.csv", "store_status.csv", "store_statuses.csv"]
            if os.path.exists(os.path.join(DATA_DIR, name))
        ]
        bh_files = [
            os.path.join(DATA_DIR, name)
            for name in ["business_hours.csv", "store_business_hours.csv", "businesshours.csv"]
            if os.path.exists(os.path.join(DATA_DIR, name))
        ]
        tz_files = [
            os.path.join(DATA_DIR, name)
            for name in ["timezones.csv", "store_timezones.csv", "time_zones.csv"]
            if os.path.exists(os.path.join(DATA_DIR, name))
        ]
        if not status_files or not tz_files:
            raise RuntimeError(
                f"CSV files not found in {DATA_DIR}. Expected status.csv/business_hours.csv/timezones.csv"
            )

        # Ingest status
        for path in status_files:
            for r in read_csv_detect(path):
                s.add(
                    StoreStatus(
                        store_id=r["store_id"],
                        timestamp_utc=parse_iso_utc(r["timestamp_utc"]).replace(tzinfo=ZoneInfo("UTC")).replace(tzinfo=None),
                        # store naive UTC in DB for simplicity; add tz when using
                        status=r["status"].lower(),
                    )
                )
        s.commit()

        # Ingest business hours (optional)
        if bh_files:
            for path in bh_files:
                for r in read_csv_detect(path):
                    s.add(
                        BusinessHours(
                            store_id=r["store_id"],
                            day_of_week=int(r["dayOfWeek"]),
                            start_time_local=_parse_hhmm(r["start_time_local"]),
                            end_time_local=_parse_hhmm(r["end_time_local"]),
                        )
                    )
            s.commit()

        # Ingest timezones
        for path in tz_files:
            for r in read_csv_detect(path):
                tz = r.get("timezone_str") or r.get("timezone") or DEFAULT_TZ
                s.merge(
                    StoreTimezone(
                        store_id=r["store_id"],
                        timezone_str=tz,
                    )
                )
        s.commit()


def _parse_hhmm(sval: str) -> time:
    # supports HH:MM[:SS]
    parts = [int(p) for p in sval.split(":")]
    if len(parts) == 2:
        return time(parts[0], parts[1])
    if len(parts) == 3:
        return time(parts[0], parts[1], parts[2])
    raise ValueError(f"Invalid time string: {sval}")


# --------------
# Time Windows
# --------------

def get_current_utc(s: SessionLocal) -> datetime:
    dt_naive = s.execute(select(func.max(StoreStatus.timestamp_utc))).scalar_one()
    if not dt_naive:
        raise HTTPException(500, "No status data ingested")
    return dt_naive.replace(tzinfo=ZoneInfo("UTC"))


def window_last_hour(now_utc: datetime) -> Tuple[datetime, datetime]:
    return (now_utc - timedelta(hours=1), now_utc)


def window_last_day(now_utc: datetime) -> Tuple[datetime, datetime]:
    return (now_utc - timedelta(days=1), now_utc)


def window_last_week(now_utc: datetime) -> Tuple[datetime, datetime]:
    return (now_utc - timedelta(days=7), now_utc)


# ---------------------------------------
# Business Hours: Local -> UTC intervals
# ---------------------------------------

def get_store_timezone(s, store_id: str) -> ZoneInfo:
    tz = s.get(StoreTimezone, store_id)
    tzname = tz.timezone_str if tz else DEFAULT_TZ
    try:
        return ZoneInfo(tzname)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def store_has_any_bh(s, store_id: str) -> bool:
    count = s.execute(
        select(func.count(BusinessHours.id)).where(BusinessHours.store_id == store_id)
    ).scalar_one()
    return bool(count)


def iterate_local_business_intervals(
    s,
    store_id: str,
    tz: ZoneInfo,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> List[Tuple[datetime, datetime]]:
    """
    Build a list of UTC intervals that represent the store's OPEN time inside the UTC window.
    If the store has NO BH rows -> treat as open 24x7 (just return [window_start_utc, window_end_utc]).
    """
    if not store_has_any_bh(s, store_id):
        return [(window_start_utc, window_end_utc)]

    # Convert window bounds into local dates to know which days to iterate
    local_start = window_start_utc.astimezone(tz)
    local_end = window_end_utc.astimezone(tz)

    # iterate each local calendar day touched by the window
    intervals: List[Tuple[datetime, datetime]] = []
    day_cursor = local_start.date()
    last_day = local_end.date()

    while day_cursor <= last_day:
        dow = (day_cursor.weekday())  # Monday=0 .. Sunday=6
        rows = s.execute(
            select(BusinessHours).where(
                (BusinessHours.store_id == store_id) & (BusinessHours.day_of_week == dow)
            )
        ).scalars().all()

        for row in rows:
            # build local start/end datetimes for this day
            st = row.start_time_local
            et = row.end_time_local

            def to_local(dt_date: date, t: time) -> datetime:
                return datetime(dt_date.year, dt_date.month, dt_date.day, t.hour, t.minute, t.second, tzinfo=tz)

            if st <= et:
                local_a = to_local(day_cursor, st)
                local_b = to_local(day_cursor, et)
                # convert to UTC and intersect with window
                a = local_a.astimezone(ZoneInfo("UTC"))
                b = local_b.astimezone(ZoneInfo("UTC"))
                a2 = max(a, window_start_utc)
                b2 = min(b, window_end_utc)
                if a2 < b2:
                    intervals.append((a2, b2))
            else:
                # Overnight window (e.g., 20:00 -> 02:00 next day)
                local_a1 = to_local(day_cursor, st)
                local_b1 = to_local(day_cursor + timedelta(days=1), et)
                a1 = local_a1.astimezone(ZoneInfo("UTC"))
                b1 = local_b1.astimezone(ZoneInfo("UTC"))
                a2 = max(a1, window_start_utc)
                b2 = min(b1, window_end_utc)
                if a2 < b2:
                    intervals.append((a2, b2))

        day_cursor += timedelta(days=1)

    # Merge overlapping intervals (if multiple rows per day)
    intervals.sort(key=lambda x: x[0])
    merged: List[Tuple[datetime, datetime]] = []
    for a, b in intervals:
        if not merged or a > merged[-1][1]:
            merged.append((a, b))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
    return merged


# ---------------------------------------
# Interpolate uptime/downtime inside open intervals
# ---------------------------------------

def compute_uptime_downtime_for_interval(s, store_id: str, a: datetime, b: datetime) -> Tuple[float, float]:
    """Return uptime_seconds, downtime_seconds in [a,b) using step function interpolation.
    Rules:
    - status is piecewise-constant between polls.
    - if no observation before 'a', assume INACTIVE until the first observation.
    - tie to bounds (extend last obs to 'b').
    """
    utc = ZoneInfo("UTC")
    a_naive = a.astimezone(utc).replace(tzinfo=None)
    b_naive = b.astimezone(utc).replace(tzinfo=None)

    obs = s.execute(
        select(StoreStatus)
        .where((StoreStatus.store_id == store_id) & (StoreStatus.timestamp_utc >= a_naive - timedelta(days=1)) & (StoreStatus.timestamp_utc <= b_naive + timedelta(days=1)))
        .order_by(StoreStatus.timestamp_utc.asc())
    ).scalars().all()

    # Find last obs <= a
    last_status: Optional[str] = None
    last_time = a_naive

    for o in obs:
        if o.timestamp_utc <= a_naive:
            last_status = o.status
            last_time = a_naive  # we start counting at 'a'
        else:
            break

    uptime = 0.0
    downtime = 0.0

    # If we have no observation before 'a', we start as INACTIVE
    current_status = last_status or "inactive"
    current_time = a_naive

    for o in obs:
        if o.timestamp_utc < a_naive:
            continue
        if o.timestamp_utc > b_naive:
            break
        # accumulate until this observation time
        seg_end = o.timestamp_utc
        dur = (seg_end - current_time).total_seconds()
        if current_status == "active":
            uptime += max(0.0, dur)
        else:
            downtime += max(0.0, dur)
        # switch status at the observation time
        current_status = o.status
        current_time = seg_end

    # tail to 'b'
    if current_time < b_naive:
        dur = (b_naive - current_time).total_seconds()
        if current_status == "active":
            uptime += dur
        else:
            downtime += dur

    return uptime, downtime


def compute_windows_for_store(s, store_id: str, now_utc: datetime) -> Dict[str, Tuple[float, float]]:
    tz = get_store_timezone(s, store_id)

    windows = {
        "last_hour": window_last_hour(now_utc),
        "last_day": window_last_day(now_utc),
        "last_week": window_last_week(now_utc),
    }

    results: Dict[str, Tuple[float, float]] = {}

    for key, (wstart, wend) in windows.items():
        # Find open intervals in UTC inside this window
        open_intervals = iterate_local_business_intervals(s, store_id, tz, wstart, wend)
        uptime_sec = 0.0
        downtime_sec = 0.0
        for a, b in open_intervals:
            u, d = compute_uptime_downtime_for_interval(s, store_id, a, b)
            uptime_sec += u
            downtime_sec += d
        results[key] = (uptime_sec, downtime_sec)

    return results


# ----------------------
# API Models
# ----------------------
class TriggerResponse(BaseModel):
    report_id: str


# ----------------------
# Report Generation
# ----------------------

def generate_report(report_id: str):
    ensure_dirs()
    with SessionLocal() as s:
        now_utc = get_current_utc(s)
        # all store_ids present in status table
        store_ids = [r[0] for r in s.execute(select(StoreStatus.store_id).distinct()).all()]

        out_lines: List[str] = []
        header = (
            "store_id,uptime_last_hour(in minutes),uptime_last_day(in hours),uptime_last_week(in hours),"
            "downtime_last_hour(in minutes),downtime_last_day(in hours),downtime_last_week(in hours)"
        )
        out_lines.append(header)

        for store_id in store_ids:
            res = compute_windows_for_store(s, store_id, now_utc)
            # Convert seconds to minutes/hours
            uh_min = res["last_hour"][0] / 60.0
            dh_min = res["last_hour"][1] / 60.0
            ud_hrs = res["last_day"][0] / 3600.0
            dd_hrs = res["last_day"][1] / 3600.0
            uw_hrs = res["last_week"][0] / 3600.0
            dw_hrs = res["last_week"][1] / 3600.0

            out_lines.append(
                f"{store_id},{uh_min:.2f},{ud_hrs:.2f},{uw_hrs:.2f},{dh_min:.2f},{dd_hrs:.2f},{dw_hrs:.2f}"
            )

        # Save to file
        csv_path = os.path.join(REPORTS_DIR, f"{report_id}.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines))

        # Mark complete
        rep = s.get(Report, report_id)
        if rep:
            rep.status = "Complete"
            rep.completed_at = datetime.utcnow()
            rep.csv_path = csv_path
            s.commit()


# ----------------------
# FastAPI App
# ----------------------
app = FastAPI(title="Loop Store Monitoring API", version="1.0.0")


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(engine)
    # Ingest if empty
    ingest_if_needed()


@app.post("/trigger_report", response_model=TriggerResponse)
def trigger_report(background_tasks: BackgroundTasks):
    report_id = uuid.uuid4().hex
    with SessionLocal() as s:
        s.add(Report(id=report_id, status="Running"))
        s.commit()
    background_tasks.add_task(generate_report, report_id)
    return TriggerResponse(report_id=report_id)


@app.get("/get_report")
def get_report(report_id: str = Query(...)):
    with SessionLocal() as s:
        rep = s.get(Report, report_id)
        if not rep:
            raise HTTPException(404, "report_id not found")
        if rep.status != "Complete" or not rep.csv_path or not os.path.exists(rep.csv_path):
            return JSONResponse({"status": "Running"})
        # Return CSV content
        with open(rep.csv_path, "r", encoding="utf-8") as f:
            content = f.read()
        return PlainTextResponse(content, media_type="text/csv")


# ----------------------
# Minimal requirements.txt (copy this block to a file named requirements.txt)
# ----------------------
# fastapi
# uvicorn
# sqlalchemy
# pydantic
# python-multipart
# backports.zoneinfo ; python_version < '3.9'
