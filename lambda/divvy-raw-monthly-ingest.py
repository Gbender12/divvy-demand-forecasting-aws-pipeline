import os
import json
import logging
import boto3
import zipfile
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_json(message, **fields):
    logger.info("%s | %s", message, json.dumps(fields, default=str))

s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
ZIP_PREFIX = os.environ.get("ZIP_PREFIX", "zip/divvy-tripdata").strip("/")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/divvy-tripdata").strip("/")


def parse_yyyymm(yyyymm: str):
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"Invalid YYYYMM value: {yyyymm}")

    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])

    if month_num < 1 or month_num > 12:
        raise ValueError(f"Invalid month in YYYYMM: {yyyymm}")

    return year_num, month_num


def format_yyyymm(year_num: int, month_num: int) -> str:
    return f"{year_num:04d}{month_num:02d}"


def month_range(start_yyyymm: str, end_yyyymm: str):
    start_year, start_month = parse_yyyymm(start_yyyymm)
    end_year, end_month = parse_yyyymm(end_yyyymm)

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(
            f"target_start_yyyymm must be <= target_end_yyyymm. "
            f"Got {start_yyyymm} > {end_yyyymm}"
        )

    months = []
    cur_year, cur_month = start_year, start_month

    while (cur_year, cur_month) <= (end_year, end_month):
        months.append(format_yyyymm(cur_year, cur_month))
        if cur_month == 12:
            cur_year += 1
            cur_month = 1
        else:
            cur_month += 1

    return months


def yyyymm_to_parts(yyyymm: str):
    year = yyyymm[:4]
    month = yyyymm[4:6]

    zip_filename = f"{yyyymm}-divvy-tripdata.zip"
    csv_filename = f"{yyyymm}-divvy-tripdata.csv"

    source_url = f"https://divvy-tripdata.s3.amazonaws.com/{zip_filename}"

    zip_key = f"{ZIP_PREFIX}/year={year}/month={month}/{zip_filename}"
    csv_key = f"{RAW_PREFIX}/year={year}/month={month}/{csv_filename}"

    return {
        "yyyymm": yyyymm,
        "year": year,
        "month": month,
        "zip_filename": zip_filename,
        "csv_filename": csv_filename,
        "source_url": source_url,
        "zip_key": zip_key,
        "csv_key": csv_key,
    }


def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def source_exists(url):
    try:
        req = Request(url, method="HEAD")
        with urlopen(req, timeout=30) as resp:
            return 200 <= resp.status < 300
    except HTTPError as e:
        if e.code == 404:
            return False
        raise
    except URLError:
        raise


def download_zip_to_tmp(url, tmp_zip_path):
    req = Request(url, method="GET")
    with urlopen(req, timeout=300) as resp, open(tmp_zip_path, "wb") as out:
        while True:
            chunk = resp.read(8 * 1024 * 1024)
            if not chunk:
                break
            out.write(chunk)


def upload_local_file_to_s3(local_path, bucket, key, content_type):
    with open(local_path, "rb") as f:
        s3.upload_fileobj(
            f,
            bucket,
            key,
            ExtraArgs={"ContentType": content_type}
        )


def extract_csv_from_zip_to_s3(tmp_zip_path, bucket, csv_key):
    with zipfile.ZipFile(tmp_zip_path, "r") as zf:
        csv_members = []

        for name in zf.namelist():
            normalized = name.strip()

            if normalized.endswith("/"):
                continue
            if normalized.startswith("__MACOSX/"):
                continue

            basename = normalized.split("/")[-1]
            if basename.startswith("._"):
                continue

            if normalized.lower().endswith(".csv"):
                csv_members.append(normalized)

        if not csv_members:
            raise RuntimeError("No usable CSV file found inside ZIP archive")

        if len(csv_members) > 1:
            raise RuntimeError(
                f"Expected one usable CSV in ZIP, found {len(csv_members)}: {csv_members}"
            )

        csv_member = csv_members[0]
        log_json("zip_csv_member_selected", csv_member=csv_member, csv_key=csv_key)

        with zf.open(csv_member, "r") as csv_stream:
            s3.upload_fileobj(
                csv_stream,
                bucket,
                csv_key,
                ExtraArgs={"ContentType": "text/csv"}
            )


def ingest_one_month(yyyymm: str):
    parts = yyyymm_to_parts(yyyymm)

    log_json(
        "month_processing_started",
        yyyymm=yyyymm,
        zip_key=parts["zip_key"],
        csv_key=parts["csv_key"],
        source_url=parts["source_url"]
    )

    if object_exists(RAW_BUCKET, parts["csv_key"]):
        log_json(
            "csv_already_exists",
            yyyymm=yyyymm,
            csv_key=parts["csv_key"]
        )
        return {
            "yyyymm": yyyymm,
            "status": "already_loaded",
            "zip_key": parts["zip_key"],
            "csv_key": parts["csv_key"]
        }

    if not source_exists(parts["source_url"]):
        logger.warning(
            "source_not_available | %s",
            json.dumps({
                "yyyymm": yyyymm,
                "source_url": parts["source_url"]
            })
        )
        return {
            "yyyymm": yyyymm,
            "status": "source_not_available_yet",
            "zip_key": parts["zip_key"],
            "csv_key": parts["csv_key"]
        }

    tmp_zip_path = f"/tmp/{parts['zip_filename']}"

    log_json("zip_download_started", yyyymm=yyyymm, tmp_zip_path=tmp_zip_path)
    download_zip_to_tmp(parts["source_url"], tmp_zip_path)

    log_json("zip_upload_started", bucket=RAW_BUCKET, zip_key=parts["zip_key"])
    upload_local_file_to_s3(
        tmp_zip_path,
        RAW_BUCKET,
        parts["zip_key"],
        "application/zip"
    )

    log_json("csv_extract_upload_started", bucket=RAW_BUCKET, csv_key=parts["csv_key"])
    extract_csv_from_zip_to_s3(
        tmp_zip_path,
        RAW_BUCKET,
        parts["csv_key"]
    )

    log_json("month_processing_completed", yyyymm=yyyymm)

    return {
        "yyyymm": yyyymm,
        "status": "loaded",
        "zip_key": parts["zip_key"],
        "csv_key": parts["csv_key"]
    }


def lambda_handler(event, context):
    try:
        log_json(
            "lambda_started",
            request_id=context.aws_request_id,
            event=event or {}
        )

        if not event:
            raise ValueError("Event payload is required")

        target_start_yyyymm = event.get("target_start_yyyymm")
        target_end_yyyymm = event.get("target_end_yyyymm")

        if not target_start_yyyymm or not target_end_yyyymm:
            raise ValueError(
                "target_start_yyyymm and target_end_yyyymm are required"
            )

        raw_ingest_months = month_range(target_start_yyyymm, target_end_yyyymm)

        log_json(
            "raw_ingest_range_resolved",
            target_start_yyyymm=target_start_yyyymm,
            target_end_yyyymm=target_end_yyyymm,
            raw_ingest_months=raw_ingest_months
        )

        ingest_results = [ingest_one_month(m) for m in raw_ingest_months]

        any_new_data_loaded = any(r["status"] == "loaded" for r in ingest_results)
        all_sources_unavailable = all(r["status"] == "source_not_available_yet" for r in ingest_results)
        all_already_loaded = all(r["status"] == "already_loaded" for r in ingest_results)

        result = {
            "target_start_yyyymm": target_start_yyyymm,
            "target_end_yyyymm": target_end_yyyymm,
            "raw_ingest_months": raw_ingest_months,
            "ingest_results": ingest_results,
            "any_new_data_loaded": any_new_data_loaded,
            "all_sources_unavailable": all_sources_unavailable,
            "all_already_loaded": all_already_loaded
        }

        log_json("lambda_completed", result=result)
        return result

    except Exception:
        logger.exception("lambda_failed")
        raise
