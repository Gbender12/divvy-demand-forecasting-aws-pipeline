import json
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_json(message, **fields):
    logger.info("%s | %s", message, json.dumps(fields, default=str))


RAW_INGEST_EXTRA_LOOKBACK_MONTHS = 1
TRIPDATA_CURATION_EXTRA_LOOKBACK_MONTHS = 1
WEATHER_REFRESH_EXTRA_LOOKBACK_MONTHS = 1

AGGREGATE_JOB_EXPANDS_INTERNALLY = True


def previous_month_yyyymm(today=None):
    if today is None:
        today = date.today()

    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    return last_day_prev_month.strftime("%Y%m")


def parse_yyyymm(yyyymm: str):
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"Invalid YYYYMM value: {yyyymm}")

    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])

    if month_num < 1 or month_num > 12:
        raise ValueError(f"Invalid month in YYYYMM: {yyyymm}")

    return year_num, month_num


def format_yyyymm(year_num: int, month_num: int):
    return f"{year_num:04d}{month_num:02d}"


def shift_month(yyyymm: str, offset: int):
    year_num, month_num = parse_yyyymm(yyyymm)

    absolute_month = year_num * 12 + (month_num - 1)
    shifted = absolute_month + offset

    shifted_year = shifted // 12
    shifted_month = (shifted % 12) + 1

    return format_yyyymm(shifted_year, shifted_month)


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


def resolve_requested_range(event):
    event = event or {}

    today = None
    if event.get("today"):
        today = datetime.strptime(event["today"], "%Y-%m-%d").date()

    target_start = event.get("target_start_yyyymm")
    target_end = event.get("target_end_yyyymm")

    if not target_start and not target_end:
        prev_month = previous_month_yyyymm(today)
        return prev_month, prev_month

    if target_start and not target_end:
        return target_start, target_start

    if target_end and not target_start:
        raise ValueError(
            "target_start_yyyymm is required when target_end_yyyymm is provided"
        )

    return target_start, target_end


def lambda_handler(event, context):
    try:
        log_json(
            "resolve_date_range_started",
            request_id=context.aws_request_id,
            event=event or {}
        )

        requested_start_yyyymm, requested_end_yyyymm = resolve_requested_range(event)

        requested_months = month_range(
            requested_start_yyyymm,
            requested_end_yyyymm
        )

        raw_ingest_start_yyyymm = shift_month(
            requested_start_yyyymm,
            -RAW_INGEST_EXTRA_LOOKBACK_MONTHS
        )
        raw_ingest_end_yyyymm = requested_end_yyyymm

        tripdata_start_yyyymm = shift_month(
            requested_start_yyyymm,
            -TRIPDATA_CURATION_EXTRA_LOOKBACK_MONTHS
        )
        tripdata_end_yyyymm = requested_end_yyyymm

        weather_start_yyyymm = shift_month(
            requested_start_yyyymm,
            -WEATHER_REFRESH_EXTRA_LOOKBACK_MONTHS
        )
        weather_end_yyyymm = requested_end_yyyymm

        aggregate_start_yyyymm = requested_start_yyyymm
        aggregate_end_yyyymm = requested_end_yyyymm

        result = {
            "requested_start_yyyymm": requested_start_yyyymm,
            "requested_end_yyyymm": requested_end_yyyymm,
            "requested_months": requested_months,

            "overlap_policy": {
                "reason": (
                    "Monthly Divvy trip files may contain rides whose started_at belongs "
                    "to the previous month, so raw ingest and tripdata curation are widened "
                    "by one month. Weather does not require the same overlap refresh."
                ),
                "raw_ingest_extra_lookback_months": RAW_INGEST_EXTRA_LOOKBACK_MONTHS,
                "tripdata_curation_extra_lookback_months": TRIPDATA_CURATION_EXTRA_LOOKBACK_MONTHS,
                "weather_refresh_extra_lookback_months": WEATHER_REFRESH_EXTRA_LOOKBACK_MONTHS,
                "aggregate_note": (
                    "Aggregate job expands one month backward internally, so the requested "
                    "range is passed to the job."
                )
            },

            "raw_ingest": {
                "target_start_yyyymm": raw_ingest_start_yyyymm,
                "target_end_yyyymm": raw_ingest_end_yyyymm,
                "months": month_range(raw_ingest_start_yyyymm, raw_ingest_end_yyyymm)
            },

            "tripdata_curation": {
                "target_start_yyyymm": tripdata_start_yyyymm,
                "target_end_yyyymm": tripdata_end_yyyymm,
                "months": month_range(tripdata_start_yyyymm, tripdata_end_yyyymm),
                "glue_arguments": {
                    "--target_start_yyyymm": tripdata_start_yyyymm,
                    "--target_end_yyyymm": tripdata_end_yyyymm
                }
            },

            "weather_refresh": {
                "target_start_yyyymm": weather_start_yyyymm,
                "target_end_yyyymm": weather_end_yyyymm,
                "months": month_range(weather_start_yyyymm, weather_end_yyyymm),
                "glue_arguments": {
                    "--target_start_yyyymm": weather_start_yyyymm,
                    "--target_end_yyyymm": weather_end_yyyymm
                }
            },

            "aggregate_refresh": {
                "target_start_yyyymm": aggregate_start_yyyymm,
                "target_end_yyyymm": aggregate_end_yyyymm,
                "months_requested": month_range(aggregate_start_yyyymm, aggregate_end_yyyymm),
                "job_expands_one_month_backward_internally": AGGREGATE_JOB_EXPANDS_INTERNALLY,
                "glue_arguments": {
                    "--target_start_yyyymm": aggregate_start_yyyymm,
                    "--target_end_yyyymm": aggregate_end_yyyymm
                }
            },

            "model_training": {
                "glue_arguments": {}
            }
        }

        log_json("resolve_date_range_completed", result=result)
        return result

    except Exception:
        logger.exception("resolve_date_range_failed")
        raise
