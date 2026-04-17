import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
from playwright.sync_api import sync_playwright


APP_TIMEZONE = ZoneInfo(os.getenv("REPORT_TIMEZONE", "Asia/Almaty"))
MIN_LESSON_MINUTES_FOR_PAYROLL = int(os.getenv("MIN_LESSON_MINUTES_FOR_PAYROLL", "40"))

SHEET_COLUMNS = [
    "Name",
    "Role",
    "Duration",
    "Activity Score",
    "Talk Time",
    "Webcam Time",
    "Messages",
    "Reactions",
    "Poll Votes",
    "Raise Hands",
    "Join",
    "Left",
    "Status",
]

EXPORT_COLUMNS = SHEET_COLUMNS + ["Email"]

PRODUCT_ENV_KEYS = {
    "IELTS": "GOOGLE_SHEET_IELTS",
    "RTI": "GOOGLE_SHEET_RTI",
    "RTN": "GOOGLE_SHEET_RTN",
    "VIP": "GOOGLE_SHEET_VIP",
    "NUET": "GOOGLE_SHEET_NUET",
}

PRODUCT_ALIASES = {
    "IELTS": ["IELTS"],
    "RTI": ["RTI"],
    "RTN": ["RTN"],
    "VIP": ["VIP"],
    "NUET": ["NUET"],
}

LESSON_INDEX_COLUMNS = [
    "Lesson ID",
    "Product",
    "Flow",
    "Month Key",
    "Meeting Name",
    "Meeting Date",
    "Teacher",
    "Sheet Name",
    "Student Count",
    "Inserted At",
]

PAYROLL_LOG_COLUMNS = [
    "Lesson ID",
    "Month Key",
    "Product",
    "Flow",
    "Meeting Name",
    "Meeting Date",
    "Teacher",
    "Student Count",
]

LESSON_ARCHIVE_COLUMNS = [
    "Lesson ID",
    "Month Key",
    "Product",
    "Flow",
    "Meeting Name",
    "Meeting Date",
    "Teacher",
    "Student Count",
    "Lesson Payload",
]


def gspread_with_retry(func, *args, retries=5, **kwargs):
    retriable_codes = ("429", "500", "502", "503", "504")
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            message = str(exc)
            if any(code in message for code in retriable_codes) and attempt < retries - 1:
                wait = 20 + attempt * 15
                print(f"  Google API временно недоступен — жду {wait} сек...")
                time.sleep(wait)
            else:
                raise


def safe_batch_update(spreadsheet, body: dict):
    return gspread_with_retry(spreadsheet.batch_update, body)


def safe_sheet_title(title: str, fallback: str = "Sheet") -> str:
    cleaned = re.sub(r"[\[\]\*:/\\?]", " ", title)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return (cleaned or fallback)[:100]


def parse_meeting_datetime(meeting_date: str) -> datetime:
    normalized = meeting_date.strip()
    normalized = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", normalized, flags=re.IGNORECASE)
    patterns = [
        "%B %d %Y, %I:%M:%S %p",
        "%B %d, %Y, %I:%M:%S %p",
        "%b %d %Y, %I:%M:%S %p",
        "%b %d, %Y, %I:%M:%S %p",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(normalized, pattern)
        except ValueError:
            continue
    raise ValueError(f"Не удалось распарсить дату встречи: '{meeting_date}'")


def parse_join_left(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None

    patterns = [
        "%m/%d/%Y, %I:%M:%S %p",
        "%m/%d/%Y, %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    return None


def load_runtime_config() -> dict:
    mode = (os.getenv("SYNC_MODE") or "daily").strip().lower()
    start_raw = os.getenv("REPORT_START_DATE", "").strip()
    end_raw = os.getenv("REPORT_END_DATE", "").strip()

    if mode not in {"daily", "backfill", "all"}:
        raise ValueError("SYNC_MODE должен быть daily, backfill или all")

    start_date = datetime.strptime(start_raw, "%Y-%m-%d").date() if start_raw else None
    end_date = datetime.strptime(end_raw, "%Y-%m-%d").date() if end_raw else None

    if mode == "daily" and not start_date and not end_date:
        today_local = datetime.now(APP_TIMEZONE).date()
        target_date = today_local - timedelta(days=1)
        start_date = target_date
        end_date = target_date

    if start_date and not end_date:
        end_date = start_date
    if end_date and not start_date:
        start_date = end_date
    if start_date and end_date and start_date > end_date:
        raise ValueError("REPORT_START_DATE не может быть позже REPORT_END_DATE")

    return {
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
    }


def is_in_range(meeting_dt: datetime, config: dict) -> bool:
    if config["mode"] == "all":
        return True
    if not config["start_date"] or not config["end_date"]:
        return True
    return config["start_date"] <= meeting_dt.date() <= config["end_date"]


def detect_product(meeting_name: str) -> str | None:
    normalized = meeting_name.upper()
    for product, aliases in PRODUCT_ALIASES.items():
        if any(alias in normalized for alias in aliases):
            return product
    return None


def build_month_key(meeting_dt: datetime) -> str:
    return meeting_dt.strftime("%Y-%m")


def normalize_flow_name(meeting_name: str) -> str:
    cleaned = re.sub(r"\s+", " ", meeting_name).strip()
    return cleaned[:80]


def build_flow_month_sheet_name(flow_name: str, meeting_dt: datetime) -> str:
    return safe_sheet_title(f"{flow_name} | {build_month_key(meeting_dt)}", flow_name)


def compute_lesson_id(product: str, flow_name: str, meeting_dt: datetime) -> str:
    payload = f"{product}|{flow_name}|{meeting_dt.isoformat()}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def parse_duration_to_seconds(value) -> int:
    if pd.isna(value):
        return 0
    text = str(value).strip()
    if not text or text == "-":
        return 0
    parts = text.split(":")
    if len(parts) != 3:
        return 0
    try:
        hours, minutes, seconds = [int(part) for part in parts]
    except ValueError:
        return 0
    return hours * 3600 + minutes * 60 + seconds


def parse_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def derive_attendance_status(join_ts, left_ts, duration_seconds: int, lesson_start_ts, lesson_end_ts) -> str:
    if duration_seconds <= 0:
        return "No data"

    late = False
    left_early = False

    if pd.notna(join_ts) and pd.notna(lesson_start_ts):
        late = (join_ts - lesson_start_ts).total_seconds() > 10 * 60

    if pd.notna(left_ts) and pd.notna(lesson_end_ts):
        left_early = (lesson_end_ts - left_ts).total_seconds() > 10 * 60

    if late and left_early:
        return "Late + Left early"
    if late:
        return "Late"
    if left_early:
        return "Left early"
    return "Full lesson"


def detect_email_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Email",
        "E-mail",
        "Email Address",
        "Primary Email",
        "User Email",
        "Participant Email",
    ]
    for column in candidates:
        if column in df.columns:
            return column
    return None


def pick_teacher_name(df: pd.DataFrame) -> str:
    moderators = df[df["Role"] == "Moderator"].copy()
    if moderators.empty:
        return "Unknown"

    moderators["Talk Seconds"] = moderators["Talk Time"].apply(parse_duration_to_seconds)
    moderators["Webcam Seconds"] = moderators["Webcam Time"].apply(parse_duration_to_seconds)
    moderators["Duration Seconds"] = moderators["Duration"].apply(parse_duration_to_seconds)
    moderators["Messages Num"] = pd.to_numeric(moderators["Messages"], errors="coerce").fillna(0)
    moderators["JoinParsed"] = parse_datetime_series(moderators["Join"])

    moderators = moderators.sort_values(
        by=["Talk Seconds", "Webcam Seconds", "Messages Num", "Duration Seconds", "JoinParsed"],
        ascending=[False, False, False, False, True],
        na_position="last",
    )
    return str(moderators.iloc[0]["Name"]).strip() or "Unknown"


def get_lesson_duration_minutes(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    durations = df["Duration"].apply(parse_duration_to_seconds)
    if durations.empty:
        return 0
    return int(durations.max() // 60)


def prepare_dataframe(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df = df[df["Name"].notna() & (df["Name"].astype(str).str.strip() != "Anonymous")].copy()
    if df.empty:
        return df

    df["Role"] = df["Moderator"].apply(
        lambda value: "Moderator" if str(value).upper() == "TRUE" else "Student"
    )

    email_source = detect_email_column(df)
    if email_source:
        df["Email"] = df[email_source].fillna("").astype(str).str.strip()
    else:
        df["Email"] = ""

    for column in SHEET_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df["Name"] = df["Name"].astype(str).str.strip()
    df["JoinParsed"] = parse_datetime_series(df["Join"])
    df["LeftParsed"] = parse_datetime_series(df["Left"])
    df["Duration Seconds"] = df["Duration"].apply(parse_duration_to_seconds)

    lesson_start_ts = df["JoinParsed"].min()
    lesson_end_ts = df["LeftParsed"].max()

    df["Status"] = df.apply(
        lambda row: "Teacher"
        if row["Role"] == "Moderator"
        else derive_attendance_status(
            row["JoinParsed"],
            row["LeftParsed"],
            int(row["Duration Seconds"]),
            lesson_start_ts,
            lesson_end_ts,
        ),
        axis=1,
    )

    df["RoleSort"] = df["Role"].map({"Moderator": 0, "Student": 1}).fillna(2)
    df = df.sort_values(
        by=["RoleSort", "JoinParsed", "Name"],
        ascending=[True, True, True],
        na_position="last",
    )

    for column in ["Join", "Left", "Talk Time", "Webcam Time"]:
        df[column] = df[column].fillna("")

    return df[EXPORT_COLUMNS].copy()


def build_lesson_title(flow_name: str, meeting_dt: datetime, teacher_name: str, student_count: int) -> str:
    return (
        f"{flow_name} | {meeting_dt.strftime('%d.%m.%Y %H:%M')} | "
        f"Teacher: {teacher_name} | Students: {student_count}"
    )


def ensure_worksheet(spreadsheet, title: str, rows: int = 2000, cols: int = 20):
    safe_title = safe_sheet_title(title)
    try:
        worksheet = gspread_with_retry(spreadsheet.worksheet, safe_title)
        created = False
    except gspread.exceptions.WorksheetNotFound:
        worksheet = gspread_with_retry(
            spreadsheet.add_worksheet,
            title=safe_title,
            rows=str(rows),
            cols=str(cols),
        )
        created = True
    return worksheet, created


def ensure_header(worksheet, header_values: list[str]):
    first_row = gspread_with_retry(worksheet.row_values, 1)
    if first_row != header_values:
        gspread_with_retry(worksheet.update, range_name="A1", values=[header_values])


def apply_sheet_basics(spreadsheet, worksheet, header_len: int):
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": header_len,
                    }
                }
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": header_len,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.16, "green": 0.38, "blue": 0.62},
                        "textFormat": {
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                            "bold": True,
                            "fontSize": 10,
                        },
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
    ]
    safe_batch_update(spreadsheet, {"requests": requests})


def format_lesson_block(spreadsheet, worksheet, title_row_number: int, student_rows: int, header_len: int):
    end_student_row = title_row_number + student_rows
    requests = [
        {
            "mergeCells": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": title_row_number - 1,
                    "endRowIndex": title_row_number,
                    "startColumnIndex": 0,
                    "endColumnIndex": header_len,
                },
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": title_row_number - 1,
                    "endRowIndex": title_row_number,
                    "startColumnIndex": 0,
                    "endColumnIndex": header_len,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.90, "green": 0.94, "blue": 0.98},
                        "textFormat": {
                            "bold": True,
                            "fontSize": 11,
                            "foregroundColor": {"red": 0.12, "green": 0.24, "blue": 0.38},
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": title_row_number,
                        "endRowIndex": end_student_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": header_len,
                    },
                    "rowProperties": {
                        "firstBandColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        "secondBandColor": {"red": 0.97, "green": 0.98, "blue": 0.99},
                    },
                }
            }
        },
    ]
    safe_batch_update(spreadsheet, {"requests": requests})


def open_product_spreadsheets(gc):
    spreadsheets = {}
    missing = []

    for product, env_key in PRODUCT_ENV_KEYS.items():
        spreadsheet_id = (os.getenv(env_key) or "").strip()
        if not spreadsheet_id:
            missing.append(env_key)
            continue

        print(f"Пробую открыть {product}: {spreadsheet_id}")
        try:
            spreadsheets[product] = gspread_with_retry(gc.open_by_key, spreadsheet_id, retries=5)
            print(f"ОК: {product}")
        except Exception as exc:
            print(f"Ошибка для {product} ({spreadsheet_id}): {exc}")
            raise

    if missing:
        raise ValueError(f"Не заданы переменные окружения: {', '.join(missing)}")

    return spreadsheets


def ensure_meta_sheet(spreadsheet, title: str, columns: list[str]):
    worksheet, created = ensure_worksheet(spreadsheet, title, rows=2000, cols=max(20, len(columns)))
    ensure_header(worksheet, columns)
    if created:
        safe_batch_update(
            spreadsheet,
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": worksheet.id,
                                "hidden": True,
                                "gridProperties": {"frozenRowCount": 1},
                            },
                            "fields": "hidden,gridProperties.frozenRowCount",
                        }
                    }
                ]
            },
        )
    return worksheet


def get_existing_lesson_ids(index_ws) -> set[str]:
    values = gspread_with_retry(index_ws.col_values, 1)
    return {value for value in values[1:] if value}


def append_index_row(index_ws, row: list[str]):
    gspread_with_retry(index_ws.append_row, row, value_input_option="RAW")


def append_payroll_row(payroll_ws, row: list[str]):
    gspread_with_retry(payroll_ws.append_row, row, value_input_option="RAW")


def append_archive_row(archive_ws, row: list[str]):
    gspread_with_retry(archive_ws.append_row, row, value_input_option="RAW")


def load_payroll_log(payroll_ws) -> pd.DataFrame:
    values = gspread_with_retry(payroll_ws.get_all_values)
    if len(values) <= 1:
        return pd.DataFrame(columns=PAYROLL_LOG_COLUMNS)
    return pd.DataFrame(values[1:], columns=values[0])


def load_archive_log(archive_ws) -> pd.DataFrame:
    values = gspread_with_retry(archive_ws.get_all_values)
    if len(values) <= 1:
        return pd.DataFrame(columns=LESSON_ARCHIVE_COLUMNS)
    return pd.DataFrame(values[1:], columns=values[0])


def ensure_data_rows(worksheet, required_rows: int, required_cols: int):
    if worksheet.row_count < required_rows:
        gspread_with_retry(worksheet.add_rows, required_rows - worksheet.row_count)
    if worksheet.col_count < required_cols:
        gspread_with_retry(worksheet.add_cols, required_cols - worksheet.col_count)


def write_lesson_sheet(spreadsheet, flow_name: str, meeting_dt: datetime, teacher_name: str, df: pd.DataFrame):
    sheet_name = build_flow_month_sheet_name(flow_name, meeting_dt)
    worksheet, _ = ensure_worksheet(spreadsheet, sheet_name, rows=2500, cols=max(20, len(SHEET_COLUMNS)))

    ensure_header(worksheet, SHEET_COLUMNS)
    apply_sheet_basics(spreadsheet, worksheet, len(SHEET_COLUMNS))

    next_row = len(gspread_with_retry(worksheet.get_all_values)) + 1
    lesson_title = build_lesson_title(
        flow_name=flow_name,
        meeting_dt=meeting_dt,
        teacher_name=teacher_name,
        student_count=int((df["Role"] == "Student").sum()),
    )
    title_row = [lesson_title] + [""] * (len(SHEET_COLUMNS) - 1)
    spacer_row = [""] * len(SHEET_COLUMNS)
    rows_to_append = [spacer_row, title_row] + df[SHEET_COLUMNS].values.tolist()

    required_rows = next_row + len(rows_to_append) + 10
    ensure_data_rows(worksheet, required_rows, len(SHEET_COLUMNS))
    gspread_with_retry(worksheet.append_rows, rows_to_append, value_input_option="RAW")
    format_lesson_block(spreadsheet, worksheet, next_row + 1, len(df), len(SHEET_COLUMNS))
    return sheet_name


def build_lesson_payload(product: str, flow_name: str, meeting_name: str, meeting_dt: datetime, teacher_name: str, df: pd.DataFrame) -> str:
    students_df = df[df["Role"] == "Student"].copy()
    rows = []

    join_values = []
    left_values = []

    for _, row in df.iterrows():
        join_dt = parse_join_left(row["Join"])
        left_dt = parse_join_left(row["Left"])

        if join_dt:
            join_values.append(join_dt)
        if left_dt:
            left_values.append(left_dt)

        rows.append(
            {
                "name": str(row["Name"]),
                "email": str(row.get("Email", "")),
                "role": str(row["Role"]),
                "duration": str(row["Duration"]),
                "activity_score": str(row["Activity Score"]),
                "talk_time": str(row["Talk Time"]),
                "webcam_time": str(row["Webcam Time"]),
                "messages": str(row["Messages"]),
                "join": str(row["Join"]),
                "left": str(row["Left"]),
                "status": str(row["Status"]),
            }
        )

    status_counts = students_df["Status"].value_counts().to_dict() if not students_df.empty else {}

    lesson_start = min(join_values).strftime("%H:%M") if join_values else meeting_dt.strftime("%H:%M")
    lesson_end = max(left_values).strftime("%H:%M") if left_values else meeting_dt.strftime("%H:%M")

    payload = {
        "lesson_id": compute_lesson_id(product, flow_name, meeting_dt),
        "product": product,
        "flow": flow_name,
        "meeting_name": meeting_name,
        "meeting_date": meeting_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "month_key": build_month_key(meeting_dt),
        "teacher": teacher_name,
        "student_count": int((df["Role"] == "Student").sum()),
        "moderator_count": int((df["Role"] == "Moderator").sum()),
        "status_counts": status_counts,
        "lesson_start": lesson_start,
        "lesson_end": lesson_end,
        "rows": rows,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def find_next_page_control(page):
    selectors = [
        "a[rel='next']",
        "button[rel='next']",
        "a:has-text('Next')",
        "button:has-text('Next')",
        "a:has-text('›')",
        "button:has-text('›')",
        "a:has-text('>')",
        "button:has-text('>')",
        "[aria-label='Next page']",
        "[aria-label='Next']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() and locator.is_visible():
                disabled_attr = (locator.get_attribute("disabled") or "").lower()
                aria_disabled = (locator.get_attribute("aria-disabled") or "").lower()
                class_name = (locator.get_attribute("class") or "").lower()
                if disabled_attr or aria_disabled == "true" or "disabled" in class_name:
                    continue
                return locator
        except Exception:
            continue
    return None


def rebuild_payroll_summary(spreadsheet, month_key: str):
    payroll_log_ws = ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)
    df = load_payroll_log(payroll_log_ws)
    if df.empty:
        return

    month_df = df[df["Month Key"] == month_key].copy()
    if month_df.empty:
        return

    group_counts = (
        month_df.groupby(["Flow", "Teacher"])
        .agg(
            lesson_count=("Lesson ID", "count"),
            dates=("Meeting Date", lambda s: ", ".join(sorted(set(s)))),
        )
        .reset_index()
    )

    main_teacher_map = {}
    for flow, flow_df in group_counts.groupby("Flow"):
        flow_df = flow_df.sort_values(by=["lesson_count", "Teacher"], ascending=[False, True])
        main_teacher_map[flow] = flow_df.iloc[0]["Teacher"]

    group_rows = []
    for flow in sorted(month_df["Flow"].unique()):
        flow_df = month_df[month_df["Flow"] == flow]
        flow_counts = group_counts[group_counts["Flow"] == flow]
        main_teacher = main_teacher_map[flow]
        replacement_rows = flow_counts[flow_counts["Teacher"] != main_teacher]
        group_rows.append(
            [
                flow,
                str(flow_df.iloc[0]["Product"]),
                main_teacher,
                str(int(flow_counts.loc[flow_counts["Teacher"] == main_teacher, "lesson_count"].sum())),
                ", ".join(replacement_rows["Teacher"].tolist()) or "-",
                str(int(replacement_rows["lesson_count"].sum()) if not replacement_rows.empty else 0),
                ", ".join(replacement_rows["dates"].tolist()) if not replacement_rows.empty else "-",
                str(int(flow_df["Lesson ID"].count())),
            ]
        )

    teacher_rows = []
    teacher_group = (
        month_df.groupby("Teacher")
        .agg(
            total_lessons=("Lesson ID", "count"),
            groups=("Flow", lambda s: ", ".join(sorted(set(s)))),
            replacement_dates=("Meeting Date", lambda s: ", ".join(sorted(set(s)))),
        )
        .reset_index()
    )
    for _, row in teacher_group.iterrows():
        teacher = str(row["Teacher"])
        main_lesson_count = sum(
            1
            for flow in month_df[month_df["Teacher"] == teacher]["Flow"]
            if main_teacher_map.get(flow) == teacher
        )
        teacher_rows.append(
            [
                teacher,
                str(int(row["total_lessons"])),
                str(main_lesson_count),
                str(int(row["total_lessons"]) - int(main_lesson_count)),
                row["groups"],
                row["replacement_dates"] if int(row["total_lessons"]) - int(main_lesson_count) else "-",
            ]
        )

    summary_ws, _ = ensure_worksheet(spreadsheet, safe_sheet_title(f"Payroll {month_key}"), rows=500, cols=10)
    gspread_with_retry(summary_ws.clear)
    values = [
        [f"Payroll Summary {month_key}"],
        [],
        ["By group"],
        ["Group", "Product", "Main teacher", "Main lessons", "Replacement teacher", "Replacement lessons", "Replacement dates", "Total lessons"],
        *group_rows,
        [],
        ["By teacher"],
        ["Teacher", "Total lessons", "Main lessons", "Replacement lessons", "Groups", "Replacement dates"],
        *teacher_rows,
    ]
    gspread_with_retry(summary_ws.update, range_name="A1", values=values)


def export_dashboard_site(spreadsheets: dict):
    root_data_dir = os.path.abspath("data")
    site_data_dir = os.path.abspath(os.path.join("site", "data"))

    os.makedirs(root_data_dir, exist_ok=True)
    os.makedirs(site_data_dir, exist_ok=True)

    dashboard = {
        "generated_at": datetime.now(APP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": str(APP_TIMEZONE),
        "products": {},
    }

    for product, spreadsheet in spreadsheets.items():
        payroll_ws = ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)
        archive_ws = ensure_meta_sheet(spreadsheet, "_LESSON_ARCHIVE", LESSON_ARCHIVE_COLUMNS)

        payroll_df = load_payroll_log(payroll_ws)
        archive_df = load_archive_log(archive_ws)

        lessons = []
        for _, row in archive_df.iterrows():
            payload_raw = row.get("Lesson Payload", "")
            if not payload_raw:
                continue
            try:
                lessons.append(json.loads(payload_raw))
            except json.JSONDecodeError:
                continue

        lessons.sort(key=lambda item: item.get("meeting_date", ""), reverse=True)

        months = {}
        parallel_load = {}
        flows_set = set()
        teachers_set = set()

        for lesson in lessons:
            month_key = lesson.get("month_key", "unknown")
            flow_name = lesson.get("flow", "")
            teacher_name = lesson.get("teacher", "")
            student_count = int(lesson.get("student_count", 0))

            flows_set.add(flow_name)
            teachers_set.add(teacher_name)

            bucket = months.setdefault(
                month_key,
                {
                    "month_key": month_key,
                    "lesson_count": 0,
                    "flows": set(),
                    "teachers": set(),
                    "student_total": 0,
                },
            )
            bucket["lesson_count"] += 1
            bucket["flows"].add(flow_name)
            bucket["teachers"].add(teacher_name)
            bucket["student_total"] += student_count

            lesson_start = lesson.get("lesson_start")
            lesson_end = lesson.get("lesson_end")

            if lesson_start and lesson_end:
                try:
                    start_dt = datetime.strptime(lesson_start, "%H:%M")
                    end_dt = datetime.strptime(lesson_end, "%H:%M")
                    cursor = start_dt.replace(minute=0)
                    if cursor > start_dt:
                        cursor -= timedelta(hours=1)

                    while cursor <= end_dt:
                        slot = cursor.strftime("%H:00")
                        load_bucket = parallel_load.setdefault(
                            slot,
                            {"hour": slot, "groups": 0, "students": 0, "flows": set()},
                        )
                        if flow_name not in load_bucket["flows"]:
                            load_bucket["groups"] += 1
                            load_bucket["flows"].add(flow_name)
                        load_bucket["students"] += student_count
                        cursor += timedelta(hours=1)
                except ValueError:
                    pass

        month_list = []
        for month in sorted(months.keys(), reverse=True):
            bucket = months[month]
            month_list.append(
                {
                    "month_key": month,
                    "lesson_count": bucket["lesson_count"],
                    "flow_count": len([x for x in bucket["flows"] if x]),
                    "teacher_count": len([x for x in bucket["teachers"] if x]),
                    "student_total": bucket["student_total"],
                }
            )

        parallel_load_list = []
        for key in sorted(parallel_load.keys()):
            row = parallel_load[key]
            parallel_load_list.append(
                {
                    "hour": row["hour"],
                    "groups": row["groups"],
                    "students": row["students"],
                }
            )

        payroll_by_group = []
        payroll_by_teacher = []
        if not payroll_df.empty:
            for month_key in sorted(payroll_df["Month Key"].unique(), reverse=True):
                month_df = payroll_df[payroll_df["Month Key"] == month_key].copy()

                group_counts = (
                    month_df.groupby(["Flow", "Teacher"])
                    .agg(
                        lesson_count=("Lesson ID", "count"),
                        dates=("Meeting Date", lambda s: ", ".join(sorted(set(s)))),
                    )
                    .reset_index()
                )

                main_teacher_map = {}
                for flow, flow_df in group_counts.groupby("Flow"):
                    flow_df = flow_df.sort_values(by=["lesson_count", "Teacher"], ascending=[False, True])
                    main_teacher_map[flow] = flow_df.iloc[0]["Teacher"]

                for flow in sorted(month_df["Flow"].unique()):
                    flow_df = month_df[month_df["Flow"] == flow]
                    flow_counts = group_counts[group_counts["Flow"] == flow]
                    main_teacher = main_teacher_map[flow]
                    replacement_rows = flow_counts[flow_counts["Teacher"] != main_teacher]

                    payroll_by_group.append(
                        {
                            "month_key": month_key,
                            "group": flow,
                            "product": product,
                            "main_teacher": main_teacher,
                            "main_lessons": int(
                                flow_counts.loc[flow_counts["Teacher"] == main_teacher, "lesson_count"].sum()
                            ),
                            "replacement_teacher": ", ".join(replacement_rows["Teacher"].tolist()) or "-",
                            "replacement_lessons": int(replacement_rows["lesson_count"].sum())
                            if not replacement_rows.empty else 0,
                            "replacement_dates": ", ".join(replacement_rows["dates"].tolist())
                            if not replacement_rows.empty else "-",
                            "total_lessons": int(flow_df["Lesson ID"].count()),
                        }
                    )

                teacher_group = (
                    month_df.groupby("Teacher")
                    .agg(
                        total_lessons=("Lesson ID", "count"),
                        groups=("Flow", lambda s: ", ".join(sorted(set(s)))),
                        replacement_dates=("Meeting Date", lambda s: ", ".join(sorted(set(s)))),
                    )
                    .reset_index()
                )
                for _, teacher_row in teacher_group.iterrows():
                    teacher = str(teacher_row["Teacher"])
                    main_lesson_count = sum(
                        1
                        for flow in month_df[month_df["Teacher"] == teacher]["Flow"]
                        if main_teacher_map.get(flow) == teacher
                    )

                    payroll_by_teacher.append(
                        {
                            "month_key": month_key,
                            "teacher": teacher,
                            "total_lessons": int(teacher_row["total_lessons"]),
                            "main_lessons": int(main_lesson_count),
                            "replacement_lessons": int(teacher_row["total_lessons"]) - int(main_lesson_count),
                            "groups": teacher_row["groups"],
                            "replacement_dates": teacher_row["replacement_dates"],
                        }
                    )

        dashboard["products"][product] = {
            "spreadsheet_title": spreadsheet.title,
            "lesson_count": len(lessons),
            "flow_count": len([x for x in flows_set if x]),
            "teacher_count": len([x for x in teachers_set if x]),
            "months": month_list,
            "all_flows": sorted([x for x in flows_set if x]),
            "parallel_load": parallel_load_list,
            "lessons": lessons,
            "payroll_by_group": payroll_by_group,
            "payroll_by_teacher": payroll_by_teacher,
        }

    for output_path in [
        os.path.join(root_data_dir, "dashboard_data.json"),
        os.path.join(site_data_dir, "dashboard_data.json"),
    ]:
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(dashboard, handle, ensure_ascii=False, indent=2)

    print("Dashboard JSON обновлён")


def run_bot(config: dict) -> list[dict]:
    password = os.getenv("BBB_PASSWORD")
    if not password:
        raise ValueError("BBB_PASSWORD не задан!")

    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        print("BOT: Открываю страницу логина...")
        page.goto("https://biggerbluebutton.com/login", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        btn = page.query_selector("button:has-text('Sign In')")
        if btn:
            btn.click()
            page.wait_for_timeout(1500)

        inputs = page.query_selector_all("input")
        visible_text, visible_pass = None, None
        for inp in inputs:
            input_type = inp.get_attribute("type") or "text"
            if inp.is_visible() and input_type in ("text", "email") and visible_text is None:
                visible_text = inp
            if inp.is_visible() and input_type == "password" and visible_pass is None:
                visible_pass = inp

        if not visible_text or not visible_pass:
            page.screenshot(path="debug_login.png", full_page=True)
            raise RuntimeError("Поля логина не найдены.")

        visible_text.click()
        page.keyboard.type("260401190051930", delay=50)
        visible_pass.click()
        page.keyboard.type(password, delay=50)
        page.wait_for_timeout(500)

        print("BOT: Нажимаю SIGN IN...")
        try:
            with page.expect_navigation(timeout=15000):
                page.click('button:has-text("SIGN IN")')
        except Exception:
            page.wait_for_timeout(5000)

        page.screenshot(path="debug_login.png", full_page=True)
        if "login" in page.url:
            raise RuntimeError("Авторизация не прошла. Смотри debug_login.png")

        print("BOT: Авторизован!")

        page.goto("https://biggerbluebutton.com/rooms/meetings", wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        page.screenshot(path="debug_meetings.png", full_page=True)

        saved_files = []
        skipped_by_date = 0
        page_number = 1

        while True:
            rows = page.query_selector_all("tr")
            print(f"BOT: Страница {page_number}, строк в таблице: {len(rows)}")

            reached_older_than_range = False
            downloads_on_page = 0

            for i, row in enumerate(rows):
                report_link = row.query_selector("a:has-text('Report.CSV'), a:has-text('Report.csv')")
                if not report_link:
                    continue

                cells = row.query_selector_all("td")
                meeting_name = f"Meeting_{page_number}_{i}"
                if cells:
                    name_link = cells[0].query_selector("a")
                    meeting_name = name_link.inner_text().strip() if name_link else cells[0].inner_text().strip()
                    if not meeting_name:
                        meeting_name = f"Meeting_{page_number}_{i}"

                meeting_date = cells[1].inner_text().strip() if len(cells) > 1 else ""
                try:
                    meeting_dt = parse_meeting_datetime(meeting_date)
                except ValueError:
                    print(f"BOT: Пропускаю '{meeting_name}' — не удалось разобрать дату.")
                    continue

                if config["start_date"] and meeting_dt.date() < config["start_date"]:
                    reached_older_than_range = True
                    skipped_by_date += 1
                    continue

                if not is_in_range(meeting_dt, config):
                    skipped_by_date += 1
                    continue

                print(f"BOT: Скачиваю '{meeting_name}' ({meeting_date})...")
                try:
                    with page.expect_download(timeout=30000) as dl_info:
                        report_link.click()

                    download = dl_info.value
                    safe_name = re.sub(r'[\\/*?:"<>|]', "_", meeting_name)
                    filename = f"{safe_name}_{page_number}_{i}.csv"
                    save_path = os.path.join(download_dir, filename)
                    download.save_as(save_path)
                    saved_files.append(
                        {
                            "meeting_name": meeting_name,
                            "meeting_date": meeting_date,
                            "meeting_dt": meeting_dt,
                            "file_path": save_path,
                        }
                    )
                    downloads_on_page += 1
                    page.wait_for_timeout(500)
                except Exception as exc:
                    print(f"BOT: Ошибка при скачивании '{meeting_name}': {exc}")

            if reached_older_than_range:
                print("BOT: Дошёл до уроков старше нижней границы периода, останавливаюсь.")
                break

            next_control = find_next_page_control(page)
            if not next_control:
                print("BOT: Следующая страница не найдена, пагинация завершена.")
                break

            before_marker = rows[-1].inner_text().strip() if rows else ""
            print(f"BOT: Перехожу на страницу {page_number + 1}...")
            try:
                next_control.click()
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(2000)
            except Exception as exc:
                print(f"BOT: Не удалось открыть следующую страницу: {exc}")
                break

            new_rows = page.query_selector_all("tr")
            after_marker = new_rows[-1].inner_text().strip() if new_rows else ""
            if before_marker and before_marker == after_marker and downloads_on_page == 0:
                print("BOT: Похоже, страница не сменилась, прекращаю пагинацию.")
                break

            page_number += 1

        browser.close()
        print(f"BOT: Пропущено по диапазону дат: {skipped_by_date}")
        print(f"BOT: Всего скачано: {len(saved_files)} файлов")
        return saved_files


def process_lessons(files: list[dict], spreadsheets: dict):
    impacted_months_by_product = {product: set() for product in spreadsheets}

    for product, spreadsheet in spreadsheets.items():
        ensure_meta_sheet(spreadsheet, "_LESSON_INDEX", LESSON_INDEX_COLUMNS)
        ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)
        ensure_meta_sheet(spreadsheet, "_LESSON_ARCHIVE", LESSON_ARCHIVE_COLUMNS)

    existing_ids_by_product = {
        product: get_existing_lesson_ids(ensure_meta_sheet(spreadsheet, "_LESSON_INDEX", LESSON_INDEX_COLUMNS))
        for product, spreadsheet in spreadsheets.items()
    }
    existing_archive_ids_by_product = {
        product: get_existing_lesson_ids(ensure_meta_sheet(spreadsheet, "_LESSON_ARCHIVE", LESSON_ARCHIVE_COLUMNS))
        for product, spreadsheet in spreadsheets.items()
    }

    for item in files:
        meeting_name = item["meeting_name"]
        meeting_dt = item["meeting_dt"]
        file_path = item["file_path"]

        product = detect_product(meeting_name)
        if not product:
            print(f"  Пропускаю '{meeting_name}' — продукт не распознан.")
            continue

        flow_name = normalize_flow_name(meeting_name)
        lesson_id = compute_lesson_id(product, flow_name, meeting_dt)

        try:
            df = prepare_dataframe(file_path)
        except Exception as exc:
            print(f"  ОШИБКА чтения '{meeting_name}': {exc}")
            continue

        if df.empty:
            print(f"  Пропускаю '{meeting_name}' — нет данных.")
            continue

        teacher_name = pick_teacher_name(df)
        spreadsheet = spreadsheets[product]
        month_key = build_month_key(meeting_dt)
        student_count = int((df["Role"] == "Student").sum())
        lesson_duration_minutes = get_lesson_duration_minutes(df)
        count_for_payroll = lesson_duration_minutes >= MIN_LESSON_MINUTES_FOR_PAYROLL

        lesson_already_exists = lesson_id in existing_ids_by_product[product]
        archive_exists = lesson_id in existing_archive_ids_by_product[product]

        if lesson_already_exists and archive_exists:
            print(f"  Пропускаю '{meeting_name}' — уже загружен ({lesson_id}).")
            continue

        if lesson_already_exists:
            print(f"  [{product}] Урок '{flow_name}' уже в таблице, дозаполняю архив сайта.")
        else:
            sheet_name = write_lesson_sheet(spreadsheet, flow_name, meeting_dt, teacher_name, df)
            inserted_at = datetime.now(APP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

            lesson_index_ws = ensure_meta_sheet(spreadsheet, "_LESSON_INDEX", LESSON_INDEX_COLUMNS)
            append_index_row(
                lesson_index_ws,
                [
                    lesson_id,
                    product,
                    flow_name,
                    month_key,
                    meeting_name,
                    meeting_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    teacher_name,
                    sheet_name,
                    str(student_count),
                    inserted_at,
                ],
            )

            if count_for_payroll:
                payroll_log_ws = ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)
                append_payroll_row(
                    payroll_log_ws,
                    [
                        lesson_id,
                        month_key,
                        product,
                        flow_name,
                        meeting_name,
                        meeting_dt.strftime("%Y-%m-%d"),
                        teacher_name,
                        str(student_count),
                    ],
                )
            else:
                print(f"  [{product}] Урок '{flow_name}' слишком короткий ({lesson_duration_minutes} мин), не считаю в payroll")

        archive_ws = ensure_meta_sheet(spreadsheet, "_LESSON_ARCHIVE", LESSON_ARCHIVE_COLUMNS)
        if not archive_exists:
            append_archive_row(
                archive_ws,
                [
                    lesson_id,
                    month_key,
                    product,
                    flow_name,
                    meeting_name,
                    meeting_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    teacher_name,
                    str(student_count),
                    build_lesson_payload(product, flow_name, meeting_name, meeting_dt, teacher_name, df),
                ],
            )
            existing_archive_ids_by_product[product].add(lesson_id)

        if not lesson_already_exists:
            existing_ids_by_product[product].add(lesson_id)

        impacted_months_by_product[product].add(month_key)
        print(f"  [{product}] Добавлен урок '{flow_name}' за {month_key}, teacher={teacher_name}")
        time.sleep(2)

    for product, month_keys in impacted_months_by_product.items():
        if not month_keys:
            continue
        spreadsheet = spreadsheets[product]
        for month_key in sorted(month_keys):
            print(f"  [{product}] Пересчитываю payroll summary для {month_key}")
            rebuild_payroll_summary(spreadsheet, month_key)

    export_dashboard_site(spreadsheets)


if __name__ == "__main__":
    print("=== СТАРТ ===")

    try:
        config = load_runtime_config()
        print("Режим:", config["mode"], "| диапазон:", config["start_date"], "—", config["end_date"])
    except Exception as exc:
        print(f"ОШИБКА конфигурации: {exc}")
        raise SystemExit(1)

    try:
        service_account_info = json.loads(os.getenv("GOOGLE_JSON"))
        gc = gspread.service_account_from_dict(service_account_info)
        print("Google авторизация: ОК")
    except Exception as exc:
        print(f"ОШИБКА Google авторизации: {exc}")
        raise SystemExit(1)

    try:
        spreadsheets = open_product_spreadsheets(gc)
        for product, spreadsheet in spreadsheets.items():
            print(f"Таблица для {product}: '{spreadsheet.title}'")
    except Exception as exc:
        print(f"ОШИБКА открытия таблиц: {exc}")
        raise SystemExit(1)

    try:
        files = run_bot(config)
    except Exception as exc:
        print(f"ОШИБКА бота: {exc}")
        raise SystemExit(1)

    try:
        process_lessons(files, spreadsheets)
    except Exception as exc:
        print(f"ОШИБКА обработки уроков: {exc}")
        raise SystemExit(1)

    print("=== ГОТОВО ===")
