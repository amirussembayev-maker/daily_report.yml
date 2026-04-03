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

# ──────────────────────────────────────────────
# 1. КОНФИГ
# ──────────────────────────────────────────────


APP_TIMEZONE = ZoneInfo(os.getenv("REPORT_TIMEZONE", "Asia/Almaty"))

COLUMNS = [
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

MONTH_NAMES_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
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


# ──────────────────────────────────────────────
# 2. HELPERS
# ──────────────────────────────────────────────


def gspread_with_retry(func, *args, retries=5, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            if "429" in str(exc) and attempt < retries - 1:
                wait = 60 + attempt * 20
                print(f"  Rate limit — жду {wait} сек...")
                time.sleep(wait)
            else:
                raise


def safe_batch_update(spreadsheet, body: dict):
    return gspread_with_retry(spreadsheet.batch_update, body)


def safe_sheet_title(title: str, fallback: str = "Sheet") -> str:
    cleaned = re.sub(r"[\[\]\*:/\\?]", " ", title)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return (cleaned or fallback)[:100]


def col_to_letter(col_idx: int) -> str:
    result = ""
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        result = chr(65 + rem) + result
    return result


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


def build_month_label(meeting_dt: datetime) -> str:
    return f"{MONTH_NAMES_RU[meeting_dt.month]} {meeting_dt.year}"


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


def derive_attendance_status(
    join_ts,
    left_ts,
    duration_seconds: int,
    lesson_start_ts,
    lesson_end_ts,
) -> str:
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


def prepare_dataframe(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df = df[df["Name"].notna() & (df["Name"].astype(str).str.strip() != "Anonymous")].copy()
    if df.empty:
        return df

    df["Role"] = df["Moderator"].apply(
        lambda value: "Moderator" if str(value).upper() == "TRUE" else "Student"
    )

    for column in COLUMNS:
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

    return df[COLUMNS].copy()


def build_lesson_title(
    flow_name: str,
    meeting_dt: datetime,
    teacher_name: str,
    student_count: int,
) -> str:
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


def resize_columns(spreadsheet, sheet_id: int, header_len: int):
    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 220},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": min(header_len, 3),
                },
                "properties": {"pixelSize": 120},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 3,
                    "endIndex": header_len,
                },
                "properties": {"pixelSize": 130},
                "fields": "pixelSize",
            }
        },
    ]
    safe_batch_update(spreadsheet, {"requests": requests})


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
    resize_columns(spreadsheet, worksheet.id, header_len)


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
        spreadsheet_id = os.getenv(env_key, "").strip()
        if not spreadsheet_id:
            missing.append(env_key)
            continue

        print(f"Пробую открыть {product}: {spreadsheet_id}")
        try:
            spreadsheets[product] = gc.open_by_key(spreadsheet_id)
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


def load_payroll_log(payroll_ws) -> pd.DataFrame:
    values = gspread_with_retry(payroll_ws.get_all_values)
    if len(values) <= 1:
        return pd.DataFrame(columns=PAYROLL_LOG_COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    return df


def ensure_data_rows(worksheet, required_rows: int, required_cols: int):
    if worksheet.row_count < required_rows:
        gspread_with_retry(worksheet.add_rows, required_rows - worksheet.row_count)
    if worksheet.col_count < required_cols:
        gspread_with_retry(worksheet.add_cols, required_cols - worksheet.col_count)


def write_lesson_sheet(spreadsheet, flow_name: str, meeting_dt: datetime, teacher_name: str, df: pd.DataFrame):
    sheet_name = build_flow_month_sheet_name(flow_name, meeting_dt)
    worksheet, _ = ensure_worksheet(spreadsheet, sheet_name, rows=2500, cols=max(20, len(COLUMNS)))

    header_values = COLUMNS
    ensure_header(worksheet, header_values)
    apply_sheet_basics(spreadsheet, worksheet, len(header_values))

    next_row = len(gspread_with_retry(worksheet.get_all_values)) + 1
    lesson_title = build_lesson_title(
        flow_name=flow_name,
        meeting_dt=meeting_dt,
        teacher_name=teacher_name,
        student_count=int((df["Role"] == "Student").sum()),
    )
    title_row = [lesson_title] + [""] * (len(header_values) - 1)
    spacer_row = [""] * len(header_values)
    rows_to_append = [spacer_row, title_row] + df[header_values].values.tolist()

    required_rows = next_row + len(rows_to_append) + 10
    ensure_data_rows(worksheet, required_rows, len(header_values))
    gspread_with_retry(worksheet.append_rows, rows_to_append, value_input_option="RAW")
    format_lesson_block(spreadsheet, worksheet, next_row + 1, len(df), len(header_values))
    return sheet_name


def rebuild_payroll_summary(spreadsheet, month_key: str):
    payroll_log_ws = ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)
    df = load_payroll_log(payroll_log_ws)
    if df.empty:
        return

    month_df = df[df["Month Key"] == month_key].copy()
    if month_df.empty:
        return

    month_df["Meeting Date Parsed"] = pd.to_datetime(month_df["Meeting Date"], errors="coerce")

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
        main_lessons = int(flow_counts.loc[flow_counts["Teacher"] == main_teacher, "lesson_count"].sum())
        replacement_rows = flow_counts[flow_counts["Teacher"] != main_teacher]
        replacement_teacher = ", ".join(replacement_rows["Teacher"].tolist()) or "-"
        replacement_lessons = int(replacement_rows["lesson_count"].sum()) if not replacement_rows.empty else 0
        replacement_dates = ", ".join(replacement_rows["dates"].tolist()) if not replacement_rows.empty else "-"
        product = str(flow_df.iloc[0]["Product"])
        total_lessons = int(flow_df["Lesson ID"].count())
        group_rows.append(
            [
                flow,
                product,
                main_teacher,
                str(main_lessons),
                replacement_teacher,
                str(replacement_lessons),
                replacement_dates,
                str(total_lessons),
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
        replacement_lesson_count = int(row["total_lessons"]) - main_lesson_count
        teacher_rows.append(
            [
                teacher,
                str(int(row["total_lessons"])),
                str(main_lesson_count),
                str(replacement_lesson_count),
                row["groups"],
                row["replacement_dates"] if replacement_lesson_count else "-",
            ]
        )

    summary_title = safe_sheet_title(f"Payroll {month_key}")
    summary_ws, _ = ensure_worksheet(spreadsheet, summary_title, rows=500, cols=10)
    gspread_with_retry(summary_ws.clear)

    values = [
        [f"Payroll Summary {month_key}"],
        [],
        ["By group"],
        [
            "Group",
            "Product",
            "Main teacher",
            "Main lessons",
            "Replacement teacher",
            "Replacement lessons",
            "Replacement dates",
            "Total lessons",
        ],
        *group_rows,
        [],
        ["By teacher"],
        [
            "Teacher",
            "Total lessons",
            "Main lessons",
            "Replacement lessons",
            "Groups",
            "Replacement dates",
        ],
        *teacher_rows,
    ]
    gspread_with_retry(summary_ws.update, range_name="A1", values=values)

    max_cols = max(len(row) for row in values if row)
    safe_batch_update(
        spreadsheet,
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": summary_ws.id,
                            "gridProperties": {"frozenRowCount": 4},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": summary_ws.id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": max_cols,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True, "fontSize": 12},
                                "backgroundColor": {"red": 0.90, "green": 0.94, "blue": 0.98},
                            }
                        },
                        "fields": "userEnteredFormat(textFormat,backgroundColor)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": summary_ws.id,
                            "startRowIndex": 3,
                            "endRowIndex": 4,
                            "startColumnIndex": 0,
                            "endColumnIndex": 8,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.16, "green": 0.38, "blue": 0.62},
                                "textFormat": {
                                    "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                    "bold": True,
                                },
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": summary_ws.id,
                            "startRowIndex": 6 + len(group_rows),
                            "endRowIndex": 7 + len(group_rows),
                            "startColumnIndex": 0,
                            "endColumnIndex": 6,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.16, "green": 0.38, "blue": 0.62},
                                "textFormat": {
                                    "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                                    "bold": True,
                                },
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
            ]
        },
    )


# ──────────────────────────────────────────────
# 3. PLAYWRIGHT И ЗАГРУЗКА CSV
# ──────────────────────────────────────────────


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

        rows = page.query_selector_all("tr")
        print(f"BOT: Строк в таблице: {len(rows)}")

        saved_files = []
        skipped_by_date = 0
        for i, row in enumerate(rows):
            report_link = row.query_selector("a:has-text('Report.CSV'), a:has-text('Report.csv')")
            if not report_link:
                continue

            cells = row.query_selector_all("td")
            meeting_name = f"Meeting_{i}"
            if cells:
                name_link = cells[0].query_selector("a")
                meeting_name = name_link.inner_text().strip() if name_link else cells[0].inner_text().strip()
                if not meeting_name:
                    meeting_name = f"Meeting_{i}"

            meeting_date = cells[1].inner_text().strip() if len(cells) > 1 else ""
            try:
                meeting_dt = parse_meeting_datetime(meeting_date)
            except ValueError:
                print(f"BOT: Пропускаю '{meeting_name}' — не удалось разобрать дату.")
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
                filename = f"{safe_name}_{i}.csv"
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
                page.wait_for_timeout(500)
            except Exception as exc:
                print(f"BOT: Ошибка при скачивании '{meeting_name}': {exc}")

        browser.close()
        print(f"BOT: Пропущено по диапазону дат: {skipped_by_date}")
        print(f"BOT: Всего скачано: {len(saved_files)} файлов")
        return saved_files


# ──────────────────────────────────────────────
# 4. ОБРАБОТКА И ЗАПИСЬ
# ──────────────────────────────────────────────


def process_lessons(files: list[dict], spreadsheets: dict):
    impacted_months_by_product: dict[str, set[str]] = {product: set() for product in spreadsheets}

    for product, spreadsheet in spreadsheets.items():
        ensure_meta_sheet(spreadsheet, "_LESSON_INDEX", LESSON_INDEX_COLUMNS)
        ensure_meta_sheet(spreadsheet, "_PAYROLL_LOG", PAYROLL_LOG_COLUMNS)

    existing_ids_by_product = {
        product: get_existing_lesson_ids(ensure_meta_sheet(spreadsheet, "_LESSON_INDEX", LESSON_INDEX_COLUMNS))
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
        if lesson_id in existing_ids_by_product[product]:
            print(f"  Пропускаю '{meeting_name}' — уже загружен ({lesson_id}).")
            continue

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
        sheet_name = write_lesson_sheet(spreadsheet, flow_name, meeting_dt, teacher_name, df)
        student_count = int((df["Role"] == "Student").sum())
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


# ──────────────────────────────────────────────
# 5. ТОЧКА ВХОДА
# ──────────────────────────────────────────────


if __name__ == "__main__":
    print("=== СТАРТ ===")

    try:
        config = load_runtime_config()
        print(
            "Режим:",
            config["mode"],
            "| диапазон:",
            config["start_date"],
            "—",
            config["end_date"],
        )
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
