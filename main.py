

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
                meeting_dt = parse_meeting_datetime(meeting_date)
            except ValueError:
                print(f"BOT: Пропускаю '{meeting_name}' — не удалось разобрать дату.")
                continue
                next_control.click()
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(2000)
            except Exception as exc:
                print(f"BOT: Не удалось открыть следующую страницу: {exc}")
                break

            if not is_in_range(meeting_dt, config):
                skipped_by_date += 1
                continue
            new_rows = page.query_selector_all("tr")
            after_marker = new_rows[-1].inner_text().strip() if new_rows else ""
            if before_marker and before_marker == after_marker and downloads_on_page == 0:
                print("BOT: Похоже, страница не сменилась, прекращаю пагинацию.")
                break

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
            page_number += 1



        browser.close()
        print(f"BOT: Пропущено по диапазону дат: {skipped_by_date}")
