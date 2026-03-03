"""Parallel table enhancement pipeline based on tables.py (document-level threading)."""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from tables import (
    PDF_TO_PROC_LIST,
    MD_DIR,
    DBG_DIR,
    FORCE_REPROCESS,
    MATCH_THRESHOLD,
    MATCH_ORDER_WEIGHT,
    MATCH_AMBIGUITY_GAP,
    MATCH_STRICT_AMBIGUITY,
    ORPHAN_SCAN_MAX_CHARS,
    ORPHAN_MIN_PREFIX_CHARS,
    ORPHAN_MIN_TOKEN_OVERLAP,
    OCR_CLIENT,
    OCR_MODEL,
    MD_ENABLE_LLM_TABLE_ENHANCEMENT,
    MD_CLIENT,
    MD_MODEL,
    PROCESS_LOG_DIR,
    get_existing_dbg_table_count,
    extract_tables_from_pdf,
    batch_rotate_tables_90deg_pil,
    extract_html_tables_from_markdown,
    create_debug_subdirs,
    get_table_artifact_paths,
    read_text_if_exists,
    load_image_if_exists,
    render_html_table,
    qwen3vl_extract,
    qwen3vl_fix,
    strip_markdown_code_fences,
    save_debug_artifacts,
    fuzzy_match_tables_improved,
    enhance_markdown_context_with_aimd,
    find_orphan_duplicate_tail_end,
)


# How many documents are processed in parallel.
MAX_PARALLEL_DOCS = int(os.environ.get("MAX_PARALLEL_DOCS", "4"))


print_lock = threading.Lock()


def log_table_match_issue(doc_name: str,
                          extracted_tables: int,
                          matched_tables: int,
                          existing_tables: int,
                          output_path: str) -> None:
    """Append a process-log record for docs with unmatched extracted tables."""
    os.makedirs(PROCESS_LOG_DIR, exist_ok=True)
    log_path = os.path.join(PROCESS_LOG_DIR, "table_match_issues.log")
    line = (
        f"doc={doc_name} | extracted={extracted_tables} | matched={matched_tables} | "
        f"existing_md_tables={existing_tables} | output={output_path}\n"
    )
    # Protect append in multithreaded mode.
    with print_lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)


def log(message: str) -> None:
    with print_lock:
        print(message, flush=True)


def process_one_document(pdf_idx: int, pdf_name: str, total_docs: int, stop_event: threading.Event) -> tuple[str, str]:
    """Process a single document end-to-end. Returns (pdf_name, status)."""
    if stop_event.is_set():
        return pdf_name, "stopped"

    doc_name_no_ext = pdf_name[:-4]
    md_name = doc_name_no_ext + ".md"
    md_path = MD_DIR + "/" + md_name
    pdf_path = os.path.join(os.environ.get("PDF_DIR", "./pdf"), pdf_name)
    output_path = md_path.replace('.md', '_enhanced.md')

    log(f"\n{'='*70}\n[{pdf_idx+1}/{total_docs}] Processing: {pdf_name}\n{'='*70}")

    output_exists = os.path.exists(output_path)

    # Always re-check extracted table count from PDF.
    # For already-enhanced docs we only reprocess when count increased.
    try:
        tables = extract_tables_from_pdf(
            pdf_path,
            dpi=300,
            min_table_area=1000,
            save_debug_images=False,
        )
        log(f"Extracted {len(tables)} individual tables from PDF for {pdf_name}")
    except Exception as e:
        log(f"Error extracting tables from PDF {pdf_name}: {e}")
        return pdf_name, "error"

    current_extracted_count = len(tables)

    with state_lock:
        prev_extracted_count = get_existing_dbg_table_count(doc_name_no_ext)

        if output_exists and not FORCE_REPROCESS:
            if current_extracted_count <= prev_extracted_count:
                log(
                    f"Skipping document (already enhanced): {output_path}. "
                    f"Extracted tables did not increase "
                    f"({current_extracted_count} <= {prev_extracted_count})."
                )
                return pdf_name, "skipped"

            log(
                f"Reprocessing document because extracted table count increased "
                f"({prev_extracted_count} -> {current_extracted_count})."
            )

    try:
        with open(md_path, 'r', encoding="utf-8") as file:
            markdown_text = file.read()
    except Exception as e:
        log(f"Error reading markdown {md_path}: {e}")
        return pdf_name, "error"

    if stop_event.is_set():
        return pdf_name, "stopped"

    existing_tables = extract_html_tables_from_markdown(markdown_text)
    log(f"Found {len(existing_tables)} existing HTML tables in markdown for {pdf_name}")

    if stop_event.is_set():
        return pdf_name, "stopped"

    tables_images = batch_rotate_tables_90deg_pil(
        tables,
        use_majority_voting=False,
        save_images=False,
        debug=False,
    )
    log(f"Rotated and processed {len(tables_images)} table images for {pdf_name}")

    debug_subdir = create_debug_subdirs(DBG_DIR, doc_name_no_ext)
    log(f"Created debug directory: {debug_subdir}")

    intermediate_code = []
    final_code = []

    log(f"Processing tables with OCR for {pdf_name}...")
    for table_idx, table_image in enumerate(tables_images, 1):
        if stop_event.is_set():
            return pdf_name, "stopped"

        log(f"  [{pdf_name}] Table {table_idx}/{len(tables_images)}...")

        artifact_paths = get_table_artifact_paths(debug_subdir, table_idx)
        cached_intermediate_html = read_text_if_exists(artifact_paths['intermediate_html'])
        cached_final_html = read_text_if_exists(artifact_paths['final_html'])

        if cached_final_html:
            reused_intermediate_html = cached_intermediate_html or cached_final_html
            intermediate_code.append(reused_intermediate_html)
            final_code.append(cached_final_html)

            img = load_image_if_exists(artifact_paths['intermediate_render'])
            if img is None:
                img = render_html_table(reused_intermediate_html)
                img.save(artifact_paths['intermediate_render'])

            img_fixed = load_image_if_exists(artifact_paths['final_render'])
            if img_fixed is None:
                img_fixed = render_html_table(cached_final_html)
                img_fixed.save(artifact_paths['final_render'])

            continue

        if cached_intermediate_html:
            table_code = strip_markdown_code_fences(cached_intermediate_html, table_only=True)
            intermediate_code.append(table_code)

            img = load_image_if_exists(artifact_paths['intermediate_render'])
            if img is None:
                img = render_html_table(table_code)

            table_code_fixed = qwen3vl_fix(table_image, img, table_code, OCR_CLIENT, OCR_MODEL)
            table_code_fixed = strip_markdown_code_fences(table_code_fixed, table_only=True)
            final_code.append(table_code_fixed)

            img_fixed = render_html_table(table_code_fixed)
            save_debug_artifacts(debug_subdir, table_idx, img, img_fixed, table_code, table_code_fixed)
            continue

        table_code = qwen3vl_extract(table_image, OCR_CLIENT, OCR_MODEL)
        table_code = strip_markdown_code_fences(table_code, table_only=True)
        intermediate_code.append(table_code)

        img = render_html_table(table_code)

        table_code_fixed = qwen3vl_fix(table_image, img, table_code, OCR_CLIENT, OCR_MODEL)
        table_code_fixed = strip_markdown_code_fences(table_code_fixed, table_only=True)
        final_code.append(table_code_fixed)

        img_fixed = render_html_table(table_code_fixed)
        save_debug_artifacts(debug_subdir, table_idx, img, img_fixed, table_code, table_code_fixed)

    if stop_event.is_set():
        return pdf_name, "stopped"

    matched_updates = fuzzy_match_tables_improved(
        final_code,
        existing_tables,
        markdown_text,
        threshold=MATCH_THRESHOLD,
        order_weight=MATCH_ORDER_WEIGHT,
        ambiguity_gap=MATCH_AMBIGUITY_GAP,
        strict_ambiguity=MATCH_STRICT_AMBIGUITY,
    )
    log(f"[{pdf_name}] Found {len(matched_updates)} matches")

    matched_table_replacements = sum(
        1 for m in matched_updates if not m.get('text_islands_included')
    )
    if len(final_code) > matched_table_replacements:
        log_table_match_issue(
            doc_name=pdf_name,
            extracted_tables=len(final_code),
            matched_tables=matched_table_replacements,
            existing_tables=len(existing_tables),
            output_path=output_path,
        )
        log(
            f"⚠ [{pdf_name}] Logged mismatch to {PROCESS_LOG_DIR}: "
            f"extracted={len(final_code)} > matched={matched_table_replacements}"
        )

    enhanced_markdown = markdown_text
    sorted_matches = sorted(
        matched_updates,
        key=lambda m: existing_tables[m['table_indices'][0]]['start_pos']
        if m['table_indices'] and m['table_indices'][0] >= 0
        else len(markdown_text),
        reverse=True,
    )

    for match in sorted_matches:
        if stop_event.is_set():
            return pdf_name, "stopped"

        if match.get('text_islands_included'):
            continue

        table_indices = match['table_indices']
        extracted_html = strip_markdown_code_fences(match['extracted_html'])

        first_table = existing_tables[table_indices[0]]
        last_table = existing_tables[table_indices[-1]]

        region_start = max(0, first_table['start_pos'] - 100)
        region_end = min(len(enhanced_markdown), last_table['end_pos'] + 100)
        region_to_enhance = enhanced_markdown[region_start:region_end]

        if MD_ENABLE_LLM_TABLE_ENHANCEMENT and MD_CLIENT and MD_MODEL:
            try:
                _ = enhance_markdown_context_with_aimd(region_to_enhance, extracted_html, MD_CLIENT, MD_MODEL)
                replacement_html = extracted_html
            except Exception as e:
                log(f"[{pdf_name}] llm-enhance error: {e}; fallback to direct replacement")
                replacement_html = extracted_html
        else:
            replacement_html = extracted_html

        actual_start = first_table['start_pos']
        actual_end = last_table['end_pos']
        next_table_start = (
            existing_tables[table_indices[-1] + 1]['start_pos']
            if table_indices[-1] + 1 < len(existing_tables)
            else len(enhanced_markdown)
        )
        dedup_end = find_orphan_duplicate_tail_end(
            enhanced_markdown,
            tail_start=actual_end,
            hard_end=next_table_start,
            replacement_html=replacement_html,
            max_scan_chars=ORPHAN_SCAN_MAX_CHARS,
            min_prefix_chars=ORPHAN_MIN_PREFIX_CHARS,
            min_token_overlap=ORPHAN_MIN_TOKEN_OVERLAP,
        )
        actual_end = max(actual_end, dedup_end)

        enhanced_markdown = enhanced_markdown[:actual_start] + replacement_html + enhanced_markdown[actual_end:]

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(enhanced_markdown)
        log(f"✓ [{pdf_name}] Enhanced markdown saved to: {output_path}")
    except Exception as e:
        log(f"Error writing output for {pdf_name}: {e}")
        return pdf_name, "error"

    return pdf_name, "done"


def main() -> None:
    total_docs = len(PDF_TO_PROC_LIST)
    if total_docs == 0:
        log("No documents to process.")
        return

    stop_event = threading.Event()
    global state_lock
    state_lock = threading.Lock()

    log("=" * 70)
    log(f"Parallel processing started: {total_docs} docs, MAX_PARALLEL_DOCS={MAX_PARALLEL_DOCS}")
    log("Press Ctrl+C to stop all threads immediately.")
    log("=" * 70)

    done = 0
    skipped = 0
    errors = 0
    failed_with_error = []

    try:
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOCS) as executor:
            futures = {
                executor.submit(process_one_document, idx, pdf_name, total_docs, stop_event): pdf_name
                for idx, pdf_name in enumerate(PDF_TO_PROC_LIST)
            }

            for future in as_completed(futures):
                pdf_name = futures[future]
                try:
                    _, status = future.result()
                except Exception as e:
                    log(f"[{pdf_name}] Unhandled worker error: {e}")
                    status = "error"

                if status == "done":
                    done += 1
                elif status == "skipped":
                    skipped += 1
                elif status == "error":
                    errors += 1
                    failed_with_error.append(pdf_name)
    except KeyboardInterrupt:
        stop_event.set()
        log("\nKeyboardInterrupt received. Stopping all worker threads...")
        # Immediate process exit ensures all threads are stopped with interruption.
        os._exit(130)

    log("\n" + "=" * 70)
    log(f"Completed. done={done}, skipped={skipped}, errors={errors}")
    if failed_with_error:
        log(f"Docs failed with error: {',\n'.join(failed_with_error)}")
    log("=" * 70)


if __name__ == "__main__":
    main()
