"""Enhance tables in markdown documents"""

from PIL import Image
import io
import os
from openai import OpenAI
import imgkit
import re
from difflib import SequenceMatcher
from pathlib import Path
from collections import Counter

from table_extract import extract_tables_from_pdf, batch_rotate_tables_90deg_pil
from ocr import qwen3vl_extract, qwen3vl_fix

PDF_DIR = os.environ.get('PDF_DIR', './pdf')
MD_DIR  = os.environ.get('MD_DIR',  './md')
DBG_DIR = os.environ.get('DBG_DIR', './dbg')
PDF_TO_PROC_LIST = sorted(os.listdir(PDF_DIR))
# MD_TO_PROC_LIST  = sorted(os.listdir(MD_DIR))
MD_TO_PROC_LIST = sorted([f for f in os.listdir(MD_DIR) if not f.endswith('_enhanced.md')])
DBG_SUBDIRS = [i[:-4] for i in PDF_TO_PROC_LIST]

assert DBG_SUBDIRS == [i[:-3] for i in MD_TO_PROC_LIST], "pdf and md files must be equal"
assert DBG_SUBDIRS, "must be at least one doc to proc"

OCR_BASE_URL = os.environ.get('OCR_BASE_URL')
OCR_API_KEY  = os.environ.get('OCR_API_KEY')
OCR_MODEL    = os.environ.get('OCR_MODEL')
OCR_CLIENT   = OpenAI(base_url=OCR_BASE_URL, api_key=OCR_API_KEY)

MD_BASE_URL = os.environ.get('MD_BASE_URL')
MD_API_KEY  = os.environ.get('MD_API_KEY')
MD_MODEL    = os.environ.get('MD_MODEL')
MD_ENABLE_LLM_TABLE_ENHANCEMENT = os.environ.get("MD_ENABLE_LLM_TABLE_ENHANCEMENT", "0").lower() in {"1", "true", "yes"}
MD_CLIENT   = OpenAI(base_url=MD_BASE_URL, api_key=MD_API_KEY) if MD_ENABLE_LLM_TABLE_ENHANCEMENT else None

FORCE_REPROCESS = os.environ.get("FORCE_REPROCESS", "0").lower() in {"1", "true", "yes"}
APPEND_MODE = os.environ.get("APPEND_MODE", "1").lower() in {"1", "true", "yes"}
MATCH_THRESHOLD = float(os.environ.get("MATCH_THRESHOLD", "0.35"))
MATCH_ORDER_WEIGHT = float(os.environ.get("MATCH_ORDER_WEIGHT", "0.40"))
MATCH_AMBIGUITY_GAP = float(os.environ.get("MATCH_AMBIGUITY_GAP", "0.03"))
MATCH_STRICT_AMBIGUITY = os.environ.get("MATCH_STRICT_AMBIGUITY", "0").lower() in {"1", "true", "yes"}
ORPHAN_SCAN_MAX_CHARS = int(os.environ.get("ORPHAN_SCAN_MAX_CHARS", "2000"))
ORPHAN_MIN_PREFIX_CHARS = int(os.environ.get("ORPHAN_MIN_PREFIX_CHARS", "4"))
ORPHAN_MIN_TOKEN_OVERLAP = float(os.environ.get("ORPHAN_MIN_TOKEN_OVERLAP", "0.85"))
PROCESS_LOG_DIR = os.environ.get("PROCESS_LOG_DIR", "./process_logs")
Path(PROCESS_LOG_DIR).mkdir(parents=True, exist_ok=True)


def log_table_match_issue(doc_name: str,
                          extracted_tables: int,
                          matched_tables: int,
                          existing_tables: int,
                          output_path: str) -> None:
    """Append a process-log record for docs with unmatched extracted tables."""
    log_path = Path(PROCESS_LOG_DIR) / "table_match_issues.log"
    line = (
        f"doc={doc_name} | extracted={extracted_tables} | matched={matched_tables} | "
        f"existing_md_tables={existing_tables} | output={output_path}\n"
    )
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)


def get_existing_dbg_table_count(doc_name_no_ext: str) -> int:
    """Count previously extracted tables by HTML artifacts in DBG_DIR/<doc>/final_htmls."""
    final_html_dir = Path(DBG_DIR) / doc_name_no_ext / "final_htmls"
    if not final_html_dir.exists():
        return 0
    return len(list(final_html_dir.glob("*.html")))

def render_html_table(html):
    img_data = imgkit.from_string(
"""<html><head><style>
body {
margin: 20px;
}
table {
border-collapse: collapse;
}
table, th, td {
border: 1px solid black;
}
</style></head><body>
""" + html + "\n</body></html>", False)
    return Image.open(io.BytesIO(img_data))

def strip_markdown_code_fences(text: str, table_only: bool = False) -> str:
    """
    Remove markdown code fences such as ```html ... ``` around model outputs.
    Handles both full-string fences and fenced fragments inside larger text.

    If table_only=True, additionally keeps only HTML table content
    (from first <table ...> to matching </table>) and removes any extra text
    before/after the table.
    """
    if not text:
        return text

    cleaned = text.strip()

    # Case 1: whole response is a fenced block
    full_block = re.match(r"^```(?:html|markdown)?\s*\n([\s\S]*?)\n```\s*$", cleaned, re.IGNORECASE)
    if full_block:
        return full_block.group(1).strip()

    # Case 2: remove any fenced html/markdown fragments inline
    cleaned = re.sub(
        r"```(?:html|markdown)?\s*\n([\s\S]*?)\n```",
        r"\1",
        cleaned,
        flags=re.IGNORECASE,
    )

    cleaned = cleaned.strip()

    if table_only:
        table_match = re.search(r"<table[^>]*>.*?</table>", cleaned, re.DOTALL | re.IGNORECASE)
        if table_match:
            return table_match.group(0).strip()

    return cleaned

def extract_html_tables_from_markdown(markdown_text: str) -> list[dict]:
    """
    Extract all HTML tables from markdown text.
    Returns list of dicts with: {html, start_pos, end_pos, context_before, context_after}
    """
    tables = []
    pattern = r'<table[^>]*>.*?</table>'
    
    for match in re.finditer(pattern, markdown_text, re.DOTALL | re.IGNORECASE):
        html = match.group(0)
        start = match.start()
        end = match.end()
        
        # Get context (up to 500 chars before and after)
        context_start = max(0, start - 500)
        context_end = min(len(markdown_text), end + 500)
        
        tables.append({
            'html': html,
            'start_pos': start,
            'end_pos': end,
            'context_before': markdown_text[context_start:start],
            'context_after': markdown_text[end:context_end],
            'full_context': markdown_text[context_start:context_end]
        })
    
    return tables

def extract_table_text(html: str) -> str:
    """Extract all text content from HTML table for fuzzy matching."""
    # Remove HTML tags but keep structure
    text = re.sub(r'<[^>]+>', ' ', html)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_normalized_text(text: str) -> str:
    """Normalize text for comparison - lowercase, remove extra spaces."""
    return re.sub(r'\s+', ' ', text.lower()).strip()

def extract_merged_region_text(existing_tables: list[dict], table_indices: list[int], 
                               full_markdown: str, max_following_gap: int = 1500) -> tuple[str, int, int]:
    """
    Extract text from a region containing one or more tables (including surrounding context).
    This captures both HTML tables and text content between/around them.
    
    Also includes text blocks that follow the last table if they're close enough
    (within max_following_gap), as these might be orphaned parts of the original table.
    
    Returns: (merged_text, region_start, region_end)
    """
    if not table_indices:
        return "", 0, 0
    
    # Get start of first table and end of last table
    region_start = max(0, existing_tables[table_indices[0]]['start_pos'] - 500)
    region_end = existing_tables[table_indices[-1]]['end_pos']
    
    # Check if there's a text block following the last table that might be orphaned
    last_table_idx = table_indices[-1]
    if last_table_idx < len(existing_tables) - 1:
        # There are more tables after this group
        next_table_start = existing_tables[last_table_idx + 1]['start_pos']
        gap_to_next = next_table_start - region_end
        
        # If gap is small, it might be orphaned content - include it
        if gap_to_next <= max_following_gap:
            region_end = next_table_start
    else:
        # No more tables after - include all remaining text
        region_end = len(full_markdown)
    
    # Add buffer for context
    region_end = min(region_end + 500, len(full_markdown))
    
    region_text = full_markdown[region_start:region_end]
    
    # Extract both table text and text between tables
    all_text = extract_table_text(region_text)
    
    return all_text, region_start, region_end

def find_text_islands(full_markdown: str, existing_tables: list[dict], min_island_size: int = 50) -> list[dict]:
    """
    Find text "islands" - blocks of actual text content outside <table> tags.
    These might be orphaned parts of split tables from the OCR pipeline.
    
    Returns list of island dicts:
    {
        'text': str,
        'start_pos': int,
        'end_pos': int,
        'after_table_idx': int (index of preceding table, or -1 if before all tables)
    }
    """
    islands = []
    
    if not existing_tables:
        # No tables - all content is islands
        text = full_markdown.strip()
        if len(text) >= min_island_size:
            islands.append({
                'text': text,
                'start_pos': 0,
                'end_pos': len(full_markdown),
                'after_table_idx': -1
            })
        return islands

    # Text before first table
    if existing_tables[0]['start_pos'] > 0:
        leading_text = full_markdown[:existing_tables[0]['start_pos']].strip()
        if leading_text and len(leading_text) >= min_island_size:
            islands.append({
                'text': leading_text,
                'start_pos': 0,
                'end_pos': existing_tables[0]['start_pos'],
                'after_table_idx': -1
            })

    # Text between tables
    for idx in range(len(existing_tables) - 1):
        gap_start = existing_tables[idx]['end_pos']
        gap_end = existing_tables[idx + 1]['start_pos']
        if gap_end <= gap_start:
            continue

        gap_text = full_markdown[gap_start:gap_end].strip()
        if gap_text and len(gap_text) >= min_island_size:
            islands.append({
                'text': gap_text,
                'start_pos': gap_start,
                'end_pos': gap_end,
                'after_table_idx': idx
            })

    # Text after last table
    last_table_end = existing_tables[-1]['end_pos']
    following_text = full_markdown[last_table_end:].strip()

    if following_text and len(following_text) >= min_island_size:
        islands.append({
            'text': following_text,
            'start_pos': last_table_end,
            'end_pos': len(full_markdown),
            'after_table_idx': len(existing_tables) - 1
        })
    
    return islands

def positional_alignment_score(extracted_idx: int, extracted_total: int,
                               table_start_idx: int, tables_total: int) -> float:
    """
    Score how well table index aligns with extraction order (0..1).
    Higher = closer to monotonic expected position.
    """
    if extracted_total <= 1 or tables_total <= 1:
        return 1.0

    ext_pos = extracted_idx / (extracted_total - 1)
    tbl_pos = table_start_idx / (tables_total - 1)
    distance = abs(ext_pos - tbl_pos)

    # Linear decay: distance 0 -> 1.0, distance >= 0.5 -> 0.0
    return max(0.0, 1.0 - min(1.0, distance * 2.0))

def _tokenize_for_overlap(text: str) -> list[str]:
    """Tokenize text for overlap checks (words, numbers, percentages)."""
    return re.findall(r"[\w\.-]+%?", text.lower(), flags=re.UNICODE)

def _multiset_overlap_ratio(candidate_tokens: list[str], reference_tokens: list[str]) -> float:
    """How much of candidate token multiset is covered by reference tokens (0..1)."""
    if not candidate_tokens:
        return 0.0
    c1 = Counter(candidate_tokens)
    c2 = Counter(reference_tokens)
    common = sum((c1 & c2).values())
    return common / max(1, len(candidate_tokens))

def find_orphan_duplicate_tail_end(markdown_text: str,
                                   tail_start: int,
                                   hard_end: int,
                                   replacement_html: str,
                                   max_scan_chars: int = 1200,
                                   min_prefix_chars: int = 40,
                                   min_token_overlap: float = 0.72) -> int:
    """
    Dynamically detect duplicated orphan text right after a replaced table.

    We scan a bounded tail region and find the longest leading prefix whose text
    is strongly covered by tokens present in the replacement table.
    Returns the end position to which replacement region should extend.
    """
    if tail_start >= hard_end:
        return tail_start

    scan_end = min(hard_end, tail_start + max_scan_chars)
    tail_region = markdown_text[tail_start:scan_end]
    if not tail_region.strip():
        return tail_start

    table_text = get_normalized_text(extract_table_text(replacement_html))
    table_tokens = _tokenize_for_overlap(table_text)
    if not table_tokens:
        return tail_start

    best_rel_end = 0
    cumulative = ""
    lines = tail_region.splitlines(keepends=True)

    # Prefer semantic chunks by lines (dynamic-length detection)
    for line in lines:
        cumulative += line
        candidate = cumulative.strip()
        if len(candidate) < min_prefix_chars:
            continue

        candidate_norm = get_normalized_text(candidate)
        cand_tokens = _tokenize_for_overlap(candidate_norm)
        if not cand_tokens:
            continue

        token_overlap = _multiset_overlap_ratio(cand_tokens, table_tokens)
        fuzzy_ratio = SequenceMatcher(None, candidate_norm, table_text).ratio()

        if token_overlap >= min_token_overlap and fuzzy_ratio >= 0.30:
            best_rel_end = len(cumulative)

    # Fallback for dense single-line tails: check fixed-size prefixes
    if best_rel_end == 0 and "\n" not in tail_region:
        for step in range(min_prefix_chars, len(tail_region) + 1, 20):
            candidate = tail_region[:step].strip()
            if len(candidate) < min_prefix_chars:
                continue
            candidate_norm = get_normalized_text(candidate)
            cand_tokens = _tokenize_for_overlap(candidate_norm)
            if not cand_tokens:
                continue
            token_overlap = _multiset_overlap_ratio(cand_tokens, table_tokens)
            fuzzy_ratio = SequenceMatcher(None, candidate_norm, table_text).ratio()
            if token_overlap >= min_token_overlap and fuzzy_ratio >= 0.30:
                best_rel_end = step

    return tail_start + best_rel_end if best_rel_end > 0 else tail_start

def fuzzy_match_tables_improved(extracted_code: list[str], existing_tables: list[dict], 
                                full_markdown: str, threshold: float = 0.35, max_merge: int = 10,
                                order_weight: float = 0.20,
                                ambiguity_gap: float = 0.03,
                                strict_ambiguity: bool = False) -> list[dict]:
    """
    Improved fuzzy matching that tries merging successive tables and matching text islands.
    
    For each extracted table, tries matching against:
    - Individual existing tables
    - Successive pairs, triples, etc. (up to max_merge tables)
    - Text islands (orphaned content outside <table> tags)
    
    This detects split tables without forcing unrelated sequential tables together,
    and also captures orphaned text fragments that are parts of original tables.
    
    Returns: list of match dicts containing:
    {
        'extracted_html': str,
        'table_indices': list[int],  # indices of matched existing tables
        'confidence': float,
        'match_info': dict,
        'text_islands_included': bool  # whether text island(s) were part of this match
    }
    """
    matches = []
    used_tables = set()  # Track which existing tables have been matched
    used_text_islands = set()  # Track which text islands have been matched
    last_matched_table_idx = -1  # Enforce monotonic matching by document order
    
    # Pre-compute text islands
    text_islands = find_text_islands(full_markdown, existing_tables, min_island_size=50)
    
    for ext_idx, ext_code in enumerate(extracted_code):
        ext_text = get_normalized_text(extract_table_text(ext_code))
        
        best_match = None
        best_score = 0
        second_best_score = 0
        
        # Try all possible mergers of successive tables, but only in forward order
        # to reduce cross-matching between similar numeric tables.
        for start_idx in range(last_matched_table_idx + 1, len(existing_tables)):
            if start_idx in used_tables:
                continue
            
            for merge_count in range(1, max_merge + 1):
                end_idx = min(start_idx + merge_count, len(existing_tables))
                table_indices = list(range(start_idx, end_idx))
                
                # Skip if any table in this range is already used
                if any(idx in used_tables for idx in table_indices):
                    continue
                
                # Extract text from merged region (includes following text if nearby)
                region_text, _, _ = extract_merged_region_text(existing_tables, table_indices, full_markdown)
                region_text_norm = get_normalized_text(region_text)
                
                # Calculate match scores
                region_ratio = SequenceMatcher(None, ext_text, region_text_norm).ratio()
                
                # Also check individual table matches
                individual_ratios = []
                for table_idx in table_indices:
                    table_text = get_normalized_text(extract_table_text(existing_tables[table_idx]['html']))
                    table_ratio = SequenceMatcher(None, ext_text, table_text).ratio()
                    individual_ratios.append(table_ratio)
                
                max_individual_ratio = max(individual_ratios) if individual_ratios else 0
                
                # Penalty for merging: merged matches should be slightly penalized
                # so single table matches are preferred unless they're significantly better
                merge_penalty = 0.95 ** (len(table_indices) - 1)

                # Positional prior: extracted order should roughly align with markdown order
                order_score = positional_alignment_score(
                    extracted_idx=ext_idx,
                    extracted_total=len(extracted_code),
                    table_start_idx=start_idx,
                    tables_total=len(existing_tables),
                )
                
                # Weighted score
                content_score = (region_ratio * 0.4 + max_individual_ratio * 0.6)
                safe_order_weight = min(max(order_weight, 0.0), 1.0)
                content_weight = 1.0 - safe_order_weight
                combined_score = (content_score * content_weight + order_score * safe_order_weight) * merge_penalty
                
                if combined_score > best_score:
                    second_best_score = best_score
                    best_score = combined_score
                    best_match = {
                        'extracted_html': ext_code,
                        'table_indices': table_indices,
                        'confidence': combined_score,
                        'text_islands_included': False,
                        'match_info': {
                            'region_ratio': region_ratio,
                            'best_individual_ratio': max_individual_ratio,
                            'order_score': order_score,
                            'second_best_score': second_best_score,
                            'merge_count': len(table_indices),
                            'combined_score': combined_score
                        }
                    }
                elif combined_score > second_best_score:
                    second_best_score = combined_score
        
        # Also try matching against text islands (orphaned content)
        for island_idx, island in enumerate(text_islands):
            if island_idx in used_text_islands:
                continue
            
            island_text_norm = get_normalized_text(island['text'])
            island_ratio = SequenceMatcher(None, ext_text, island_text_norm).ratio()
            
            # Apply penalty for text-only matches (less reliable than table structure)
            text_only_penalty = 0.85
            island_score = island_ratio * text_only_penalty
            
            if island_score > best_score:
                second_best_score = best_score
                best_score = island_score
                # For text islands, we reference the preceding table index
                preceding_table_idx = island['after_table_idx']
                best_match = {
                    'extracted_html': ext_code,
                    'table_indices': [preceding_table_idx] if preceding_table_idx >= 0 else [],
                    'text_island_idx': island_idx,
                    'confidence': island_score,
                    'text_islands_included': True,
                    'match_info': {
                        'island_ratio': island_ratio,
                        'text_only_score': island_score,
                        'order_score': 0.0,
                        'second_best_score': second_best_score,
                        'merge_count': 0
                    }
                }
            elif island_score > second_best_score:
                second_best_score = island_score
        
        # Ambiguity guard: if top-2 candidates are too close, skip weak/ambiguous match
        ambiguous = second_best_score > 0 and (best_score - second_best_score) < ambiguity_gap

        # By default we DO NOT drop ambiguous matches, because strict ambiguity rejection
        # can lose many valid matches when several tables are structurally very similar.
        allow_ambiguous = (not strict_ambiguity) or (best_score >= threshold + 0.08)

        # Add best match if confidence exceeds threshold and ambiguity policy allows it
        if best_match and best_score >= threshold and allow_ambiguous:
            best_match['match_info']['ambiguous'] = ambiguous
            matches.append(best_match)
            # Mark matched tables as used
            for idx in best_match.get('table_indices', []):
                if idx >= 0:
                    used_tables.add(idx)
                    last_matched_table_idx = max(last_matched_table_idx, idx)
            # Mark text island as used if this match includes it
            if best_match.get('text_islands_included'):
                used_text_islands.add(best_match.get('text_island_idx', -1))
    
    return matches

def enhance_markdown_context_with_aimd(original_context: str, better_html: str, 
                                       md_client, md_model: str) -> str:
    """
    Use MD_CLIENT to carefully enhance markdown context around a table.
    This rewrites the surrounding markdown to best match the improved table.
    """
    prompt = f"""You are a technical document editor. Your task is to enhance the markdown text around an HTML table.

The context contains markdown text with an embedded HTML table. The table has been improved with better extraction and formatting.

ORIGINAL MARKDOWN CONTEXT:
```markdown
{original_context}
```

IMPROVED HTML TABLE:
```html
{better_html}
```

TASK:
1. Keep the overall structure and meaning of the markdown
2. Update any text references or descriptions to match the improved table
3. Fix any obvious OCR errors in the surrounding text
4. Ensure the table integrates well with surrounding content
5. Return ONLY the updated markdown context with the improved table embedded

RULES:
- Preserve the exact HTML table code, except for fixing obvious contradictions with the context (usually, the number and title of the table are mistakenly entered inside the table)
- Return only the markdown text, no explanations
- Keep the same formatting style as input"""

    messages = [
        {
            "role": "system",
            "content": "You are an expert technical document editor. Your task is to integrate improved HTML tables into markdown documents while preserving document structure and fixing surrounding text."
        },
        {
            "role": "user",
            "content": prompt
        }
    ]
    
    response = md_client.chat.completions.create(
        model=md_model,
        messages=messages,
        temperature=0.3,
    )
    
    response_text = response.choices[0].message.content or ""
    return strip_markdown_code_fences(response_text)

def create_debug_subdirs(base_debug_dir: str, doc_name: str) -> str:
    """Create debug subdirectory structure for a document."""
    doc_subdir = Path(base_debug_dir) / doc_name
    doc_subdir.mkdir(parents=True, exist_ok=True)
    
    (doc_subdir / "intermediate_renders").mkdir(exist_ok=True)
    (doc_subdir / "final_renders").mkdir(exist_ok=True)
    (doc_subdir / "intermediate_htmls").mkdir(exist_ok=True)
    (doc_subdir / "final_htmls").mkdir(exist_ok=True)
    
    return str(doc_subdir)

def get_table_artifact_paths(debug_subdir: str, table_idx: int) -> dict[str, Path]:
    """Get all artifact paths for a table index."""
    debug_path = Path(debug_subdir)
    return {
        'intermediate_render': debug_path / "intermediate_renders" / f"table_{table_idx:03d}.png",
        'final_render': debug_path / "final_renders" / f"table_{table_idx:03d}.png",
        'intermediate_html': debug_path / "intermediate_htmls" / f"table_{table_idx:03d}.html",
        'final_html': debug_path / "final_htmls" / f"table_{table_idx:03d}.html",
    }

def read_text_if_exists(path: Path) -> str | None:
    """Read UTF-8 text file if it exists and is non-empty."""
    if not path.exists():
        return None
    try:
        content = path.read_text(encoding='utf-8').strip()
        return content if content else None
    except Exception:
        return None

def load_image_if_exists(path: Path):
    """Load image from path if exists, returning a detached PIL copy."""
    if not path.exists():
        return None
    try:
        with Image.open(path) as img:
            return img.copy()
    except Exception:
        return None

def save_debug_artifacts(debug_subdir: str, table_idx: int, 
                         intermediate_render, final_render,
                         intermediate_html: str, final_html: str):
    """Save intermediate and final renders and HTML to debug directory."""
    debug_path = Path(debug_subdir)
    
    intermediate_render.save(debug_path / f"intermediate_renders" / f"table_{table_idx:03d}.png")
    final_render.save(debug_path / f"final_renders" / f"table_{table_idx:03d}.png")
    
    with open(debug_path / f"intermediate_htmls" / f"table_{table_idx:03d}.html", 'w', encoding='utf-8') as f:
        f.write(intermediate_html)
    
    with open(debug_path / f"final_htmls" / f"table_{table_idx:03d}.html", 'w', encoding='utf-8') as f:
        f.write(final_html)

if __name__ == "__main__":
    for pdf_idx, pdf_name in enumerate(PDF_TO_PROC_LIST):
        doc_name_no_ext = pdf_name[:-4]
        md_name = doc_name_no_ext + ".md"
        md_path = MD_DIR + "/" + md_name
        pdf_path = PDF_DIR + "/" + pdf_name
        output_path = md_path.replace('.md', '_enhanced.md')
        
        print(f"\n{'='*70}")
        print(f"[{pdf_idx+1}/{len(PDF_TO_PROC_LIST)}] Processing: {pdf_name}")
        print(f"{'='*70}")

        output_exists = os.path.exists(output_path)
        prev_extracted_count = get_existing_dbg_table_count(doc_name_no_ext)

        # If enhanced output already exists, do not re-open/extract this document
        # unless FORCE_REPROCESS is explicitly enabled.
        if output_exists and not FORCE_REPROCESS:
            print(f"Skipping document (already enhanced): {output_path}")
            continue

        # Extract current table count from PDF for non-enhanced (or forced) docs.
        try:
            tables = extract_tables_from_pdf(
                pdf_path,
                dpi=300,
                min_table_area=1000,
                save_debug_images=False
            )
            print(f"Extracted {len(tables)} individual tables from PDF")
        except Exception as e:
            print(f"Error extracting tables from PDF: {e}")
            continue

        current_extracted_count = len(tables)
        
        append_processing = (
            APPEND_MODE
            and not output_exists
            and not FORCE_REPROCESS
            and prev_extracted_count > 0
            and current_extracted_count > prev_extracted_count
        )

        # For non-enhanced docs, base markdown source is the original markdown.
        markdown_source_path = md_path
        markdown_text = ""
        with open(markdown_source_path, 'r', encoding="utf-8") as file:
            markdown_text = file.read()

        if append_processing:
            print(
                f"Append mode: reusing existing debug artifacts for first {prev_extracted_count} tables "
                f"and continuing OCR from table {prev_extracted_count + 1} to {current_extracted_count}."
            )
        
        # Extract existing tables from markdown
        existing_tables = extract_html_tables_from_markdown(markdown_text)
        print(f"Found {len(existing_tables)} existing HTML tables in markdown")

        match_tables = existing_tables
        match_index_offset = 0
        
        tables_images = batch_rotate_tables_90deg_pil(
            tables,
            use_majority_voting=False,
            save_images=False,
            debug=False
        )
        print(f"Rotated and processed {len(tables_images)} table images")
        
        # Create debug structure
        debug_subdir = create_debug_subdirs(DBG_DIR, doc_name_no_ext)
        print(f"Created debug directory: {debug_subdir}")
        
        # OCR and fix tables
        intermediate_render = []
        final_render = []
        intermediate_code = []
        final_code = []
        
        print("Processing tables with OCR...")
        for table_idx, table_image in enumerate(tables_images, 1):
            print(f"  Table {table_idx}/{len(tables_images)}...", end=" ")

            artifact_paths = get_table_artifact_paths(debug_subdir, table_idx)
            cached_intermediate_html = read_text_if_exists(artifact_paths['intermediate_html'])
            cached_final_html = read_text_if_exists(artifact_paths['final_html'])

            # Resume mode: if final HTML exists, reuse table result and skip OCR/fix calls
            if cached_final_html:
                reused_intermediate_html = cached_intermediate_html or cached_final_html
                intermediate_code.append(reused_intermediate_html)
                final_code.append(cached_final_html)

                img = load_image_if_exists(artifact_paths['intermediate_render'])
                if img is None:
                    img = render_html_table(reused_intermediate_html)
                    img.save(artifact_paths['intermediate_render'])
                intermediate_render.append(img)

                img_fixed = load_image_if_exists(artifact_paths['final_render'])
                if img_fixed is None:
                    img_fixed = render_html_table(cached_final_html)
                    img_fixed.save(artifact_paths['final_render'])
                final_render.append(img_fixed)

                print("resumed (final html)")
                continue

            # Partial resume: intermediate HTML exists, run only fix step
            if cached_intermediate_html:
                table_code = strip_markdown_code_fences(cached_intermediate_html, table_only=True)
                intermediate_code.append(table_code)

                img = load_image_if_exists(artifact_paths['intermediate_render'])
                if img is None:
                    img = render_html_table(table_code)
                intermediate_render.append(img)

                table_code_fixed = qwen3vl_fix(table_image, img, table_code, OCR_CLIENT, OCR_MODEL)
                table_code_fixed = strip_markdown_code_fences(table_code_fixed, table_only=True)
                final_code.append(table_code_fixed)

                img_fixed = render_html_table(table_code_fixed)
                final_render.append(img_fixed)

                save_debug_artifacts(
                    debug_subdir, table_idx,
                    img, img_fixed,
                    table_code, table_code_fixed
                )

                print("resumed (from intermediate html)")
                continue

            # Fresh processing
            table_code = qwen3vl_extract(table_image, OCR_CLIENT, OCR_MODEL)
            table_code = strip_markdown_code_fences(table_code, table_only=True)
            intermediate_code.append(table_code)

            img = render_html_table(table_code)
            intermediate_render.append(img)

            table_code_fixed = qwen3vl_fix(table_image, img, table_code, OCR_CLIENT, OCR_MODEL)
            table_code_fixed = strip_markdown_code_fences(table_code_fixed, table_only=True)
            final_code.append(table_code_fixed)

            img_fixed = render_html_table(table_code_fixed)
            final_render.append(img_fixed)

            save_debug_artifacts(
                debug_subdir, table_idx,
                img, img_fixed,
                table_code, table_code_fixed
            )

            print("done")
        
        # Fuzzy match and update markdown
        print(f"\nMatching {len(final_code)} extracted tables with {len(match_tables)} existing...")
        effective_order_weight = 0.0 if append_processing else MATCH_ORDER_WEIGHT
        matched_updates = fuzzy_match_tables_improved(
            final_code,
            match_tables,
            markdown_text,
            threshold=MATCH_THRESHOLD,
            order_weight=effective_order_weight,
            ambiguity_gap=MATCH_AMBIGUITY_GAP,
            strict_ambiguity=MATCH_STRICT_AMBIGUITY,
        )

        # Remap local indices from sliced match_tables back to full existing_tables indices.
        if match_index_offset:
            for match in matched_updates:
                if match.get('table_indices'):
                    match['table_indices'] = [idx + match_index_offset for idx in match['table_indices']]
        print(f"Found {len(matched_updates)} matches")

        matched_table_replacements = sum(
            1 for m in matched_updates if not m.get('text_islands_included')
        )
        if len(final_code) > matched_table_replacements:
            log_table_match_issue(
                doc_name=pdf_name,
                extracted_tables=len(final_code),
                matched_tables=matched_table_replacements,
                existing_tables=len(match_tables),
                output_path=output_path,
            )
            print(
                f"⚠ Logged mismatch to {PROCESS_LOG_DIR}: "
                f"extracted={len(final_code)} > matched={matched_table_replacements}"
            )
        
        if matched_updates:
            print("\nMatch details:")
            for match_idx, match in enumerate(matched_updates, 1):
                table_indices = match['table_indices']
                num_merged = len(table_indices)
                conf = match['confidence']
                
                if match.get('text_islands_included'):
                    print(f"  [{match_idx}] Matched text island (confidence: {conf:.2%})")
                elif num_merged > 1:
                    region_ratio = match['match_info']['region_ratio']
                    indiv_ratio = match['match_info']['best_individual_ratio']
                    print(f"  [{match_idx}] Merged {num_merged} tables (confidence: {conf:.2%}, "
                          f"region: {region_ratio:.2%}, best_table: {indiv_ratio:.2%})")
                else:
                    region_ratio = match['match_info']['region_ratio']
                    indiv_ratio = match['match_info']['best_individual_ratio']
                    print(f"  [{match_idx}] Matched 1 table (confidence: {conf:.2%})")
        
        # Apply updates to markdown
        enhanced_markdown = markdown_text
        
        # Sort matches by position (reverse order to preserve positions when editing)
        sorted_matches = sorted(
            matched_updates, 
            key=lambda m: existing_tables[m['table_indices'][0]]['start_pos'] 
                          if m['table_indices'] and m['table_indices'][0] >= 0 
                          else len(markdown_text),
            reverse=True
        )
        
        for match in sorted_matches:
            # Skip text-island-only matches - they're for informational purposes
            # and don't have a clear position to update
            if match.get('text_islands_included'):
                print(f"  Skipping text-island match (info-only)")
                continue
            
            table_indices = match['table_indices']
            extracted_html = match['extracted_html']
            extracted_html = strip_markdown_code_fences(extracted_html)
            conf = match['confidence']
            
            # Get the positions of first and last table in this match
            first_table = existing_tables[table_indices[0]]
            last_table = existing_tables[table_indices[-1]]
            
            # Include broader context for AI enhancement
            region_start = max(0, first_table['start_pos'] - 100)
            region_end = min(len(enhanced_markdown), last_table['end_pos'] + 100)
            region_to_enhance = enhanced_markdown[region_start:region_end]
            # print("\n\n!!! ENHANCING:\n" + region_to_enhance + "\n\n")
            
            num_merged = len(table_indices)
            table_desc = f"{num_merged} merged tables" if num_merged > 1 else "1 table"
            print(f"  Enhancing {table_desc} (confidence: {conf:.2%})...", end=" ")
            
            # LLM context enhancement can be toggled off. By default it is disabled,
            # and we directly replace matched markdown table region with OCR-improved HTML.
            if MD_ENABLE_LLM_TABLE_ENHANCEMENT and MD_CLIENT and MD_MODEL:
                try:
                    enhanced_context = enhance_markdown_context_with_aimd(
                        region_to_enhance, extracted_html,
                        MD_CLIENT, MD_MODEL
                    )

                    # Extract the table(s) from enhanced context
                    enhanced_tables = extract_html_tables_from_markdown(enhanced_context)
                    replacement_html = extracted_html if enhanced_tables else extracted_html
                except Exception as e:
                    print(f"llm-enhance error: {e}; fallback to direct replacement", end=" ")
                    replacement_html = extracted_html
            else:
                replacement_html = extracted_html

            actual_start = first_table['start_pos']
            actual_end = last_table['end_pos']

            # If orphaned text after table is actually duplicated inside
            # replacement_html, extend removal dynamically to include it.
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

            # Replace the entire region
            enhanced_markdown = enhanced_markdown[:actual_start] + replacement_html + enhanced_markdown[actual_end:]
            print("done")
        
        # Save enhanced markdown
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(enhanced_markdown)
        print(f"\n✓ Enhanced markdown saved to: {output_path}")
        
        # Save final markdown in place (optional - uncomment to overwrite original)
        # with open(md_path, 'w', encoding='utf-8') as f:
        #     f.write(enhanced_markdown)
        # print(f"✓ Original markdown updated")

    print("\n" + "="*70)
    print("All documents processed successfully!")
    print("="*70)
