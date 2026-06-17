# ============================================================
# ingestion/player_type_detector.py
# ============================================================
# PURPOSE:
#   Detects whether a CSV file contains goalkeeper or outfield
#   player data, by inspecting the DataFrame columns and values.
#
# WHY IT EXISTS:
#   Goalkeepers and outfield players live in the SAME folders.
#   There are no separate GK directories. The only way to tell
#   them apart is to look at the data itself.
#
# HOW IT CONNECTS TO THE PIPELINE:
#   initial_load.py imports detect_player_type() and calls it
#   for every CSV it reads, then stores the result in the
#   _PLAYER_TYPE metadata column before loading to Snowflake.
#
# DETECTION STRATEGY (in priority order):
#   1. Check player_position column — if it says GK or Goalkeeper
#   2. Check for goalkeeper-specific column names (saves, etc.)
#   3. Fallback to 'outfield' and log a warning
# ============================================================

import logging
import pandas as pd

logger = logging.getLogger(__name__)

# ── Goalkeeper-specific column names ───────────────────────────────────
# If ANY of these columns exist in the CSV headers, the file is a GK file.
# This set uses lowercase for comparison (we normalise column names below).
GK_COLUMNS = {
    'saves',
    'goals_conceded',
    'clean_sheets',
    'save_percentage',
    'save_pct',
    'penalties_faced',
    'penalties_saved',
    'goals_against',
    'shots_on_target_against',
}

# Keywords that identify a goalkeeper in the player_position column
GK_POSITION_KEYWORDS = ['gk', 'goalkeeper', 'goalie', 'portero', 'gardien', 'torwart']


def detect_player_type(df: pd.DataFrame, source_file: str = '') -> str:
    """
    Detect whether a DataFrame contains goalkeeper or outfield player data.

    Args:
        df:           The loaded CSV as a Pandas DataFrame.
        source_file:  The file path — used only in log messages.

    Returns:
        'goalkeeper' or 'outfield' (always one of these two strings).
    """

    # Normalise column names to lowercase for comparison
    normalised_cols = set(df.columns.str.lower().str.strip())

    # ── Strategy 1: Check player_position column ──────────────────────
    # This is the most reliable signal. If the CSV has a column called
    # player_position and the first non-null value looks like a GK code,
    # we classify the whole file as goalkeeper.
    if 'player_position' in normalised_cols:
        # Get all unique non-null position values from the column
        position_col = _get_column_case_insensitive(df, 'player_position')
        if position_col is not None:
            unique_positions = (
                position_col
                .dropna()
                .astype(str)
                .str.lower()
                .str.strip()
                .unique()
            )
            for pos in unique_positions:
                for keyword in GK_POSITION_KEYWORDS:
                    if keyword in pos:
                        logger.info(
                            f"[DETECTOR] '{source_file}' → goalkeeper "
                            f"(player_position='{pos}')"
                        )
                        return 'goalkeeper'

    # ── Strategy 2: Check for GK-specific column names ────────────────
    # If the CSV header contains columns like 'saves' or 'clean_sheets',
    # the file is almost certainly a goalkeeper stats file.
    matching_gk_cols = normalised_cols & GK_COLUMNS
    if matching_gk_cols:
        logger.info(
            f"[DETECTOR] '{source_file}' → goalkeeper "
            f"(found GK columns: {matching_gk_cols})"
        )
        return 'goalkeeper'

    # ── Strategy 3: Fallback to outfield ──────────────────────────────
    logger.warning(
        f"[DETECTOR] '{source_file}' → outfield (fallback — "
        f"no GK signals found). "
        f"Columns found: {sorted(normalised_cols)}"
    )
    return 'outfield'


def _get_column_case_insensitive(df: pd.DataFrame, col_name: str):
    """
    Find a column in df regardless of its case (e.g. 'Player_Position' or 'PLAYER_POSITION').

    Returns the column Series, or None if not found.
    """
    for col in df.columns:
        if col.lower().strip() == col_name.lower():
            return df[col]
    return None


# ── Manual test (run this file directly to test the detector) ──────────
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s — %(message)s')

    print('\n--- Test 1: GK detected via player_position ---')
    df_gk_pos = pd.DataFrame({
        'player_name': ['Alisson Becker'],
        'player_position': ['GK'],
        'clean_sheets': [10],
    })
    result = detect_player_type(df_gk_pos, 'test_gk_position.csv')
    assert result == 'goalkeeper', f'Expected goalkeeper, got {result}'
    print(f'Result: {result} ✓')

    print('\n--- Test 2: GK detected via column names ---')
    df_gk_cols = pd.DataFrame({
        'player_name': ['Ederson'],
        'saves': [120],
        'goals_conceded': [30],
    })
    result = detect_player_type(df_gk_cols, 'test_gk_cols.csv')
    assert result == 'goalkeeper', f'Expected goalkeeper, got {result}'
    print(f'Result: {result} ✓')

    print('\n--- Test 3: Outfield player ---')
    df_outfield = pd.DataFrame({
        'player_name': ['Mohamed Salah'],
        'player_position': ['RW'],
        'goals': [25],
        'assists': [13],
    })
    result = detect_player_type(df_outfield, 'test_outfield.csv')
    assert result == 'outfield', f'Expected outfield, got {result}'
    print(f'Result: {result} ✓')

    print('\n--- Test 4: No position column, no GK columns (fallback) ---')
    df_unknown = pd.DataFrame({
        'player_name': ['Unknown Player'],
        'goals': [5],
    })
    result = detect_player_type(df_unknown, 'test_unknown.csv')
    assert result == 'outfield', f'Expected outfield fallback, got {result}'
    print(f'Result: {result} ✓')

    print('\n✅ All tests passed.')
