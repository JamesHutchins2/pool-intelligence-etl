import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# -----------------------------
# Helpers
# -----------------------------

def _normalize_text_series(s: pd.Series) -> pd.Series:
    """
    Lowercase, remove punctuation, collapse whitespace.
    Keep digits for things like 16x32.
    """
    return (
        s.fillna("")
         .astype(str)
         .str.lower()
         .str.replace(r"[^a-z0-9\s]", " ", regex=True)
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
    )

def _phrase_window_pattern(target: str, phrases: list[str], window_words: int = 12) -> str:
    """
    Build a regex that matches:
      target ... (<=window words) ... any(phrases)
    OR
      any(phrases) ... (<=window words) ... target

    Works for multi-word phrases too.
    """
    # Escape phrases and turn spaces into \s+ so "maintenance fees" works
    def esc_phrase(p: str) -> str:
        p = p.strip().lower()
        p = re.escape(p)
        p = p.replace(r"\ ", r"\s+")
        return p

    target_pat = esc_phrase(target)
    phrase_pats = [esc_phrase(p) for p in phrases if p and p.strip()]
    if not phrase_pats:
        return r"(?!x)x"  # never match

    any_phrase = r"(?:%s)" % r"|".join(phrase_pats)

    # Note: \w+ matches letters/digits/_; our normalized text is mostly that.
    between = rf"(?:\s+\w+){{0,{window_words}}}"

    return rf"(?:{target_pat}{between}\s+{any_phrase})|(?:{any_phrase}{between}\s+{target_pat})"


def _contains_any(df: pd.DataFrame, col: str, phrases: list[str]) -> pd.Series:
    if not phrases:
        return pd.Series(False, index=df.index)
    pat = r"(?:%s)" % "|".join([re.escape(p).replace(r"\ ", r"\s+") for p in phrases])
    return df[col].str.contains(pat, regex=True, na=False)


# -----------------------------
# Step 1: Normalize
# -----------------------------

def clean_and_normalize_description(df: pd.DataFrame, src_col: str = "Description") -> pd.DataFrame:
    df = df.copy()
    df["description_norm"] = _normalize_text_series(df.get(src_col, pd.Series("", index=df.index)))
    return df


# -----------------------------
# Step 2: Pool mention flag + exclusions
# -----------------------------

def create_pool_flag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # "pool" mention (broad)
    df["pool_flag"] = df["description_norm"].str.contains(r"\bpool\b", regex=True, na=False)

    # Exclusions that should flip pool_flag back to False
    # (Keep this list short + high precision.)
    exclusion_phrases = [
        "pool table",
        "carpool",
        "pooling",
        "shared pool of",  # sometimes used in finance/legal language
    ]
    df["pool_exclusion_flag"] = _contains_any(df, "description_norm", exclusion_phrases)

    df.loc[df["pool_exclusion_flag"], "pool_flag"] = False
    return df


# -----------------------------
# Step 3: Negations
# -----------------------------

def identify_pool_negation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect cases like "no pool", "without a pool".
    If negated, we should treat as no private/community pool even if 'pool' appears.
    """
    df = df.copy()

    neg_phrases = [
        "no pool",
        "without a pool",
        "does not have a pool",
        "doesn't have a pool",
        "not a pool",
    ]
    df["pool_negation_flag"] = _contains_any(df, "description_norm", neg_phrases)

    # If negated, force pool_flag False (you can decide if you want to keep mention separately)
    df.loc[df["pool_negation_flag"], "pool_flag"] = False
    return df


# -----------------------------
# Step 4: Communal vs Private indicators near "pool"
# -----------------------------

def identify_communal_indications(df: pd.DataFrame, window_words: int = 12) -> pd.DataFrame:
    df = df.copy()

    communal_flags = [
        "community",
        "condo",
        "clubhouse",
        "amenities",
        "fitness",
        "maintenance fees",
        "hoa",
        "gated community",
        "rec centre",
        "recreation centre",
        "indoor pool",
        "shared",
        "residents",
        "membership",
        "facility",
        "common elements",
    ]

    pat = _phrase_window_pattern("pool", communal_flags, window_words=window_words)
    df["communal_pool_flag"] = df["description_norm"].str.contains(pat, regex=True, na=False)

    # Only meaningful if pool mentioned
    df.loc[~df["pool_flag"], "communal_pool_flag"] = False
    return df


def identify_private_indicators(df: pd.DataFrame, window_words: int = 12) -> pd.DataFrame:
    df = df.copy()

    private_flags = [
        "private",
        "backyard",
        "yard",
        "backyard oasis",
        "pool house",
        "patio",
        "deck",
        "interlocked",
        "landscaped",
        "in ground",
        "inground",
        "in ground pool",
        "in ground swimming pool",
        "saltwater",
        "hot tub",
        "walkout",
        "walk out",
        "fenced",
        "heater",
        "heated",
        "pump",
        "diving board",
        "liner",
        "gazebo",
    ]

    pat = _phrase_window_pattern("pool", private_flags, window_words=window_words)

    df["private_pool_flag"] = df["description_norm"].str.contains(
        pat, regex=True, na=False
    )

    # Only meaningful if a pool was mentioned
    df.loc[~df["pool_flag"], "private_pool_flag"] = False

    return df



# -----------------------------
# Step 5: Pool construction type (optional enrichment)
# -----------------------------

def infer_pool_construction_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    in_ground_flags = [
        "in ground",
        "inground",
        "gunite",
        "concrete",
        "fiberglass",
        "saltwater",
        "lap pool",
        "plunge pool",
        "diving board",
        "pool house",
    ]
    above_ground_flags = [
        "above ground",
        "aboveground",
        "portable",
        "inflatable",
        "removable",
    ]

    df["in_ground_pool_flag"] = _contains_any(df, "description_norm", in_ground_flags)
    df["above_ground_pool_flag"] = _contains_any(df, "description_norm", above_ground_flags)

    # Only meaningful if pool mentioned
    df.loc[~df["pool_flag"], ["in_ground_pool_flag", "above_ground_pool_flag"]] = False
    return df


# -----------------------------
# Step 6: Final decision + evidence + confidence
# -----------------------------

def infer_pool_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Outputs:
      - pool_type: none | private | community | unknown
      - pool_confidence: 0..1 (simple deterministic)
      - pool_evidence: short string for QA/debug
    """
    # Handle empty DataFrame from upstream failures
    if df.empty or "Description" not in df.columns:
        logger.warning(f"Empty DataFrame or missing Description column. Columns: {df.columns.tolist()}")
        return pd.DataFrame()
    
    df = df.copy()

    # base
    df["pool_type"] = "none"
    df["pool_confidence"] = 0.0
    df["pool_evidence"] = ""

    # If no pool mention after exclusions/negations => none
    has_pool = df["pool_flag"].fillna(False)

    # Scores: keep it simple + explainable
    private_score = (
        df["private_pool_flag"].astype(int) * 3
        + df["in_ground_pool_flag"].astype(int) * 2
        + df["above_ground_pool_flag"].astype(int) * 1
    )
    communal_score = df["communal_pool_flag"].astype(int) * 3

    # Decide only where pool is mentioned
    idx = df.index[has_pool]

    # defaults where pool mentioned
    df.loc[idx, "pool_type"] = "unknown"
    df.loc[idx, "pool_confidence"] = 0.55
    df.loc[idx, "pool_evidence"] = "pool_mentioned_no_strong_signal"

    # private wins
    private_wins = has_pool & (private_score >= communal_score + 2) & (private_score > 0)
    df.loc[private_wins, "pool_type"] = "private"
    df.loc[private_wins, "pool_confidence"] = (0.7 + 0.1 * (private_score[private_wins] - communal_score[private_wins])).clip(0, 0.95)
    df.loc[private_wins, "pool_evidence"] = "private_cues_near_pool"

    # community wins
    communal_wins = has_pool & (communal_score >= private_score + 2) & (communal_score > 0)
    df.loc[communal_wins, "pool_type"] = "community"
    df.loc[communal_wins, "pool_confidence"] = (0.7 + 0.1 * (communal_score[communal_wins] - private_score[communal_wins])).clip(0, 0.95)
    df.loc[communal_wins, "pool_evidence"] = "community_cues_near_pool"

    # tie/ambiguous but has some signal
    ambiguous = has_pool & ~(private_wins | communal_wins) & ((private_score > 0) | (communal_score > 0))
    df.loc[ambiguous, "pool_type"] = "unknown"
    df.loc[ambiguous, "pool_confidence"] = (0.6 + 0.05 * (private_score[ambiguous] + communal_score[ambiguous])).clip(0, 0.85)
    df.loc[ambiguous, "pool_evidence"] = "ambiguous_private_vs_community"

    return df



def infer_pool_construction(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - pool_construction: None | 'in_ground' | 'above_ground' | 'unknown'
    Notes:
      - Most rows will remain None.
      - We only populate this when there's a strong indicator.
    """
    df = df.copy()

    # Normalize flags (assumes description_norm exists)
    in_ground_phrases = [
        "in ground", "inground", "in ground pool", "in ground swimming pool",
        "gunite", "concrete", "fiberglass",
        "saltwater", "lap pool", "plunge pool",
        "diving board", "pool house"
    ]
    above_ground_phrases = [
        "above ground", "aboveground", "above ground pool",
        "portable", "inflatable", "removable"
    ]

    # Strong patterns
    # Example: "16x32 pool" is typically in-ground, but keep as weak unless other cues exist
    dim_pat = re.compile(r"\b\d{1,2}\s*x\s*\d{1,2}\b")

    def _contains_any(s: pd.Series, phrases: list[str]) -> pd.Series:
        pat = r"(?:%s)" % "|".join([re.escape(p).replace(r"\ ", r"\s+") for p in phrases])
        return s.str.contains(pat, regex=True, na=False)

    in_ground_flag = _contains_any(df["description_norm"], in_ground_phrases)
    above_ground_flag = _contains_any(df["description_norm"], above_ground_phrases)
    has_dim = df["description_norm"].str.contains(dim_pat, regex=True, na=False)

    # Default: None (your request)
    df["pool_construction"] = None

    # Only consider construction if "pool_type" is private (or unknown but pool mentioned)
    # This avoids tagging condo/amenity indoor pools as "in_ground" because of generic words.
    eligible = df.get("pool_type", pd.Series("unknown", index=df.index)).isin(["private", "unknown"]) & df.get("pool_flag", True)

    # Scoring: keep deterministic
    in_score = (in_ground_flag.astype(int) * 3) + (has_dim.astype(int) * 1)
    ab_score = (above_ground_flag.astype(int) * 3)

    # Decide
    in_wins = eligible & (in_score >= ab_score + 2) & (in_score > 0)
    ab_wins = eligible & (ab_score >= in_score + 2) & (ab_score > 0)
    ambiguous = eligible & ~(in_wins | ab_wins) & ((in_score > 0) | (ab_score > 0))

    df.loc[in_wins, "pool_construction"] = "in_ground"
    df.loc[ab_wins, "pool_construction"] = "above_ground"
    df.loc[ambiguous, "pool_construction"] = "unknown"

    # If pool_type is none/community, force None (even if phrases appear)
    non_private = df.get("pool_type", pd.Series("none", index=df.index)).isin(["none", "community"])
    df.loc[non_private, "pool_construction"] = None

    return df

def rectify_pool_inference(df: pd.DataFrame) -> pd.DataFrame:
    """
    FINAL OUTPUT CONTRACT
    ---------------------
    pool_flag : True  -> private pool present
                False -> no private pool

    pool_type : 'in-ground' | 'above-ground' | 'none'
    """

    df = df.copy()

    # Default: no private pool
    df["pool_flag"] = False
    df["pool_type"] = "none"

    # Mask for confirmed private pools
    private_mask = df["pool_type"].eq("private")

    # Private + in-ground
    in_ground_mask = private_mask & df["in_ground_pool_flag"]
    df.loc[in_ground_mask, "pool_flag"] = True
    df.loc[in_ground_mask, "pool_type"] = "in-ground"

    # Private + above-ground
    above_ground_mask = private_mask & df["above_ground_pool_flag"]
    df.loc[above_ground_mask, "pool_flag"] = True
    df.loc[above_ground_mask, "pool_type"] = "above-ground"

    # Private but construction unknown → still private pool, type = none
    unknown_private_mask = private_mask & ~(in_ground_mask | above_ground_mask)
    df.loc[unknown_private_mask, "pool_flag"] = True
    df.loc[unknown_private_mask, "pool_type"] = "none"

    # Drop intermediate / debug columns
    df = df.drop(
        columns=[
            "pool_confidence",
            "pool_evidence",
            "in_ground_pool_flag",
            "above_ground_pool_flag",
            "description_norm",
            "pool_exclusion_flag",
            "pool_negation_flag",
            "communal_pool_flag",
            "private_pool_flag",
        ],
        errors="ignore",
    )

    return df



# -----------------------------
# One-call pipeline helper
# -----------------------------

def add_pool_inference_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    A single call you can drop into your ETL transform chain.
    """
    df = clean_and_normalize_description(df)
    df = create_pool_flag(df)
    df = identify_pool_negation(df)
    df = identify_communal_indications(df, window_words=12)
    df = identify_private_indicators(df, window_words=12)
    df = infer_pool_construction_type(df)
    df = infer_pool_type(df)
    df = rectify_pool_inference(df)
    return df
