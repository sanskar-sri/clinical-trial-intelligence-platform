# ============================================================
# SILVER COMMON UTILITIES
# Clinical Trial Intelligence Platform
# ============================================================
#
# Shared transformations used across Silver pipelines.
#
# Purpose:
#   - enforce consistent business-key normalization
#   - avoid different key semantics across Silver entities
#
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. BLANK STRING -> NULL
# ============================================================

def blank_to_null(column_name):
    """
    Normalize optional string values.

    Converts:
        NULL -> NULL
        ""   -> NULL
        "-"  -> NULL

    Otherwise returns the trimmed source value.
    """

    value = F.trim(
        F.col(column_name)
    )

    return (
        F.when(
            F.col(column_name).isNull()
            | value.isin("", "-"),
            F.lit(None)
        )
        .otherwise(value)
    )


# ============================================================
# 2. BUSINESS-KEY NORMALIZATION
# ============================================================

def normalize_key(column_name):
    """
    Apply the canonical Silver business-key normalization.

    Rules:
        1. trim whitespace
        2. convert blank / '-' to NULL
        3. convert remaining value to uppercase

    Example:
        " sub-001 " -> "SUB-001"
        ""          -> NULL
        "-"         -> NULL
        NULL        -> NULL

    All Silver primary and foreign business keys should use
    this function so joins use identical key semantics.
    """

    return F.upper(
        blank_to_null(column_name)
    )