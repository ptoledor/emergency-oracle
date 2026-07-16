"""Compatibility entry point for temporal candidate search v2."""

import search_temporal_candidates_v2 as search

# Quantiles are generated on even windows; keep shrinkage windows aligned.
search.SHRINK_WINDOWS = (28, 34, 42, 48, 56, 64, 70, 84)


if __name__ == "__main__":
    raise SystemExit(search.main())
