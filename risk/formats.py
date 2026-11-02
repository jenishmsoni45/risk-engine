"""Value formats the contracts share (design.md 1.1 to 1.3). No imports beyond the standard
library, so the feature module, the API and the stub can all use them."""

from __future__ import annotations

import re

# tally's Idempotency-Key pattern, which a requestId is (D1): printable ASCII without the space.
IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7E]{1,255}")
# RFC 9562's 8-4-4-4-12 form; hex digits are case-insensitive on input.
CANONICAL_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
CURRENCY = re.compile(r"[A-Z]{3}")
# The largest amount a Java long, and a Postgres bigint, holds.
MAX_AMOUNT = 2**63 - 1
