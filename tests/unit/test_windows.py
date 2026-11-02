"""Window membership and the 31 features (design.md 1.5 and 3.1, D40a, D40b). Phase 3,
criterion 4: an event at t - W counts and an event at t doesn't."""

from __future__ import annotations

import pytest

from risk.features.compute import (
    FEATURE_NAMES,
    WINDOWS,
    DecisionMembers,
    Member,
    features_at,
    in_window,
    read_range,
)
from tests import oracle

T = 1_793_000_000_000

FAMILIES = {
    "out": "sender_out",
    "in": "sender_in",
    "attempt": "sender_attempts",
    "dest_in": "receiver_in",
    "dest_out": "receiver_out",
}


def only(family: str, *members: Member) -> DecisionMembers:
    sets: dict[str, list[Member]] = {field: [] for field in FAMILIES.values()}
    sets[FAMILIES[family]] = list(members)
    return DecisionMembers(**sets)


@pytest.mark.parametrize(("window", "width"), WINDOWS, ids=[w for w, _ in WINDOWS])
def test_an_event_at_t_minus_w_counts_and_one_at_t_does_not(window: str, width: int) -> None:
    assert in_window(T - width, T, width)
    assert not in_window(T, T, width)
    assert not in_window(T - width - 1, T, width)
    assert in_window(T - 1, T, width)


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize(("window", "width"), WINDOWS, ids=[w for w, _ in WINDOWS])
def test_each_feature_counts_its_window_edges(family: str, window: str, width: int) -> None:
    members = only(
        family,
        Member(T - width - 1, 1),  # just before the window
        Member(T - width, 10),  # the first moment inside it
        Member(T - 1, 100),  # the last moment inside it
        Member(T, 1000),  # t itself: not before t
        Member(T + 5, 10000),  # after t
    )
    features = features_at(T, 3, members)
    assert features[f"{family}_count_{window}"] == 2
    assert features[f"{family}_sum_{window}"] == 110


def test_the_read_range_covers_every_window_and_stops_before_t() -> None:
    low, high = read_range(T)
    assert (low, high) == (T - max(width for _, width in WINDOWS), T)


def test_the_names_are_design_md_1_5s_31_in_order() -> None:
    assert FEATURE_NAMES == oracle.NAMES
    assert len(set(FEATURE_NAMES)) == 31


def test_features_hold_every_name_and_the_amount_with_nothing_read() -> None:
    features = features_at(T, 12500, only("out"))
    assert list(features) == list(FEATURE_NAMES)
    assert features == dict.fromkeys(FEATURE_NAMES, 0) | {"amount": 12500}


def test_members_of_other_families_do_not_leak() -> None:
    features = features_at(T, 1, only("dest_out", Member(T - 1, 40)))
    nonzero = {name: value for name, value in features.items() if value}
    assert nonzero == {
        "dest_out_count_1m": 1,
        "dest_out_sum_1m": 40,
        "dest_out_count_1h": 1,
        "dest_out_sum_1h": 40,
        "dest_out_count_24h": 1,
        "dest_out_sum_24h": 40,
        "amount": 1,
    }
