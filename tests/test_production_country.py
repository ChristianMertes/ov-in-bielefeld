"""Tests for the production-country language hint."""
import pytest

from production_country import maybe_en_or_fr, named_countries

# ── named_countries ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(("field", "expected"), [
    ("Italien 1974", ["italien"]),
    ("Israel/Frankreich 2024", ["israel", "frankreich"]),
    ("Frankreich, 2025", ["frankreich"]),
    ("Deutschland und Polen 2023", ["deutschland", "polen"]),
    ("USA 2026", ["usa"]),
    ("2023", []),
    ("", []),
    (None, []),
])
def test_named_countries(field, expected):
    assert named_countries(field) == expected


# ── maybe_en_or_fr ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field", [
    "USA 2026",
    "Großbritannien 2026",
    "Frankreich 2025",
    "Burkina Faso 2023",
    "Senegal 2024",
    "Kanada 2025",
])
def test_english_or_french_countries_keep_a_film(field):
    assert maybe_en_or_fr(field) is True


@pytest.mark.parametrize("field", [
    "Italien 1974",
    "Japan 2001",
    "Ukraine 2026",
    "Israel 2024",
    "Griechenland 2025",
    "Deutschland 2026",
])
def test_other_language_countries_allow_dropping_a_film(field):
    assert maybe_en_or_fr(field) is False


def test_coproduction_with_one_matching_partner_keeps_a_film():
    """"Israel/Frankreich" could well be the French film of the two."""
    assert maybe_en_or_fr("Israel/Frankreich 2024") is True


@pytest.mark.parametrize("field", ["", None, "2023", "Produktion"])
def test_no_country_gives_no_verdict(field):
    assert maybe_en_or_fr(field) is None


def test_unknown_country_name_gives_no_verdict():
    """A spelling this module does not know is not evidence against a film."""
    assert maybe_en_or_fr("Sansibar 2026") is None


def test_unknown_partner_in_a_coproduction_gives_no_verdict():
    assert maybe_en_or_fr("Italien/Sansibar 2026") is None
