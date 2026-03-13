"""Tests for CinemaxX scraper business logic."""
from scrapers.cinemaxx import (
    _has_language,
    _has_ov_marker,
    _is_omu_attr,
    _parse_film,
    _title_heuristic_ov,
    _clean_title,
    _extract_year,
    RELEVANT_LANGUAGES,
)


# ── _has_language ────────────────────────────────────────────────────────────

def _lang_attr(value, name="Language", attr_type="Language"):
    return {"attributeType": attr_type, "value": value, "name": name, "shortName": ""}


def test_has_language_english():
    assert _has_language([_lang_attr("english")], RELEVANT_LANGUAGES)


def test_has_language_english_german_spelling():
    assert _has_language([_lang_attr("englisch")], RELEVANT_LANGUAGES)


def test_has_language_french():
    assert _has_language([_lang_attr("french")], RELEVANT_LANGUAGES)


def test_has_language_french_german_spelling():
    assert _has_language([_lang_attr("französisch")], RELEVANT_LANGUAGES)


def test_has_language_case_insensitive():
    assert _has_language([_lang_attr("English")], RELEVANT_LANGUAGES)


def test_has_language_ignores_name_field():
    """Critical: 'Englische Untertitel' in name for a Hindi film must NOT match."""
    attr = {
        "attributeType": "Language",
        "name": "Englische Untertitel",
        "value": "hindi",
        "shortName": "",
    }
    assert not _has_language([attr], RELEVANT_LANGUAGES)


def test_has_language_ignores_non_language_type():
    attr = {"attributeType": "Format", "value": "english", "name": "", "shortName": ""}
    # attributeType is not "Language" → should still match (the function checks attributeType == "Language")
    assert not _has_language([attr], RELEVANT_LANGUAGES)


def test_has_language_empty():
    assert not _has_language([], RELEVANT_LANGUAGES)


def test_has_language_unknown_language():
    assert not _has_language([_lang_attr("japanese")], RELEVANT_LANGUAGES)


# ── _has_ov_marker ───────────────────────────────────────────────────────────

def _ov_attr(value):
    return {"attributeType": "ShowAttribute", "value": value, "name": ""}


def test_has_ov_marker_ov():
    assert _has_ov_marker([_ov_attr("ov")])


def test_has_ov_marker_om_u():
    assert _has_ov_marker([_ov_attr("om-u")])


def test_has_ov_marker_omu():
    assert _has_ov_marker([_ov_attr("omu")])


def test_has_ov_marker_original_version():
    assert _has_ov_marker([_ov_attr("original-version")])


def test_has_ov_marker_none():
    assert not _has_ov_marker([_ov_attr("3d"), _ov_attr("imax")])


def test_has_ov_marker_empty():
    assert not _has_ov_marker([])


# ── _is_omu_attr ─────────────────────────────────────────────────────────────

def test_is_omu_attr_om_u():
    assert _is_omu_attr({"value": "om-u"})


def test_is_omu_attr_omu():
    assert _is_omu_attr({"value": "omu"})


def test_is_omu_attr_ov_is_not_omu():
    assert not _is_omu_attr({"value": "ov"})


# ── _title_heuristic_ov ───────────────────────────────────────────────────────

def test_title_heuristic_sneak_ov():
    assert _title_heuristic_ov("Sneak OV")


def test_title_heuristic_title_ends_with_ov():
    assert _title_heuristic_ov("The Movie OV")


def test_title_heuristic_english_in_parens():
    assert _title_heuristic_ov("Der Film (English)")


def test_title_heuristic_normal_german_title():
    assert not _title_heuristic_ov("Die Drei Musketiere")


# ── _parse_film ───────────────────────────────────────────────────────────────

def _make_film(title_de="Test Film", title_orig="", session_attrs=None, film_attrs=None):
    return {
        "filmTitle": title_de,
        "originalTitle": title_orig,
        "sessionAttributes": session_attrs or [],
        "filmAttributes": film_attrs or [],
        "filmId": "123",
        "runningTime": 120,
        "releaseDate": "2025-01-15",
        "posterImageSrc": None,
        "filmUrl": "",
        "genres": [],
    }


def test_parse_film_with_english_attribute():
    film = _make_film(session_attrs=[_lang_attr("english")])
    result = _parse_film(film)
    assert result is not None
    assert result["title_display"] == "Test Film"


def test_parse_film_with_ov_heuristic():
    film = _make_film(title_de="Sneak OV")
    result = _parse_film(film)
    assert result is not None


def test_parse_film_no_ov_returns_none():
    film = _make_film(title_de="Normaler Deutscher Film")
    assert _parse_film(film) is None


def test_parse_film_no_title_returns_none():
    film = _make_film(title_de="")
    assert _parse_film(film) is None


def test_parse_film_extracts_year():
    film = _make_film(
        title_de="Test Film",
        session_attrs=[_lang_attr("english")],
    )
    film["releaseDate"] = "2024-06-20"
    result = _parse_film(film)
    assert result["release_year"] == 2024


# ── _clean_title ─────────────────────────────────────────────────────────────

def test_clean_title_strips_whitespace():
    assert _clean_title("  Hello  ") == "Hello"


def test_clean_title_strips_quotes():
    assert _clean_title('"Hello"') == "Hello"


# ── _extract_year ─────────────────────────────────────────────────────────────

def test_extract_year_iso_date():
    assert _extract_year("2024-06-20") == 2024


def test_extract_year_none():
    assert _extract_year(None) is None


def test_extract_year_empty_string():
    assert _extract_year("") is None
