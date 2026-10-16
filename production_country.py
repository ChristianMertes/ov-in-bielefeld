"""Reading the "Produktion Italien 1974" line a programme page prints.

Two jobs come out of that line. It carries a language hint for films nothing
could identify – could a film produced there be the English or French original
this site is about? And it lets a title match be checked against the page: a
search result whose production countries are nowhere near the ones the cinema
states is a different film.

A country is not a language: an Israeli film can be shot in English, a Belgian
one in Dutch. So a clear "no" is all this claims, and a country name the table
has never heard of yields no verdict rather than a convenient one.
"""
import re

# Country name as a German programme page writes it → (ISO 3166-1 alpha-2,
# cinema is substantially English- or French-language).
#
# Switzerland, Belgium and Canada count as yes for their French and English
# regions although plenty of their films are in neither: keeping a film that
# does not belong is the cheaper mistake. Anything genuinely unlisted stays
# unjudged, which is why the foreign-language side is spelled out too instead
# of being "everything else".
COUNTRIES: dict[str, tuple[str, bool]] = {
    # ── English ──────────────────────────────────────────────────────────────
    "usa": ("US", True), "vereinigte staaten": ("US", True), "amerika": ("US", True),
    "großbritannien": ("GB", True), "grossbritannien": ("GB", True),
    "uk": ("GB", True), "gb": ("GB", True), "vereinigtes königreich": ("GB", True),
    "england": ("GB", True), "schottland": ("GB", True), "wales": ("GB", True),
    "nordirland": ("GB", True), "irland": ("IE", True), "kanada": ("CA", True),
    "quebec": ("CA", True), "québec": ("CA", True), "australien": ("AU", True),
    "neuseeland": ("NZ", True), "südafrika": ("ZA", True), "nigeria": ("NG", True),
    "ghana": ("GH", True), "kenia": ("KE", True), "jamaika": ("JM", True),
    # ── French ───────────────────────────────────────────────────────────────
    "frankreich": ("FR", True), "belgien": ("BE", True), "schweiz": ("CH", True),
    "luxemburg": ("LU", True), "senegal": ("SN", True), "burkina faso": ("BF", True),
    "mali": ("ML", True), "niger": ("NE", True), "elfenbeinküste": ("CI", True),
    "côte d'ivoire": ("CI", True), "kamerun": ("CM", True), "kongo": ("CG", True),
    "dr kongo": ("CD", True), "demokratische republik kongo": ("CD", True),
    "guinea": ("GN", True), "benin": ("BJ", True), "togo": ("TG", True),
    "gabun": ("GA", True), "tschad": ("TD", True), "madagaskar": ("MG", True),
    "mauretanien": ("MR", True), "marokko": ("MA", True), "algerien": ("DZ", True),
    "tunesien": ("TN", True), "haiti": ("HT", True), "libanon": ("LB", True),
    # ── Neither ──────────────────────────────────────────────────────────────
    "deutschland": ("DE", False), "brd": ("DE", False), "österreich": ("AT", False),
    "italien": ("IT", False), "spanien": ("ES", False), "portugal": ("PT", False),
    "niederlande": ("NL", False), "holland": ("NL", False), "dänemark": ("DK", False),
    "schweden": ("SE", False), "norwegen": ("NO", False), "finnland": ("FI", False),
    "island": ("IS", False), "polen": ("PL", False), "tschechien": ("CZ", False),
    "tschechische republik": ("CZ", False), "slowakei": ("SK", False),
    "ungarn": ("HU", False), "rumänien": ("RO", False), "bulgarien": ("BG", False),
    "griechenland": ("GR", False), "zypern": ("CY", False), "türkei": ("TR", False),
    "russland": ("RU", False), "ukraine": ("UA", False), "belarus": ("BY", False),
    "weißrussland": ("BY", False), "serbien": ("RS", False), "kroatien": ("HR", False),
    "slowenien": ("SI", False), "bosnien": ("BA", False), "kosovo": ("XK", False),
    "nordmazedonien": ("MK", False), "mazedonien": ("MK", False),
    "albanien": ("AL", False), "estland": ("EE", False), "lettland": ("LV", False),
    "litauen": ("LT", False), "georgien": ("GE", False), "armenien": ("AM", False),
    "aserbaidschan": ("AZ", False),
    "israel": ("IL", False), "palästina": ("PS", False), "iran": ("IR", False),
    "irak": ("IQ", False), "syrien": ("SY", False), "jordanien": ("JO", False),
    "ägypten": ("EG", False), "saudi-arabien": ("SA", False), "katar": ("QA", False),
    "kuwait": ("KW", False), "jemen": ("YE", False), "afghanistan": ("AF", False),
    "china": ("CN", False), "japan": ("JP", False), "südkorea": ("KR", False),
    "korea": ("KR", False), "nordkorea": ("KP", False), "taiwan": ("TW", False),
    "thailand": ("TH", False), "vietnam": ("VN", False), "kambodscha": ("KH", False),
    "laos": ("LA", False), "myanmar": ("MM", False), "indonesien": ("ID", False),
    "malaysia": ("MY", False), "philippinen": ("PH", False), "indien": ("IN", False),
    "pakistan": ("PK", False), "bangladesch": ("BD", False), "sri lanka": ("LK", False),
    "nepal": ("NP", False), "bhutan": ("BT", False), "mongolei": ("MN", False),
    "kasachstan": ("KZ", False), "usbekistan": ("UZ", False),
    "kirgisistan": ("KG", False), "tadschikistan": ("TJ", False),
    "turkmenistan": ("TM", False),
    "brasilien": ("BR", False), "argentinien": ("AR", False), "chile": ("CL", False),
    "mexiko": ("MX", False), "kolumbien": ("CO", False), "peru": ("PE", False),
    "uruguay": ("UY", False), "paraguay": ("PY", False), "bolivien": ("BO", False),
    "ecuador": ("EC", False), "venezuela": ("VE", False), "kuba": ("CU", False),
    "guatemala": ("GT", False), "costa rica": ("CR", False), "panama": ("PA", False),
    "dominikanische republik": ("DO", False),
    "äthiopien": ("ET", False), "sudan": ("SD", False), "somalia": ("SO", False),
    "tansania": ("TZ", False), "uganda": ("UG", False), "ruanda": ("RW", False),
    "simbabwe": ("ZW", False), "mosambik": ("MZ", False), "angola": ("AO", False),
    "namibia": ("NA", False), "botswana": ("BW", False),
}

# "Israel/Frankreich", "Frankreich, 2025", "Deutschland und Polen"
_COUNTRY_SEPARATOR = re.compile(r"[/,;&+]|\bund\b")
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")


def named_countries(country_field: str | None) -> list[str]:
    """Split a "Produktion" field into the country names it lists."""
    text = _YEAR.sub(" ", country_field or "")
    parts = (p.strip(" .-–—:").casefold() for p in _COUNTRY_SEPARATOR.split(text))
    return [p for p in parts if p]


def iso_codes(country_field: str | None) -> set[str]:
    """Return the ISO 3166-1 codes of the countries named, as far as known."""
    return {
        COUNTRIES[name][0]
        for name in named_countries(country_field)
        if name in COUNTRIES
    }


def maybe_en_or_fr(country_field: str | None) -> bool | None:
    """Could a film produced here be an English or French original?

    True keeps a film, False allows dropping it, None means the field carries
    no usable evidence either way – an empty line, or a country this module has
    never heard of.
    """
    names = named_countries(country_field)
    if not names:
        return None
    # Co-productions count as a yes on a single matching partner: the language
    # tends to follow whichever side holds the production, and a wrong keep is
    # the cheaper mistake.
    if any(COUNTRIES.get(name, ("", False))[1] for name in names):
        return True
    if all(name in COUNTRIES for name in names):
        return False
    return None
