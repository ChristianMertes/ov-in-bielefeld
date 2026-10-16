"""Reading a language hint out of the production country a programme page prints.

The language filter needs a film's original language, which normally comes from
a TMDb match. For a film no lookup can identify, the only evidence left is the
"Produktion Italien 1974" line on the cinema's detail page.

A country is not a language: an Israeli film can be shot in English, a Belgian
one in Dutch. So this answers one narrow question only – could a film produced
there plausibly be the English or French original this site is about? Only a
clear "no" justifies dropping a film, which is why an unknown country name
yields no verdict at all rather than a convenient one.
"""
import re

# Countries whose cinema is substantially English- or French-language. A film
# from here is kept even without an identification, because dropping it would
# be the kind of mistake that silently empties the site. Switzerland, Belgium
# and Canada sit here for their French and English regions, although plenty of
# their films are in neither.
EN_FR_COUNTRIES = frozenset({
    # English
    "usa", "vereinigte staaten", "amerika", "großbritannien", "grossbritannien",
    "uk", "gb", "vereinigtes königreich", "england", "schottland", "wales",
    "nordirland", "irland", "kanada", "australien", "neuseeland", "südafrika",
    "nigeria", "ghana", "kenia", "jamaika",
    # French
    "frankreich", "belgien", "schweiz", "luxemburg", "quebec", "québec",
    "senegal", "burkina faso", "mali", "niger", "elfenbeinküste",
    "côte d'ivoire", "kamerun", "kongo", "dr kongo",
    "demokratische republik kongo", "guinea", "benin", "togo", "gabun",
    "tschad", "madagaskar", "mauretanien", "marokko", "algerien", "tunesien",
    "haiti", "libanon",
})

# Countries whose cinema is neither. Naming them explicitly, instead of
# treating everything outside the list above as foreign-language, keeps a
# country spelling nobody anticipated from quietly dropping a film.
OTHER_LANGUAGE_COUNTRIES = frozenset({
    "deutschland", "brd", "österreich", "italien", "spanien", "portugal",
    "niederlande", "holland", "dänemark", "schweden", "norwegen", "finnland",
    "island", "polen", "tschechien", "tschechische republik", "slowakei",
    "ungarn", "rumänien", "bulgarien", "griechenland", "zypern", "türkei",
    "russland", "ukraine", "belarus", "weißrussland", "serbien", "kroatien",
    "slowenien", "bosnien", "bosnien und herzegowina", "kosovo",
    "nordmazedonien", "mazedonien", "albanien", "estland", "lettland",
    "litauen", "georgien", "armenien", "aserbaidschan",
    "israel", "palästina", "iran", "irak", "syrien", "jordanien", "ägypten",
    "saudi-arabien", "katar", "kuwait", "jemen", "afghanistan",
    "china", "japan", "südkorea", "korea", "nordkorea", "taiwan", "thailand",
    "vietnam", "kambodscha", "laos", "myanmar", "indonesien", "malaysia",
    "philippinen", "indien", "pakistan", "bangladesch", "sri lanka", "nepal",
    "bhutan", "mongolei", "kasachstan", "usbekistan", "kirgisistan",
    "tadschikistan", "turkmenistan",
    "brasilien", "argentinien", "chile", "mexiko", "kolumbien", "peru",
    "uruguay", "paraguay", "bolivien", "ecuador", "venezuela", "kuba",
    "guatemala", "costa rica", "panama", "dominikanische republik",
    "äthiopien", "sudan", "somalia", "tansania", "uganda", "ruanda",
    "simbabwe", "mosambik", "angola", "namibia", "botswana",
})

# "Israel/Frankreich", "Frankreich, 2025", "Deutschland und Polen"
_COUNTRY_SEPARATOR = re.compile(r"[/,;&+]|\bund\b")
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")


def named_countries(country_field: str | None) -> list[str]:
    """Split a "Produktion" field into the country names it lists."""
    text = _YEAR.sub(" ", country_field or "")
    parts = (p.strip(" .-–—:").casefold() for p in _COUNTRY_SEPARATOR.split(text))
    return [p for p in parts if p]


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
    if any(name in EN_FR_COUNTRIES for name in names):
        return True
    if all(name in OTHER_LANGUAGE_COUNTRIES for name in names):
        return False
    return None
