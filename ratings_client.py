"""Client for fetching film ratings from external APIs."""
import logging
import os

import requests

logger = logging.getLogger(__name__)

RATINGS_API = "https://api.agregarr.org/api/ratings"
OMDB_API = "https://www.omdbapi.com/"


def fetch_imdb_ratings(imdb_ids: list[str]) -> dict[str, dict]:
    """Fetch IMDb ratings for a list of IMDb IDs.

    Returns a dict mapping imdb_id -> {"rating": float, "votes": int}.
    IDs with no rating in the dataset are omitted from the result.
    """
    if not imdb_ids:
        return {}

    try:
        params = [("id", iid) for iid in imdb_ids]
        resp = requests.get(RATINGS_API, params=params, timeout=10)
        resp.raise_for_status()

        result = {}
        for item in resp.json():
            imdb_id = item.get("imdbId")
            rating = item.get("rating")
            if imdb_id and rating is not None:
                result[imdb_id] = {"rating": rating, "votes": item.get("votes")}
        return result
    except requests.RequestException as e:
        logger.error(f"Failed to fetch IMDb ratings: {e}")
        return {}


def fetch_rt_scores(imdb_ids: list[str]) -> dict[str, int]:
    """Fetch Rotten Tomatoes scores from OMDb for a list of IMDb IDs.

    Returns a dict mapping imdb_id -> rt_score (integer percentage, e.g. 85).
    Films with no RT score in OMDb are omitted from the result.
    """
    api_key = os.environ.get("OMDB_API_KEY")
    if not imdb_ids or not api_key:
        if not api_key:
            logger.warning("OMDB_API_KEY not set, skipping RT scores")
        return {}

    result = {}
    for imdb_id in imdb_ids:
        try:
            resp = requests.get(
                OMDB_API,
                params={"i": imdb_id, "apikey": api_key},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("Response") != "True":
                continue
            for rating in data.get("Ratings", []):
                if rating["Source"] == "Rotten Tomatoes":
                    try:
                        result[imdb_id] = int(rating["Value"].rstrip("%"))
                    except ValueError:
                        pass
                    break
        except requests.RequestException as e:
            logger.error(f"OMDb fetch failed for {imdb_id}: {e}")

    return result
