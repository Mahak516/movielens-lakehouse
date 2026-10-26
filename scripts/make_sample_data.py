#!/usr/bin/env python3
"""Generate a small MovieLens-format dataset (ratings.csv + movies.csv), stdlib only.

Why this exists: it lets anyone run and test the whole pipeline in seconds without
downloading the 25M-row dataset. A small share of deliberately dirty rows is injected
so the Silver quarantine logic has something to catch. The exact number injected per
defect type is printed, so you can check it against the pipeline's quarantine report.

    python scripts/make_sample_data.py --out data/sample
"""

from __future__ import annotations

import argparse
import csv
import random
from collections import Counter
from pathlib import Path

GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
    "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery",
    "Romance", "Sci-Fi", "Thriller", "War", "Western",
]
ADJECTIVES = ["Silent", "Crimson", "Last", "Hidden", "Electric", "Broken", "Midnight", "Golden", "Lost", "Rising"]
NOUNS = ["Harbor", "Empire", "Garden", "Signal", "Horizon", "Voyage", "Letter", "Machine", "Kingdom", "Promise"]

EPOCH_1996 = 820_454_400   # 1996-01-01 UTC
EPOCH_2019 = 1_546_300_800  # 2019-01-01 UTC


def build_movies(count: int, rng: random.Random) -> list[tuple[int, str, str]]:
    rows = []
    for movie_id in range(1, count + 1):
        name = f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS)} {movie_id}"
        if rng.random() < 0.15:  # titles with commas exercise CSV quoting
            name = f"{rng.choice(NOUNS)} {movie_id}, The"
        if rng.random() < 0.985:  # ~1.5% of titles carry no year, well under the 5% DQ threshold
            name = f"{name} ({rng.randint(1950, 2018)})"
        k = rng.choices([0, 1, 2, 3], weights=[3, 40, 40, 17])[0]
        genres = "(no genres listed)" if k == 0 else "|".join(rng.sample(GENRES, k))
        rows.append((movie_id, name, genres))
    return rows


def build_ratings(users: int, movies: int, count: int, rng: random.Random) -> list[list[str]]:
    user_bias = {u: rng.gauss(0, 0.4) for u in range(1, users + 1)}
    movie_quality = {m: rng.gauss(0, 0.6) for m in range(1, movies + 1)}
    popularity = [1 / (rank ** 0.8) for rank in range(1, movies + 1)]
    movie_ids = list(range(1, movies + 1))
    rng.shuffle(movie_ids)

    seen: set[tuple[int, int]] = set()
    rows: list[list[str]] = []
    while len(rows) < count:
        user = rng.randint(1, users)
        movie = rng.choices(movie_ids, weights=popularity)[0]
        if (user, movie) in seen:
            continue
        seen.add((user, movie))
        raw = 3.5 + user_bias[user] + movie_quality[movie] + rng.gauss(0, 0.7)
        rating = min(5.0, max(0.5, round(raw * 2) / 2))
        ts = rng.randint(EPOCH_1996, EPOCH_2019)
        rows.append([str(user), str(movie), f"{rating:.1f}", str(ts)])
    return rows


def inject_dirty_rows(rows: list[list[str]], movies: int, rate: float, rng: random.Random) -> Counter:
    """Append defective rows. Returns how many of each defect were injected."""
    defects: Counter = Counter()
    kinds = ["duplicate", "rating_out_of_range", "invalid_rating", "orphan_movie", "bad_timestamp", "not_half_step"]
    clean_rows = list(rows)
    for i in range(int(len(clean_rows) * rate)):
        kind = kinds[i % len(kinds)]
        base = rng.choice(clean_rows)
        user, movie, rating, ts = base
        if kind == "duplicate":
            rows.append([user, movie, "2.0", str(int(ts) + 86_400)])  # later timestamp: supersedes the original
        elif kind == "rating_out_of_range":
            rows.append([user, movie, rng.choice(["5.5", "0.0", "-1.0"]), ts])
        elif kind == "invalid_rating":
            rows.append([user, movie, "N/A", ts])
        elif kind == "orphan_movie":
            rows.append([user, str(movies + 1000 + i), "3.0", ts])
        elif kind == "bad_timestamp":
            rows.append([user, movie, rating, "-1"])
        elif kind == "not_half_step":
            rows.append([user, movie, "3.3", ts])
        defects[kind] += 1
    return defects


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/sample")
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--movies", type=int, default=500)
    parser.add_argument("--ratings", type=int, default=100_000)
    parser.add_argument("--dirty-rate", type=float, default=0.01, help="Share of extra defective rows")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    movie_rows = build_movies(args.movies, rng)
    rating_rows = build_ratings(args.users, args.movies, args.ratings, rng)
    defects = inject_dirty_rows(rating_rows, args.movies, args.dirty_rate, rng)
    rng.shuffle(rating_rows)

    with open(out / "movies.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["movieId", "title", "genres"])
        writer.writerows(movie_rows)
    with open(out / "ratings.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["userId", "movieId", "rating", "timestamp"])
        writer.writerows(rating_rows)

    print(f"Wrote {len(movie_rows)} movies and {len(rating_rows)} ratings to {out}/")
    print(f"Injected defects: {dict(defects)}")


if __name__ == "__main__":
    main()
