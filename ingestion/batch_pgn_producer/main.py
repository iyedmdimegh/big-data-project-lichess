"""
Batch PGN producer.

Reads a Lichess PGN dump (zstandard-compressed or plain), shapes each game into
a `game-results` event, and publishes to Kafka. Throttles to a configurable
messages-per-second rate so the downstream stream job has time to process.

Run from the repo root:
    python -m ingestion.batch_pgn_producer.main \
        --pgn data/lichess_db_standard_rated_2016-02.pgn.zst \
        --bootstrap localhost:29092 \
        --topic game-results \
        --rate 500 \
        --max-games 5000
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from typing import Optional

from kafka import KafkaProducer

from .pgn_parser import parse_pgn


log = logging.getLogger("batch_pgn_producer")

TOURNAMENT_ID_RE = re.compile(r"lichess\.org/tournament/([A-Za-z0-9]+)")


def extract_tournament_id(event_header: Optional[str]) -> Optional[str]:
    """Pull the tournament id out of a PGN Event header like
    'Rated Blitz tournament https://lichess.org/tournament/abc123'."""
    if not event_header:
        return None
    m = TOURNAMENT_ID_RE.search(event_header)
    return m.group(1) if m else None


def to_event(game: dict) -> Optional[dict]:
    """Project a parsed game dict into the `game-results` Kafka event schema.

    Returns None if the game is missing data we need (ratings or timestamp).
    """
    p = game.get("players", {})
    white = p.get("white", {})
    black = p.get("black", {})

    if not (white.get("rating") and black.get("rating") and game.get("created_at")):
        return None

    game_id = game.get("id")
    if not game_id:
        seed = f"{white.get('name')}|{black.get('name')}|{game.get('created_at')}"
        game_id = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

    tc = game.get("time_control") or {}

    return {
        "game_id":       game_id,
        "played_at":     game["created_at"],
        "white_player":  white.get("name"),
        "black_player":  black.get("name"),
        "white_elo":     white.get("rating"),
        "black_elo":     black.get("rating"),
        "white_diff":    white.get("rating_diff") or 0,
        "black_diff":    black.get("rating_diff") or 0,
        "result":        game.get("result"),
        "eco":           game.get("eco"),
        "opening":       game.get("opening"),
        "time_control":  tc.get("initial"),
        "tournament_id": extract_tournament_id(game.get("event")),
    }


def to_tourn_event(game_event: dict) -> Optional[dict]:
    """Project a `game-results` event into a `tourn-events` event.
    Returns None for non-tournament games."""
    if not game_event.get("tournament_id"):
        return None
    return {
        "tournament_id": game_event["tournament_id"],
        "game_id":       game_event["game_id"],
        "played_at":     game_event["played_at"],
        "white_player":  game_event["white_player"],
        "black_player":  game_event["black_player"],
        "white_elo":     game_event["white_elo"],
        "black_elo":     game_event["black_elo"],
        "result":        game_event["result"],
    }


def run(
    pgn_path: str,
    bootstrap: str,
    topic: str,
    tourn_topic: str,
    rate: int,
    max_games: Optional[int],
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=20,
        batch_size=65536,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    sent = 0
    sent_tourn = 0
    skipped = 0
    window_start = time.monotonic()

    try:
        for game in parse_pgn(pgn_path):
            event = to_event(game)
            if event is None:
                skipped += 1
                continue

            producer.send(topic, key=event["game_id"], value=event)
            sent += 1

            tourn_event = to_tourn_event(event)
            if tourn_event is not None:
                producer.send(tourn_topic, key=tourn_event["tournament_id"], value=tourn_event)
                sent_tourn += 1

            if rate > 0 and sent % rate == 0:
                elapsed = time.monotonic() - window_start
                remaining = 1.0 - elapsed
                if remaining > 0:
                    time.sleep(remaining)
                window_start = time.monotonic()

            if sent % 1000 == 0:
                log.info("sent=%d sent_tourn=%d skipped=%d", sent, sent_tourn, skipped)

            if max_games is not None and sent >= max_games:
                break
    finally:
        producer.flush()
        producer.close()
        log.info("done. sent=%d sent_tourn=%d skipped=%d", sent, sent_tourn, skipped)


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stream a Lichess PGN dump into Kafka.")
    p.add_argument("--pgn", required=True, help="Path to .pgn or .pgn.zst file")
    p.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    p.add_argument("--topic", default="game-results", help="Kafka topic")
    p.add_argument("--tourn-topic", default="tourn-events", help="Tournament events topic")
    p.add_argument("--rate", type=int, default=1000, help="Max messages per second (0 = unthrottled)")
    p.add_argument("--max-games", type=int, default=None, help="Stop after this many games")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(args.pgn, args.bootstrap, args.topic, args.tourn_topic, args.rate, args.max_games)
    return 0


if __name__ == "__main__":
    sys.exit(main())
