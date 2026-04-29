"""
Lichess live stream producer.

Connects to the public Lichess TV feed (https://lichess.org/api/tv/feed),
which emits NDJSON events for whichever game is currently featured on TV.

Event types we forward:
- "featured" : a new game is being featured. We emit to `player-events`.
- "fen"      : a move was played. We emit to `game-moves`.

Lichess switches the featured game when the current one ends, so a single
long-lived connection yields a continuous stream of moves across many games.

Run from the repo root:
    python -m ingestion.stream_producer.main \
        --bootstrap localhost:29092
"""

import argparse
import json
import logging
import sys
import time
from typing import Optional

import requests
from kafka import KafkaProducer


LICHESS_TV_FEED = "https://lichess.org/api/tv/feed"

log = logging.getLogger("stream_producer")


def featured_to_event(payload: dict, ingested_at_ms: int) -> dict:
    """Shape a `featured` payload into a `player-events` message."""
    players = payload.get("players") or []

    def _side(color: str) -> dict:
        for p in players:
            if p.get("color") == color:
                u = p.get("user") or {}
                return {
                    "name":   u.get("name"),
                    "title":  u.get("title"),
                    "rating": p.get("rating"),
                }
        return {"name": None, "title": None, "rating": None}

    return {
        "event_type":   "game_featured",
        "game_id":      payload.get("id"),
        "ingested_at":  ingested_at_ms,
        "orientation": payload.get("orientation"),
        "white":         _side("white"),
        "black":         _side("black"),
        "fen":          payload.get("fen"),
        "wc":           payload.get("wc"),
        "bc":           payload.get("bc"),
    }


def fen_to_event(payload: dict, current_game_id: Optional[str], ingested_at_ms: int) -> dict:
    """Shape a `fen` move payload into a `game-moves` message."""
    return {
        "event_type":   "move",
        "game_id":      current_game_id,
        "ingested_at":  ingested_at_ms,
        "fen":          payload.get("fen"),
        "last_move":    payload.get("lm"),
        "wc":           payload.get("wc"),
        "bc":           payload.get("bc"),
    }


def stream_once(producer: KafkaProducer, player_topic: str, move_topic: str) -> None:
    """Open one streaming connection; raises on disconnect for the caller to retry."""
    log.info("connecting to %s", LICHESS_TV_FEED)
    with requests.get(LICHESS_TV_FEED, stream=True, timeout=(10, None)) as resp:
        resp.raise_for_status()
        current_game_id: Optional[str] = None
        featured = 0
        moves = 0

        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("skipping non-JSON line: %r", raw[:120])
                continue

            t = msg.get("t")
            d = msg.get("d") or {}
            now_ms = int(time.time() * 1000)

            if t == "featured":
                current_game_id = d.get("id")
                event = featured_to_event(d, now_ms)
                producer.send(player_topic, key=current_game_id, value=event)
                featured += 1
                log.info("featured -> game_id=%s players=%s vs %s",
                         current_game_id,
                         (event["white"]["name"], event["white"]["rating"]),
                         (event["black"]["name"], event["black"]["rating"]))
            elif t == "fen":
                event = fen_to_event(d, current_game_id, now_ms)
                producer.send(move_topic, key=current_game_id, value=event)
                moves += 1
                if moves % 50 == 0:
                    log.info("moves=%d featured=%d", moves, featured)
            else:
                log.debug("ignoring event type=%s", t)


def run(bootstrap: str, player_topic: str, move_topic: str, max_reconnects: int) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=20,
        batch_size=32768,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    backoff = 1.0
    attempts = 0
    try:
        while max_reconnects < 0 or attempts <= max_reconnects:
            try:
                stream_once(producer, player_topic, move_topic)
                # Server closed the connection cleanly; small pause then reconnect.
                backoff = 1.0
            except requests.exceptions.RequestException as e:
                log.warning("connection error: %s; backing off %.1fs", e, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            attempts += 1
    finally:
        producer.flush()
        producer.close()


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lichess TV feed -> Kafka.")
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--player-topic", default="player-events")
    p.add_argument("--move-topic", default="game-moves")
    p.add_argument("--max-reconnects", type=int, default=-1,
                   help="Max reconnect attempts (-1 = unlimited).")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(args.bootstrap, args.player_topic, args.move_topic, args.max_reconnects)
    return 0


if __name__ == "__main__":
    sys.exit(main())
