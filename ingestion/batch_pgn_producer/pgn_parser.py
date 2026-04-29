"""
Lichess PGN streaming parser.

Handles:
- Zstandard-compressed (.pgn.zst) and plain (.pgn) files
- All Lichess header tags
- Inline %eval and %clk annotations
- Memory-efficient streaming (one game at a time)
"""

import io
import re
import chess.pgn
import zstandard as zstd
from datetime import datetime
from typing import Iterator, Optional


EVAL_RE = re.compile(r"\[%eval\s+([^\]]+)\]")
CLK_RE = re.compile(r"\[%clk\s+([^\]]+)\]")


def _open_pgn_stream(path: str) -> io.TextIOWrapper:
    """Open a .pgn or .pgn.zst file as a streaming text reader."""
    raw = open(path, "rb")
    if path.endswith(".zst"):
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        reader = dctx.stream_reader(raw)
        return io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
    return io.TextIOWrapper(raw, encoding="utf-8", errors="replace")


def _parse_eval(raw: str) -> Optional[float]:
    """Convert a Stockfish eval string to a float in pawns.

    '0.17'  ->  0.17
    '#3'    ->  1000.0   (mate in 3 for side to move)
    '#-3'   -> -1000.0
    """
    raw = raw.strip()
    if raw.startswith("#"):
        try:
            mate_in = int(raw[1:])
            return 1000.0 if mate_in >= 0 else -1000.0
        except ValueError:
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_clock(raw: str) -> Optional[int]:
    """Convert 'H:MM:SS' clock string to total seconds."""
    parts = raw.strip().split(":")
    try:
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + int(float(s))
        if len(parts) == 2:
            m, s = parts
            return int(m) * 60 + int(float(s))
    except ValueError:
        return None
    return None


def _parse_time_control(tc: str) -> dict:
    """'300+0' -> {'initial': 300, 'increment': 0}"""
    if not tc or tc == "-":
        return {"initial": None, "increment": None}
    try:
        initial, increment = tc.split("+")
        return {"initial": int(initial), "increment": int(increment)}
    except (ValueError, AttributeError):
        return {"initial": None, "increment": None}


def _parse_utc_timestamp(date_str: str, time_str: str) -> Optional[int]:
    """Combine UTCDate + UTCTime into Unix epoch milliseconds."""
    if not date_str or not time_str or "?" in date_str:
        return None
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def _winner_from_result(result: str) -> Optional[str]:
    return {"1-0": "white", "0-1": "black", "1/2-1/2": None}.get(result)


def _extract_moves(game: chess.pgn.Game) -> dict:
    """Walk mainline, collecting SAN moves + per-ply eval/clock if present."""
    board = game.board()
    moves, evals, clocks = [], [], []

    for node in game.mainline():
        san = board.san(node.move)
        moves.append(san)
        board.push(node.move)

        comment = node.comment or ""
        eval_match = EVAL_RE.search(comment)
        clk_match = CLK_RE.search(comment)
        evals.append(_parse_eval(eval_match.group(1)) if eval_match else None)
        clocks.append(_parse_clock(clk_match.group(1)) if clk_match else None)

    return {
        "moves": " ".join(moves),
        "ply_count": len(moves),
        "evals": evals if any(e is not None for e in evals) else None,
        "clocks": clocks if any(c is not None for c in clocks) else None,
    }


def _normalize_game(game: chess.pgn.Game) -> dict:
    """Convert a python-chess Game into a flat dict matching the NDJSON schema."""
    h = game.headers
    site = h.get("Site", "")
    game_id = site.rsplit("/", 1)[-1] if "lichess.org/" in site else None

    record = {
        "id": game_id,
        "rated": h.get("Event", "").startswith("Rated"),
        "variant": h.get("Variant", "Standard").lower().replace(" ", ""),
        "event": h.get("Event"),
        "created_at": _parse_utc_timestamp(h.get("UTCDate", ""), h.get("UTCTime", "")),
        "result": h.get("Result"),
        "winner": _winner_from_result(h.get("Result", "")),
        "termination": h.get("Termination"),
        "eco": h.get("ECO"),
        "opening": h.get("Opening"),
        "time_control": _parse_time_control(h.get("TimeControl", "")),
        "players": {
            "white": {
                "name": h.get("White"),
                "rating": int(h["WhiteElo"]) if h.get("WhiteElo", "?").isdigit() else None,
                "rating_diff": int(h["WhiteRatingDiff"]) if h.get("WhiteRatingDiff") else None,
                "title": h.get("WhiteTitle"),
            },
            "black": {
                "name": h.get("Black"),
                "rating": int(h["BlackElo"]) if h.get("BlackElo", "?").isdigit() else None,
                "rating_diff": int(h["BlackRatingDiff"]) if h.get("BlackRatingDiff") else None,
                "title": h.get("BlackTitle"),
            },
        },
    }
    record.update(_extract_moves(game))
    return record


def parse_pgn(path: str, skip_errors: bool = True) -> Iterator[dict]:
    """Stream games from a Lichess PGN file as normalized dicts.

    Args:
        path: Path to .pgn or .pgn.zst file.
        skip_errors: If True, log and skip malformed games instead of raising.

    Yields:
        One dict per game, schema-aligned with the NDJSON API output.
    """
    stream = _open_pgn_stream(path)
    try:
        while True:
            try:
                game = chess.pgn.read_game(stream)
            except Exception:
                if skip_errors:
                    continue
                raise
            if game is None:
                break
            try:
                yield _normalize_game(game)
            except Exception:
                if not skip_errors:
                    raise
    finally:
        stream.close()


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m ingestion.batch_pgn_producer.pgn_parser <path-to-.pgn-or-.pgn.zst>")

    path = sys.argv[1]
    for i, game in enumerate(parse_pgn(path)):
        print(json.dumps(game, ensure_ascii=False))
        if i >= 4:
            break
