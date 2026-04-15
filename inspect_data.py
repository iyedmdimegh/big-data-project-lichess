import argparse
import csv
import io
import re
from pathlib import Path

import zstandard as zstd


HEADER_RE = re.compile(r'^\[(\w+)\s+"(.*)"\]$')


def iter_pgn_headers(text_stream: io.TextIOBase):
	"""Yield one dict of PGN headers per game."""
	headers = {}
	in_headers = False

	for raw_line in text_stream:
		line = raw_line.strip()

		if line.startswith("["):
			in_headers = True
			match = HEADER_RE.match(line)
			if match:
				key, value = match.groups()
				headers[key] = value
			continue

		if in_headers and line == "":
			if headers:
				yield headers
				headers = {}
			in_headers = False

	# Handle edge case where file ends right after headers.
	if headers:
		yield headers


def export_sample_to_csv(input_path: Path, output_path: Path, sample_size: int) -> int:
	fields = [
		"Event",
		"Site",
		"Date",
		"Round",
		"White",
		"Black",
		"Result",
		"WhiteElo",
		"BlackElo",
		"ECO",
		"Opening",
		"TimeControl",
		"Termination",
		"UTCDate",
		"UTCTime",
	]

	total_written = 0

	with input_path.open("rb") as compressed_file:
		decompressor = zstd.ZstdDecompressor()
		with decompressor.stream_reader(compressed_file) as stream_reader:
			text_stream = io.TextIOWrapper(stream_reader, encoding="utf-8", errors="replace")

			with output_path.open("w", newline="", encoding="utf-8") as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=fields)
				writer.writeheader()

				for headers in iter_pgn_headers(text_stream):
					row = {field: headers.get(field, "") for field in fields}
					writer.writerow(row)
					total_written += 1

					if total_written >= sample_size:
						break

	return total_written


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Create a CSV sample from a .pgn.zst file (Lichess format)."
	)
	parser.add_argument(
		"--input",
		type=Path,
		default=Path("data/lichess_db_standard_rated_2016-02.pgn.zst"),
		help="Path to the input .pgn.zst file.",
	)
	parser.add_argument(
		"--output",
		type=Path,
		default=Path("data/sample_games.csv"),
		help="Path to the output CSV sample.",
	)
	parser.add_argument(
		"--rows",
		type=int,
		default=100,
		help="Number of games to include in the sample.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	if args.rows <= 0:
		raise ValueError("--rows must be a positive integer.")

	if not args.input.exists():
		raise FileNotFoundError(f"Input file not found: {args.input}")

	args.output.parent.mkdir(parents=True, exist_ok=True)

	written = export_sample_to_csv(args.input, args.output, args.rows)
	print(f"Wrote {written} rows to {args.output}")


if __name__ == "__main__":
	main()
