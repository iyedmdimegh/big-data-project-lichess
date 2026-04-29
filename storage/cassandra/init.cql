CREATE KEYSPACE IF NOT EXISTS chess
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE chess;

-- stores every individual move event
CREATE TABLE IF NOT EXISTS game_moves (
    game_id text,
    move_number int,
    player_id text,
    move_uci text,
    clock_time_ms int,
    position_fen text,
    timestamp timestamp,
    PRIMARY KEY (game_id, move_number)
);

-- running ELO history per player
CREATE TABLE IF NOT EXISTS elo_history (
    player_id text,
    recorded_at timestamp,
    elo int,
    delta int,
    PRIMARY KEY (player_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- real-time bot suspicion scores
CREATE TABLE IF NOT EXISTS bot_scores (
    player_id text,
    window_start timestamp,
    engine_match_rate float,
    games_analyzed int,
    flagged boolean,
    PRIMARY KEY (player_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- live tournament standings; per-tournament partition, clustered by player_id.
-- Points is a regular column (mutable) so streaming upserts overwrite cleanly.
-- Top-of-leaderboard queries read the whole partition and sort client-side
-- (tournaments rarely exceed ~1000 players).
CREATE TABLE IF NOT EXISTS tournament_standings (
    tournament_id text,
    player_id text,
    points float,
    games_played int,
    wins int,
    draws int,
    losses int,
    last_updated timestamp,
    PRIMARY KEY (tournament_id, player_id)
);
