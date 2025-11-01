CREATE TABLE IF NOT EXISTS repositories (
    repo_id TEXT PRIMARY KEY,
    name TEXT,
    owner TEXT,
    stars INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stars_history (
    repo_id TEXT,
    stars INTEGER,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
