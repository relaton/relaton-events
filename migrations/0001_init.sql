-- relaton-events#1: the freshness notification surface.
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,            -- dataset.updated | record.changed | record.superseded
  flavor TEXT NOT NULL,
  subject TEXT NOT NULL,         -- flavor id, or the record's corpus key
  payload TEXT NOT NULL,         -- the typed message payload, JSON
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_events_type ON events (type, id DESC);
CREATE INDEX idx_events_flavor ON events (flavor, id DESC);

CREATE TABLE subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL,
  secret TEXT NOT NULL,          -- HMAC-SHA256 signing key for deliveries
  flavors TEXT NOT NULL DEFAULT '*',  -- comma-separated filter, '*' = all
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE deliveries (
  event_id INTEGER NOT NULL REFERENCES events(id),
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  status TEXT NOT NULL DEFAULT 'pending',  -- pending | delivered | failed
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt TEXT,
  PRIMARY KEY (event_id, subscription_id)
);
