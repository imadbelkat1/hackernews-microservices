-- Users Table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    karma INTEGER NOT NULL DEFAULT 0 CHECK (karma >= 0),
    about TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    submitted_ids INTEGER[] DEFAULT '{}'
);
-- Stories table
CREATE TABLE IF NOT EXISTS stories (
    id INTEGER PRIMARY KEY,
    type VARCHAR(10) DEFAULT 'Story' NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    score INTEGER DEFAULT 0 CHECK (score >= 0),
    author VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    comments_ids INTEGER[] DEFAULT '{}',     -- IDs of comments associated with the story
    comments_count INTEGER DEFAULT 0 CHECK (comments_count >= 0)
);

-- Asks table
CREATE TABLE IF NOT EXISTS asks (
    id INTEGER PRIMARY KEY,
    type VARCHAR(10) DEFAULT 'Ask' NOT NULL,
    title TEXT NOT NULL,
    text TEXT,
    score INTEGER DEFAULT 0 CHECK (score >= 0),
    author VARCHAR(255) NOT NULL,
    reply_ids INTEGER[] DEFAULT '{}',
    replies_count INTEGER DEFAULT 0 CHECK (replies_count >= 0),
    created_at BIGINT NOT NULL
);

-- Jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    type VARCHAR(10) DEFAULT 'Job' NOT NULL,
    title TEXT NOT NULL,
    text TEXT,
    url TEXT,
    score INTEGER DEFAULT 0 CHECK (score >= 0),
    author VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL
);

-- Comments table 
CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY,
    type VARCHAR(10) DEFAULT 'Comment' NOT NULL,
    text TEXT NOT NULL,
    author VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    parent_id INTEGER,
    reply_ids INTEGER[] DEFAULT '{}'
);

-- Polls table
CREATE TABLE IF NOT EXISTS polls (
    id INTEGER PRIMARY KEY,
    type VARCHAR(10) DEFAULT 'Poll' NOT NULL,
    title TEXT NOT NULL,
    score INTEGER DEFAULT 0 CHECK (score >= 0),
    author VARCHAR(255) NOT NULL,
    poll_options INTEGER[] DEFAULT '{}',
    reply_ids INTEGER[] DEFAULT '{}',
    created_at BIGINT NOT NULL
);

-- Poll Options table
CREATE TABLE IF NOT EXISTS poll_options (
    id INTEGER PRIMARY KEY NOT NULL,
    type VARCHAR(10) DEFAULT 'PollOption' NOT NULL,
    poll_id INTEGER NOT NULL,
    author VARCHAR(255) NOT NULL,
    option_text TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    votes INTEGER DEFAULT 0 CHECK (votes >= 0)
);