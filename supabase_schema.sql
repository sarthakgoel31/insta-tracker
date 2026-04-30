-- Social Branding Tracker — Supabase Postgres schema
-- Run this in Supabase SQL Editor (project: qutgezcgynqxqdtcgmfz)

-- User profiles (extends Supabase Auth)
CREATE TABLE IF NOT EXISTS tracker_profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  email TEXT,
  name TEXT DEFAULT '',
  tier TEXT DEFAULT 'free' CHECK (tier IN ('free', 'paid')),
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Auto-create profile on signup
CREATE OR REPLACE FUNCTION handle_new_tracker_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO tracker_profiles (id, email)
  VALUES (NEW.id, NEW.email);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS on_tracker_user_created ON auth.users;
CREATE TRIGGER on_tracker_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION handle_new_tracker_user();

-- Reels table
CREATE TABLE IF NOT EXISTS tracker_reels (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  title TEXT DEFAULT '',
  posted_date TEXT,
  platform TEXT DEFAULT 'instagram',
  account TEXT DEFAULT '',
  custom_fields JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_id, url)
);

CREATE INDEX IF NOT EXISTS idx_tracker_reels_user ON tracker_reels(user_id);

-- Snapshots (time-series views)
CREATE TABLE IF NOT EXISTS tracker_snapshots (
  id BIGSERIAL PRIMARY KEY,
  reel_id BIGINT NOT NULL REFERENCES tracker_reels(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  views INTEGER,
  likes INTEGER,
  comments INTEGER,
  fetched_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tracker_snapshots_reel ON tracker_snapshots(reel_id);
CREATE INDEX IF NOT EXISTS idx_tracker_snapshots_user ON tracker_snapshots(user_id);

-- Monthly views (cohort analysis)
CREATE TABLE IF NOT EXISTS tracker_monthly_views (
  id BIGSERIAL PRIMARY KEY,
  reel_id BIGINT NOT NULL REFERENCES tracker_reels(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  month TEXT NOT NULL,
  cumulative_views INTEGER,
  is_manual BOOLEAN DEFAULT false,
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(reel_id, month)
);

CREATE INDEX IF NOT EXISTS idx_tracker_monthly_user ON tracker_monthly_views(user_id);

-- Custom columns (per user)
CREATE TABLE IF NOT EXISTS tracker_custom_columns (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_id, name)
);

-- Row Level Security
ALTER TABLE tracker_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE tracker_reels ENABLE ROW LEVEL SECURITY;
ALTER TABLE tracker_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE tracker_monthly_views ENABLE ROW LEVEL SECURITY;
ALTER TABLE tracker_custom_columns ENABLE ROW LEVEL SECURITY;

-- Policies: users can only see/modify their own data
CREATE POLICY "Users read own profile" ON tracker_profiles FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Users update own profile" ON tracker_profiles FOR UPDATE USING (auth.uid() = id);

CREATE POLICY "Users read own reels" ON tracker_reels FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users insert own reels" ON tracker_reels FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users update own reels" ON tracker_reels FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users delete own reels" ON tracker_reels FOR DELETE USING (auth.uid() = user_id);

CREATE POLICY "Users read own snapshots" ON tracker_snapshots FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users insert own snapshots" ON tracker_snapshots FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users update own snapshots" ON tracker_snapshots FOR UPDATE USING (auth.uid() = user_id);

CREATE POLICY "Users read own monthly" ON tracker_monthly_views FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users insert own monthly" ON tracker_monthly_views FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users update own monthly" ON tracker_monthly_views FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users delete own monthly" ON tracker_monthly_views FOR DELETE USING (auth.uid() = user_id);

CREATE POLICY "Users read own columns" ON tracker_custom_columns FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users insert own columns" ON tracker_custom_columns FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users delete own columns" ON tracker_custom_columns FOR DELETE USING (auth.uid() = user_id);

-- Service role bypass for backend scraper (uses service_role key)
-- The FastAPI backend uses service_role key which bypasses RLS
