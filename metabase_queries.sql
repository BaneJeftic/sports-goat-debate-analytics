-- 1. Messi vs. Ronaldo Mention Volume (Including Synonyms & Nicknames)
-- Counts total mentions incorporating popular nicknames and aliases for both players.
SELECT 
    'Messi' AS player,
    COUNT(*) AS total_mentions
FROM stream_cleaned_comments
WHERE LOWER(text) SIMILAR TO '%(messi|leo|lm10|pessi|lionel)%'
UNION ALL
SELECT 
    'Ronaldo' AS player,
    COUNT(*) AS total_mentions
FROM stream_cleaned_comments
WHERE LOWER(text) SIMILAR TO '%(ronaldo|cr7|penaldo|cristiano)%';


-- 2. Neutral Comments Percentage (Neither Messi nor Ronaldo Mentioned)
-- Calculates the volume and percentage of incoming comments that do not explicitly mention either player.
SELECT 
    COUNT(CASE WHEN NOT (LOWER(text) SIMILAR TO '%(messi|leo|lm10|pessi|lionel|ronaldo|cr7|penaldo|cristiano)%') THEN 1 END) AS neutral_comments,
    COUNT(*) AS total_comments,
    ROUND(
        (COUNT(CASE WHEN NOT (LOWER(text) SIMILAR TO '%(messi|leo|lm10|pessi|lionel|ronaldo|cr7|penaldo|cristiano)%') THEN 1 END)::decimal / COUNT(*)) * 100, 2
    ) AS neutral_percentage
FROM stream_cleaned_comments;


-- 3. Progressive Cumulative Debate Timeline (Window Function)
-- Tracks daily and cumulative running totals of player mentions over time to measure momentum shifts.
WITH daily_counts AS (
    SELECT 
        DATE(published_at) AS comment_date,
        COUNT(CASE WHEN LOWER(text) SIMILAR TO '%(messi|leo|lm10|pessi|lionel)%' THEN 1 END) AS messi_daily,
        COUNT(CASE WHEN LOWER(text) SIMILAR TO '%(ronaldo|cr7|penaldo|cristiano)%' THEN 1 END) AS ronaldo_daily
    FROM stream_cleaned_comments
    GROUP BY DATE(published_at)
)
SELECT 
    comment_date,
    messi_daily,
    ronaldo_daily,
    SUM(messi_daily) OVER (ORDER BY comment_date) AS messi_cumulative,
    SUM(ronaldo_daily) OVER (ORDER BY comment_date) AS ronaldo_cumulative
FROM daily_counts
ORDER BY comment_date ASC;


-- 4. Weighted Engagement Impact (Like Count Analysis)
SELECT 
    'Messi' AS player,
    COUNT(*) AS total_comments,
    COALESCE(SUM(like_count), 0) AS total_likes_received,
    ROUND(COALESCE(AVG(like_count), 0)::numeric, 2) AS avg_likes_per_comment
FROM stream_cleaned_comments
WHERE text ~* '(messi|leo|lm10|pessi|lionel)'

UNION ALL

SELECT 
    'Ronaldo' AS player,
    COUNT(*) AS total_comments,
    COALESCE(SUM(like_count), 0) AS total_likes_received,
    ROUND(COALESCE(AVG(like_count), 0)::numeric, 2) AS avg_likes_per_comment
FROM stream_cleaned_comments
WHERE text ~* '(ronaldo|cr7|penaldo|cristiano)';


-- 5. Demographics Filter: "Soccer" Keyword Subset
-- Analyzes player popularity exclusively within comments that contain the term "soccer".
SELECT 
    'Messi' AS player,
    COUNT(*) AS mentions
FROM stream_cleaned_comments
WHERE LOWER(text) LIKE '%soccer%' 
  AND LOWER(text) SIMILAR TO '%(messi|leo|lm10|pessi|lionel)%'
UNION ALL
SELECT 
    'Ronaldo' AS player,
    COUNT(*) AS mentions
FROM stream_cleaned_comments
WHERE LOWER(text) LIKE '%soccer%' 
  AND LOWER(text) SIMILAR TO '%(ronaldo|cr7|penaldo|cristiano)%';


-- 6. El Clásico Club Spillover: FC Barcelona vs. Real Madrid
-- Measures mention counts for FC Barcelona and Real Madrid within the debate dataset.
SELECT 
    'FC Barcelona' AS club,
    COUNT(*) AS mentions
FROM stream_cleaned_comments
WHERE LOWER(text) SIMILAR TO '%(barca|barcelona|fcb)%'
UNION ALL
SELECT 
    'Real Madrid' AS club,
    COUNT(*) AS mentions
FROM stream_cleaned_comments
WHERE LOWER(text) SIMILAR TO '%(real|madrid|real madrid)%';


-- 7. Top 10 Most Active Comment Authors
-- Ranks individual authors generating the highest number of comments along with their total acquired likes.
SELECT 
    author,
    COUNT(*) AS total_comments,
    SUM(like_count) AS total_likes_gained
FROM stream_cleaned_comments
GROUP BY author
ORDER BY total_comments DESC
LIMIT 10;