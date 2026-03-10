# Assignment 1: Working with Nested JSON Data

## 📋 Project Overview
This project processes a semi-structured JSON dataset containing Anime data. The goal was to practice handling **nested structures** (arrays and objects), transform them into a flat SQL table, and derive meaningful analytical insights using **Window Functions** in DuckDB.

## 💾 Dataset
* **Source:** MyAnimeList dataset (local file `ani_data_nested.json`).
* **Size:**  32.279 MB. (31.459 MB nested)
* **Structure:**
    * **Nested Array:** `genres` (e.g., `["Action", "Adventure"]`).
    * **Nested Object:** `scores` (key-value pairs for voting statistics, e.g., `"10": 7721`) -- (Created by hand in Python)

---

## 🛠️ Data Loading & Parsing Strategies

### 1. Loading
Data was loaded using DuckDB's `read_json_auto`, which automatically inferred the initial schema from the JSON file.

### 2. Parsing Logic
To transform the semi-structured data into a structured format for analysis, the following techniques were applied:

* **Unnesting Arrays:** Used `CROSS JOIN UNNEST(genres)` to expand the list of genres into individual rows, allowing for genre-specific analysis.
* **Flattening Objects:** Extracted keys from the `scores` object (e.g., `scores."10"`) into separate columns (`scored_10_by`, `scored_9_by`, etc.).
* **Data Cleaning & Typing:**
    * Used `TRY_CAST` to safely convert string fields to `INT` or `DOUBLE`.
    * Used `COALESCE` to handle `NULL` values (defaulting missing score counts to 0).
    * Parsed the `premiered` field to extract the release `year` as an integer.

#### SQL Snippet (Parsing Phase):
```sql
CREATE OR REPLACE TABLE anime_ready AS
SELECT
    COALESCE(name_english, name) as name,
    try_cast(score as double) as score,
    RIGHT(premiered, 4)::INT AS year,
    genre,
    -- Flattening the 'scores' object
    COALESCE(TRY_CAST(scores."10" as INT),0) as scored_10_by,
    COALESCE(TRY_CAST(scores."9" as INT),0) as scored_9_by
    -- ... (other score columns)
FROM anime
CROSS JOIN UNNEST(genres) as genres(genre)
WHERE name IS NOT NULL AND year IS NOT NULL;
```

## 📊 Analytical Insights

### Insight 1: Dynamics of Successful Anime releases 

**SQL Query:**
```sql
WITH num_anime AS (
    SELECT
        name, year, score,
        -- Find the highest-rated anime per year
        ROW_NUMBER() OVER(PARTITION BY year ORDER BY score DESC, scored_10_by DESC) as top_num
    FROM (SELECT DISTINCT name, year, score, scored_10_by FROM anime_ready) t
),
agg_anime AS (
    SELECT 
        ar.year, 
        -- Count unique successful anime (score >= 8)
        COUNT(DISTINCT ar.name) as succesful_anime
    FROM anime_ready ar
    WHERE ar.score >= 8
    GROUP BY ar.year
)
SELECT 
    aa.year, 
    aa.succesful_anime, 
    na.name as best_anime, 
    na.score
FROM agg_anime aa
LEFT JOIN num_anime na ON na.year = aa.year
WHERE na.top_num = 1
ORDER BY aa.succesful_anime DESC;
```

<img width="648" height="489" alt="visualization_years" src="https://github.com/user-attachments/assets/b035530b-54a4-483c-acba-3a2153a2c99b" />

**INSIGHT:**
Although the 2020s have seen a massive surge in the volume of acclaimed titles, the data proves that peak quality is timeless, as classics from earlier decades rival the scores of modern blockbusters.

### Insight 2: Quality Gap: Genre Average vs Top Hit

**SQL Query:**
```sql
with num_genre as (
    select name, score, year, genre, avg(score) over(partition by genre) as avg_score,
        row_number() over(partition by genre order by score desc, scored_10_by desc) as top_num
    from anime_ready
    )
select genre,round(avg_score,2) as avg_score, name, score, year
from num_genre
where top_num=1
order by avg_score desc;
```

<img width="852" height="567" alt="visualization_genres" src="https://github.com/user-attachments/assets/ac1b78d3-3437-4f28-9865-a34cec93908b" />

**INSIGHT:**
While mainstream genres like Adventure and Fantasy exhibit a vast gap between masterpieces like Frieren and the average, categories such as Suspense and Mystery demonstrate significantly higher consistency across all titles.
