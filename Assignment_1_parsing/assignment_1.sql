LOAD json;

     ------------ LOADING ---
create or replace table anime as
       select * from read_json_auto(
                     'Assignment_1_parsing/ani_data_nested.json'
       );
    --------------------------------
select * from anime;
describe anime;

select distinct premiered
from anime;


-- PARSING --
create or REPLACE table anime_ready as
       select
           COALESCE(name_english, name) as name,
           try_cast(score as  double) as score,
           ranked,
           popularity,
           RIGHT(premiered, 4)::INT AS year,
           studios,
           cast(scored_by as int) as scored_by,
           cast(replace(total, ',','') as int) as views,
           genre,

           COALESCE(TRY_CAST(scores."10" as INT),0) as scored_10_by,
           COALESCE(TRY_CAST(scores."9" as INT),0) as scored_9_by,
           COALESCE(TRY_CAST(scores."8" as INT),0) as scored_8_by,
           COALESCE(TRY_CAST(scores."7" as INT),0) as scored_7_by,
           COALESCE(TRY_CAST(scores."6" as INT),0) as scored_6_by,
           COALESCE(TRY_CAST(scores."5" as INT),0) as scored_5_by,
           COALESCE(TRY_CAST(scores."4" as INT),0) as scored_4_by,
           COALESCE(TRY_CAST(scores."3" as INT),0) as scored_3_by,
           COALESCE(TRY_CAST(scores."2" as INT),0) as scored_2_by,
           COALESCE(TRY_CAST(scores."1" as INT),0) as scored_1_by

       from anime
       cross join unnest(genres) as genres(genre)
       where
           name is not null
           and year is not null
           and score is not null;
---------------------------------

select * from anime_ready;


---------- 1st INSIGHT----------
with num_anime as(
    select
        distinct name, year, score,
        ROW_NUMBER() OVER(PARTITION BY year ORDER BY score DESC, scored_10_by DESC) as top_num
    from anime_ready
    ),
agg_anime as(
    select ar.year, count(distinct ar.name) as succesful_anime
    from anime_ready ar
    where ar.score>=8
    group by ar.year
    )
select aa.year, aa.succesful_anime, na.name as best_anime, na.score
from agg_anime aa
left join num_anime na on na.year=aa.year
where na.top_num=1
order by aa.succesful_anime desc, na.score desc;
--INSIGHT:
--Although the 2020s have seen a massive surge in the volume of acclaimed titles,
-- the data proves that peak quality is timeless, as classics from earlier
-- decades rival the scores of modern blockbusters.


---------- 2nd INSIGHT------------
with num_genre as (
    select name, score, year, genre, avg(score) over(partition by genre) as avg_score,
        row_number() over(partition by genre order by score desc, scored_10_by desc) as top_num
    from anime_ready
    )
select genre,round(avg_score,2) as avg_score, name, score, year
from num_genre
where top_num=1
order by avg_score desc;
--INSIGHT:
-- While mainstream genres like Adventure and Fantasy exhibit a vast gap
-- between masterpieces like Frieren and the average, categories such as
-- Suspense and Mystery demonstrate significantly higher consistency across all titles.

