-- Remove duplicates

WITH CTE AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY title ORDER BY show_id) AS rn
    FROM netflix_shows
)
SELECT * FROM CTE WHERE rn > 1;

WITH CTE AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY title ORDER BY show_id) AS rn
    FROM netflix_shows
)
DELETE FROM CTE WHERE rn > 1;

-- create directors table

SELECT show_id, TRIM(value) AS director 
INTO netflix_directors
FROM netflix_shows 
CROSS APPLY STRING_SPLIT(director, ',');

-- create countries table

SELECT show_id, TRIM(value) AS country 
INTO netflix_countries
FROM netflix_shows 
CROSS APPLY STRING_SPLIT(country, ',');

-- create genres table

SELECT show_id, TRIM(value) AS genre 
INTO netflix_genres
FROM netflix_shows 
CROSS APPLY STRING_SPLIT(listed_in, ',');
select * from netflix_shows;

--populate missing values in country,duration columns

insert into netflix_countries
select  show_id,m.country 
from netflix_shows ns
inner join (
select director,country
from  netflix_countries nc
inner join netflix_directors nd on nc.show_id=nd.show_id
group by director,country
) m on ns.director=m.director
where ns.country is null

-- Populate null duration
select * from netflix_shows
where duration is null

with cte as (
select * 
,ROW_NUMBER() over(partition by title , type order by show_id) as rn
from netflix_shows
)
select show_id,type,title,cast(date_added as date) as date_added,release_year
,rating,case when duration is null then rating else duration end as duration,description
into netflix
from cte


-- netflix data analysis
-- count of movies and TV shows for each director who has worked on both

select nd.director 
,COUNT(distinct case when n.type='Movie' then n.show_id end) as total_movies
,COUNT(distinct case when n.type='TV Show' then n.show_id end) as total_tvshows
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
group by nd.director
having COUNT(distinct n.type)>1

-- Most Common Genre in Each Country

WITH GenreRanking AS (
    SELECT 
        nc.country, 
        ng.genre, 
        COUNT(*) AS genre_count,
        RANK() OVER (PARTITION BY nc.country ORDER BY COUNT(*) DESC) AS genre_rank
    FROM netflix n
    INNER JOIN netflix_countries nc ON n.show_id = nc.show_id
    INNER JOIN netflix_genres ng ON n.show_id = ng.show_id
    GROUP BY nc.country, ng.genre
)
SELECT country, genre, genre_count
FROM GenreRanking
WHERE genre_rank = 1
ORDER BY country;

--For each year (as per date added to netflix), which director has maximum number of movies released
with cte as (
select nd.director,YEAR(date_added) as date_year,count(n.show_id) as no_of_movies
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie'
group by nd.director,YEAR(date_added)
)
, cte2 as (
select *
, ROW_NUMBER() over(partition by date_year order by no_of_movies desc, director) as rn
from cte
--order by date_year, no_of_movies desc
)
select * from cte2 where rn=1

-- Average duration of movies in each genre
select ng.genre , avg(cast(REPLACE(duration,' min','') AS int)) as avg_duration
from netflix n
inner join netflix_genres ng on n.show_id=ng.show_id
where type='Movie'
group by ng.genre

--List of directors who have created both horror and comedy movies.
-- display director names along with number of comedy and horror movies directed by them 
select nd.director
, count(distinct case when ng.genre='Comedies' then n.show_id end) as no_of_comedy 
, count(distinct case when ng.genre='Horror Movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_genres ng on n.show_id=ng.show_id
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.director
having COUNT(distinct ng.genre)=2







select * from netflix_shows

