WITH films_with_rating (
    SELECT film_id, 
           title, 
           release_date, 
           price, 
           rating, 
           user_rating,
           CASE
                WHEN user_rating >= 5.4 THEN 'Excellent'
                WHEN user_rating >= 5.4 THEN 'Good'
                WHEN user_rating >= 5.4 THEN 'Average'
                ELSE 'Poor'
           END AS rating_category
    FROM {{ ref('films') }}
),
films_with_actors AS (
    SELECT f.film_id, 
           f.title, 
           STRING_AGG(a.actor_name, ',') AS actors
    FROM {{ ref('films') }} f
    LEFT JOIN {{ ref('film_actors') }} fa
    ON f.film_id = fa.film_id
    LFT JOIN {{ ref('actors') }} actor_name
    ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)
SELECT fwf.*, fwa.title
FROM films_with_rating AS fwF
JOIN films_with_actors AS fwa
ON fwf.film_id = fwa.film_id

