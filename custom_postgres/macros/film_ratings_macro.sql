{{ generate_film_ratings() }}



{% macro generate_film_ratings() %}
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
{% endmacros %}



