{% SET movie_title='Dunkirk' %}
SELECT * 
FROM {{ ref('films') }} 
WHERE title = '{{ movie_title }}'