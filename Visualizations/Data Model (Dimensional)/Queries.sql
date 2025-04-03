--  Total Number of Reviews by Product
SELECT 
    dp.product_name, 
    COUNT(fr.fact_id) AS total_reviews
FROM fact_reviews_product_summary fr
JOIN dim_product dp ON fr.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY total_reviews DESC;

-- Average Rating by Product
SELECT 
    dp.product_name, 
    ROUND(AVG(fr.rate), 2) AS avg_rating
FROM fact_reviews_product_summary fr
JOIN dim_product dp ON fr.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY avg_rating DESC;

-- Sentiment Analysis of Reviews
SELECT 
    sentiment, 
    COUNT(fact_id) AS total_reviews
FROM fact_reviews_product_summary
GROUP BY sentiment
ORDER BY total_reviews DESC;

-- Top 5 Most Expensive Products with Ratings
SELECT 
    dp.product_name, 
    dp.product_price, 
    ROUND(AVG(fr.rate), 2) AS avg_rating
FROM fact_reviews_product_summary fr
JOIN dim_product dp ON fr.product_id = dp.product_id
GROUP BY dp.product_name, dp.product_price
ORDER BY dp.product_price DESC
LIMIT 5;

-- Number of Reviews by Rating (Star Ratings)
SELECT 
    rate AS star_rating, 
    COUNT(fact_id) AS total_reviews
FROM fact_reviews_product_summary
GROUP BY rate
ORDER BY star_rating DESC;

--  Sentiment Breakdown by Product
SELECT 
    dp.product_name, 
    fr.sentiment, 
    COUNT(fr.fact_id) AS total_reviews
FROM fact_reviews_product_summary fr
JOIN dim_product dp ON fr.product_id = dp.product_id
GROUP BY dp.product_name, fr.sentiment
ORDER BY dp.product_name, fr.sentiment;

--  Price Distribution of Products
SELECT 
    product_price, 
    COUNT(product_id) AS product_count
FROM dim_product
GROUP BY product_price
ORDER BY product_price;

--  Products with the Most Positive Reviews
SELECT 
    dp.product_name, 
    COUNT(fr.fact_id) AS positive_reviews
FROM fact_reviews_product_summary fr
JOIN dim_product dp ON fr.product_id = dp.product_id
WHERE fr.sentiment = 'positive'
GROUP BY dp.product_name
ORDER BY positive_reviews DESC;

