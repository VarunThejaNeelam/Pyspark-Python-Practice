# Databricks notebook source
# Find the average borrowing duration for each genre.
spark.sql(
    """
    WITH cte AS (
        SELECT b.book_id,
           b.title,
           b.genre,
           DATEDIFF(bb.return_date, bb.borrow_date) as duration
    FROM books b 
    INNER JOIN borrowed_books bb
    ON b.book_id = bb.book_id
    )
    SELECT genre,
           avg(duration) as avg_borrowing_duration
    FROM cte
    GROUP BY genre
    """           
)

# COMMAND ----------

# Find users who have never returned a book.
spark.sql(
    """
    SELECT DISTINCT user_id FROM borrowed_books where return_date is null
    """
)