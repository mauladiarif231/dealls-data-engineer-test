import duckdb

def etl():
    # Create Table
    duckdb.sql("""
    CREATE OR REPLACE TABLE business_reviews (
        business_id VARCHAR,
        stars DOUBLE,
        useful INTEGER,
        funny INTEGER,
        cool INTEGER,
        latest_date TIMESTAMP,
        PRIMARY KEY (business_id)
    )
    """)

    # Scenario 1: Initial Load
    duckdb.sql("""
    INSERT INTO business_reviews
    SELECT 
        business_id,
        AVG(stars) as stars,
        SUM(useful) as useful,
        SUM(funny) as funny,
        SUM(cool) as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date >= TIMESTAMP '2018-01-01 00:00:00' 
    AND date <= TIMESTAMP '2018-01-31 23:59:59'
    GROUP BY business_id
    """)

    # Check metrics after Scenario 1
    print("Metrics after Initial Load:")
    duckdb.sql("""
    SELECT 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool, 
        MAX(latest_date) AS latest_date 
    FROM business_reviews
    """).show()

    print("\nDetailed Metrics per Date after Initial Load:")
    duckdb.sql("""
    SELECT 
        latest_date::date AS date, 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool
    FROM business_reviews
    GROUP BY latest_date::date
    ORDER BY date
    """).show()

    # Scenario 2: First Update (2 February 2018)
    # Create temporary table for new data
    duckdb.sql("""
    CREATE TEMPORARY TABLE temp_new_data AS
    SELECT 
        business_id,
        AVG(stars) as stars,
        SUM(useful) as useful,
        SUM(funny) as funny,
        SUM(cool) as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date > TIMESTAMP '2018-01-31 23:59:59'
    AND date <= TIMESTAMP '2018-02-02 00:00:00'
    GROUP BY business_id
    """)

    # Update existing records
    duckdb.sql("""
    UPDATE business_reviews AS target
    SET 
        stars = (target.stars + source.stars) / 2,
        useful = target.useful + source.useful,
        funny = target.funny + source.funny,
        cool = target.cool + source.cool,
        latest_date = CASE 
            WHEN source.latest_date > target.latest_date THEN source.latest_date 
            ELSE target.latest_date 
        END
    FROM temp_new_data AS source
    WHERE target.business_id = source.business_id
    """)

    # Insert new records
    duckdb.sql("""
    INSERT INTO business_reviews
    SELECT source.*
    FROM temp_new_data AS source
    WHERE NOT EXISTS (
        SELECT 1 
        FROM business_reviews AS target 
        WHERE target.business_id = source.business_id
    )
    """)

    # Drop temporary table
    duckdb.sql("DROP TABLE temp_new_data")

    # Check metrics after Scenario 2
    print("\nMetrics after First Update:")
    duckdb.sql("""
    SELECT 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool, 
        MAX(latest_date) AS latest_date 
    FROM business_reviews
    """).show()

    print("\nDetailed Metrics per Date after First Update:")
    duckdb.sql("""
    SELECT 
        latest_date::date AS date, 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool
    FROM business_reviews
    GROUP BY latest_date::date
    ORDER BY date
    """).show()

    # Scenario 3: Delayed Update (14 February 2018)
    # Create temporary table for new data
    duckdb.sql("""
    CREATE TEMPORARY TABLE temp_new_data AS
    SELECT 
        business_id,
        AVG(stars) as stars,
        SUM(useful) as useful,
        SUM(funny) as funny,
        SUM(cool) as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date > TIMESTAMP '2018-02-02 00:00:00'
    AND date <= TIMESTAMP '2018-02-14 00:00:00'
    GROUP BY business_id
    """)

    # Update existing records
    duckdb.sql("""
    UPDATE business_reviews AS target
    SET 
        stars = (target.stars + source.stars) / 2,
        useful = target.useful + source.useful,
        funny = target.funny + source.funny,
        cool = target.cool + source.cool,
        latest_date = CASE 
            WHEN source.latest_date > target.latest_date THEN source.latest_date 
            ELSE target.latest_date 
        END
    FROM temp_new_data AS source
    WHERE target.business_id = source.business_id
    """)

    # Insert new records
    duckdb.sql("""
    INSERT INTO business_reviews
    SELECT source.*
    FROM temp_new_data AS source
    WHERE NOT EXISTS (
        SELECT 1 
        FROM business_reviews AS target 
        WHERE target.business_id = source.business_id
    )
    """)

    # Drop temporary table
    duckdb.sql("DROP TABLE temp_new_data")

    # Check metrics after Scenario 3
    print("\nMetrics after Delayed Update:")
    duckdb.sql("""
    SELECT 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool, 
        MAX(latest_date) AS latest_date 
    FROM business_reviews
    """).show()

    print("\nDetailed Metrics per Date after Delayed Update:")
    duckdb.sql("""
    SELECT 
        latest_date::date AS date, 
        count(*) AS row_count, 
        SUM(stars) AS sum_stars, 
        SUM(useful) AS sum_useful, 
        SUM(funny) AS sum_funny, 
        SUM(cool) AS sum_cool
    FROM business_reviews
    GROUP BY latest_date::date
    ORDER BY date
    """).show()

etl()
