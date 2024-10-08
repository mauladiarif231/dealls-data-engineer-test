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
        AVG(useful)::INTEGER as useful,
        AVG(funny)::INTEGER as funny,
        AVG(cool)::INTEGER as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date >= TIMESTAMP '2018-01-01' 
    AND date <= TIMESTAMP '2018-01-31'
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
    
    # Scenario 2: First Update (February 1, 2018)
    duckdb.sql("""
    -- Create temporary table for new data
    CREATE TEMPORARY TABLE temp_updates AS
    SELECT 
        business_id,
        AVG(stars) as stars,
        AVG(useful)::INTEGER as useful,
        AVG(funny)::INTEGER as funny,
        AVG(cool)::INTEGER as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date > TIMESTAMP '2018-01-31' 
    AND date <= TIMESTAMP '2018-02-01'
    GROUP BY business_id;

    -- Update existing records
    UPDATE business_reviews AS br
    SET 
        stars = (br.stars + tu.stars) / 2,
        useful = ((br.useful + tu.useful) / 2)::INTEGER,
        funny = ((br.funny + tu.funny) / 2)::INTEGER,
        cool = ((br.cool + tu.cool) / 2)::INTEGER,
        latest_date = CASE 
            WHEN tu.latest_date > br.latest_date THEN tu.latest_date 
            ELSE br.latest_date 
        END
    FROM temp_updates tu
    WHERE br.business_id = tu.business_id;

    -- Insert new records
    INSERT INTO business_reviews
    SELECT tu.*
    FROM temp_updates tu
    LEFT JOIN business_reviews br ON tu.business_id = br.business_id
    WHERE br.business_id IS NULL;

    -- Drop temporary table
    DROP TABLE temp_updates;
    """)
    
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
    
    # Scenario 3: Delayed Update (February 2-13, 2018)
    duckdb.sql("""
    -- Create temporary table for new data
    CREATE TEMPORARY TABLE temp_updates AS
    SELECT 
        business_id,
        AVG(stars) as stars,
        AVG(useful)::INTEGER as useful,
        AVG(funny)::INTEGER as funny,
        AVG(cool)::INTEGER as cool,
        MAX(date) as latest_date
    FROM read_json_auto('source/yelp_academic_dataset_review.json')
    WHERE date > TIMESTAMP '2018-02-01' 
    AND date <= TIMESTAMP '2018-02-13'
    GROUP BY business_id;

    -- Update existing records
    UPDATE business_reviews AS br
    SET 
        stars = (br.stars + tu.stars) / 2,
        useful = ((br.useful + tu.useful) / 2)::INTEGER,
        funny = ((br.funny + tu.funny) / 2)::INTEGER,
        cool = ((br.cool + tu.cool) / 2)::INTEGER,
        latest_date = CASE 
            WHEN tu.latest_date > br.latest_date THEN tu.latest_date 
            ELSE br.latest_date 
        END
    FROM temp_updates tu
    WHERE br.business_id = tu.business_id;

    -- Insert new records
    INSERT INTO business_reviews
    SELECT tu.*
    FROM temp_updates tu
    LEFT JOIN business_reviews br ON tu.business_id = br.business_id
    WHERE br.business_id IS NULL;

    -- Drop temporary table
    DROP TABLE temp_updates;
    """)
    
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