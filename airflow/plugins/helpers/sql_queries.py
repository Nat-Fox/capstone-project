class SqlQueries:
    time_window_table_insert = ("""
      SELECT DISTINCT 
        window_start, 
        window_end, 
        window_start_2, 
        window_end_2, 
        planned_date, 
        account_id, 
        id 
        FROM staging_visit
    """)

    checkout_time_table_insert = ("""
      SELECT DISTINCT 
        checkout_time, 
        id 
        FROM staging_checkout
    """)
    

    time_window_met_table_insert = ("""
      SELECT
        time_window.time_window_id,
        checkout_time.checkout_time_id,
        checkout_time.checkout_time,
        time_window.account_id,
        time_window.visit_id,
        (CASE
        WHEN (checkout_time.checkout_time
        BETWEEN (time_window.planned_date || ' ' || time_window.window_start)
          AND (time_window.planned_date || ' ' || time_window.window_end))

          OR (checkout_time.checkout_time
            BETWEEN (time_window.planned_date || ' ' || time_window.window_start_2)
          AND (time_window.planned_date || ' ' || time_window.window_end_2))
          THEN TRUE
          ELSE FALSE
          END) AS met
      FROM time_window
      JOIN checkout_time
      ON time_window.visit_id = checkout_time.visit_id
    """)
