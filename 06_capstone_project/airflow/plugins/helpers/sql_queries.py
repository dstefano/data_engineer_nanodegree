class SqlQueries:
    immigration_table_insert = ("""
        INSERT INTO immigrations
        SELECT  DISTINCT
                i94.cic_id,
                airport.ident,
                i94.adm_num,
                i94.arrival_date,
                i94.departure_date,
                travel_mode.descrip,
                i94.year,
                i94.month,
                i94.country_code,
                i94.gender,
                visa.descrip,
                i94.visa_type,
                i94.us_state
            FROM staging_i94 i94 
            INNER JOIN staging_airports airport ON i94.us_entry_port = airport.iata_code
            INNER JOIN staging_travel_mode travel_mode ON i94.travel_mode = travel_mode.code
            INNER JOIN staging_visa visa on i94.visa_code = visa.code

    """)

    airport_table_insert = ("""
        INSERT INTO airport
        SELECT distinct ident, airport_type, airport_name, elevation_ft, continent, iso_country,
        iso_region, municipality, gps_code, iata_code, local_code, coordinates
        FROM staging_airports
    """)

    time_table_insert = ("""
        INSERT INTO time
        SELECT arrival_date, extract(hour from arrival_date), extract(day from arrival_date), extract(week from arrival_date), 
               extract(month from arrival_date), extract(year from arrival_date), extract(dayofweek from arrival_date)
        FROM staging_i94 where arrival_date is not null
    """)