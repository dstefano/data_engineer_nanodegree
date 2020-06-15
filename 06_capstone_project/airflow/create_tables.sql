CREATE TABLE public.staging_i94(
    cic_id          int4,
    year            int4,
    month           int4,
    country_code    int4,
    us_entry_port   varchar(5),
    arrival_date    TIMESTAMP,
    travel_mode     int4,
    us_state        varchar(5),
    departure_date  TIMESTAMP,
    age             int4,
    visa_code       int4,
    gender          varchar(5),
    adm_num         bigint,
    visa_type       varchar(5)
);

CREATE TABLE public.staging_airports(
    ident           varchar(50),
    airport_type    varchar(50),            
    airport_name    varchar(100),
    elevation_ft    varchar(100),
    continent       varchar(50),
    iso_country     varchar(50),
    iso_region      varchar(50),
    municipality    varchar(50),
    gps_code        varchar(5),
    iata_code       varchar(5),
    local_code      varchar(5),
    coordinates     varchar(50)    
);

CREATE TABLE public.staging_countries(
    code         varchar(5),
    descrip  varchar(50)            
);

CREATE TABLE public.staging_states(
    code         varchar(5),
    descrip  varchar(50)            
);

CREATE TABLE public.staging_ports(
    code         varchar(5),
    descrip  varchar(50)            
);

-- ok
CREATE TABLE public.staging_visa(
    code      int4,
    descrip  varchar(50)            
);

-- ok
CREATE TABLE public.staging_travel_mode(
    code         varchar(5),
    descrip  varchar(50)            
);

CREATE TABLE public.immigrations(
    cic_id          int4 NOT NULL,
    airport_id      varchar(50) NOT NULL,
    admission_id    varchar(50) NOT NULL,
    arrival_date    TIMESTAMP,
    departure_date  TIMESTAMP,
    travel_mode     varchar(50) NOT NULL,
    year            int4 NOT NULL,
    month           int4 NOT NULL,
    country varchar(50),
    gender varchar(5),
    visa_code varchar(50),
    visa_type varchar(50),
    admission_state varchar(50),
    age             int4,
    PRIMARY KEY(cic_id)
);


CREATE TABLE public.airport(
    airport_id   varchar(500) NOT NULL,
    airport_name varchar(500),
    airport_type varchar(500),            
    elevation_ft varchar(500),
    continent    varchar(500),
    country      varchar(500),
    region       varchar(500),
    city         varchar(500),
    gps_code     varchar(500),
    iata_code    varchar(500),
    local_code   varchar(500),
    coordinates  varchar(500),    
    PRIMARY KEY(airport_id)
);

CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP,
    hour int,
    day int, 
    week int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY(start_time)

);