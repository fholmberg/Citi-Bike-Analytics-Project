-- Please find link to Google Cloud Project:
-- https://console.cloud.google.com/bigquery?sq=686629253015:658a848dc42641bbb479f4a178c570dd&project=takehometest-349914

-- Please find the code associated with the Project:
-- ALL SQL CODE should be executed in the order of this file.
  -- PART 1) Combining and Normalizing Multiple Similar Datasets
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- SF Bikeshare Datasets (`bigquery-public-data.san_francisco.bikeshare_trips` and `bigquery-public-data.san_francisco.bikeshare_stations`)
  -- SF Trips Data table: `bigquery-public-data.san_francisco.bikeshare_trips`
  -- Actions taken to prepare/clean the data:
  -- convert timestamps from Datetime UTC to PST for start date and end date
  -- Exclude the tripID, and customer Zip Code columns as the NY Dataset doesn't have that data collected on its customers
  -- SF Stations data table: `bigquery-public-data.san_francisco.bikeshare_stations`
  -- This data table have a lot of attributes that aren't relevant to the trips data table
  -- however, there are few columns that we could leverage to enhance the trips data table, which I will join over
  -- through a LEFT JOIN on start_station_id and then another LEFT JOIN on end_station_id
  -- Columns that I will join over to the trips data table to enhance the data are the following:
  -- longitude (for start station and end station)
  -- latitude (for start station and end station)
  -- dockcount (i.e., station capacity) for (for start station and end station)
  -- landmark (for start station and end station)
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- NY Bikeshare Datasets (`bigquery-public-data.new_york_citibike.citibike_trips` and `bigquery-public-data.new_york_citibike.citibike_stations`)
  -- NYC Trips Data table: `bigquery-public-data.new_york_citibike.citibike_trips`
  -- Actions taken to prepare/clean the data:
  -- Exclude gender and birth year columns as these are not compatible with the SF dataset (no data collected for these attributes in SF dataset)
  -- NYC Stations Data table: `bigquery-public-data.new_york_citibike.citibike_stations`
  -- This data table have a lot of attributes that aren't relevant to the trips data table
  -- however, there are few columns that we could leverage to enhance the trips data table, which I will join over
  -- through a LEFT JOIN on start_station_id and then another LEFT JOIN on end_station_id
  -- Columns that I will join over to the trips data table to enhance the data are the following:
  -- capacity for (for start station and end station)
  -- create a landmark column for start station and end station by using information from the short_name column in the NYC stations dataset
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  --  Combine SF bikeshare data with NYC bikeshare data using UNION ALL and create a VIEW called `takehometest-349914.takehometest.NYC_SF_data_stacked`
  --  Had to store the data as a VIEW instead of a TABLE due to memory restrictions when being a free consumer of GCP
CREATE OR REPLACE VIEW
  `takehometest-349914.takehometest.NYC_SF_data_stacked` AS
SELECT
  duration_sec AS trip_duration,
  DATE(start_date, "America/Los_Angeles") AS start_date,
  start_station_name,
  start_station_id,
  stations_start.latitude AS start_station_latitude,
  stations_start.longitude AS start_station_longitude,
  stations_start.dockcount AS start_station_capacity,
  stations_start.landmark AS start_station_landmark,
  DATE(end_date,"America/Los_Angeles") AS end_date,
  end_station_name,
  end_station_id,
  stations_end.latitude AS end_station_latitude,
  stations_end.longitude AS end_station_longitude,
  stations_end.dockcount AS end_station_capacity,
  stations_end.landmark AS end_station_landmark,
  bike_number,
  subscriber_type AS user_type
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips` AS trips
LEFT JOIN
  `bigquery-public-data.san_francisco.bikeshare_stations` AS stations_start -- left join to add lat, long, and landmark attributes for Start station to trips table
ON
  trips.start_station_id = stations_start.station_id
LEFT JOIN
  `bigquery-public-data.san_francisco.bikeshare_stations` AS stations_end -- left join to add lat, long, and landmark attributes for End station to trips table
ON
  trips.end_station_id = stations_end.station_id
UNION ALL
  -- use union all to stack the data in the same table
SELECT
  tripduration AS trip_duration,
  starttime AS start_date,
  start_station_name,
  start_station_id,
  start_station_latitude,
  start_station_longitude,
  stations_start.capacity AS start_station_capacity,
  CASE
    WHEN stations_start.short_name LIKE '%HB%' THEN 'Hoboken'
    WHEN stations_start.short_name LIKE '%JC%' THEN 'Jersey City'
  ELSE
  'New York City'
END
  AS start_station_landmark,
  stoptime AS end_date,
  end_station_name,
  end_station_id,
  end_station_latitude,
  end_station_longitude,
  stations_end.capacity AS end_station_capacity,
  CASE
    WHEN stations_end.short_name LIKE '%HB%' THEN 'Hoboken'
    WHEN stations_end.short_name LIKE '%JC%' THEN 'Jersey City'
  ELSE
  'New York City'
END
  AS end_station_landmark,
  bikeid AS bike_number,
  usertype AS user_type
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips` AS trips
LEFT JOIN
  `bigquery-public-data.new_york_citibike.citibike_stations` AS stations_start
ON
  CAST(trips.start_station_id AS INT64) = CAST(stations_start.station_id AS INT64)
LEFT JOIN
  `bigquery-public-data.new_york_citibike.citibike_stations` AS stations_end
ON
  CAST(trips.end_station_id AS INT64) = CAST(stations_end.station_id AS INT64) ;
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- UNIFORM SCHEMA (`takehometest-349914.takehometest.NYC_SF_data_stacked`)
  -- | FIELD NAME             | TYPE              | DESCRIPTION
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- | trip_duration          | INTEGER           | Trip Duration (in seconds)
  -- | start_date             | DATETIME          | Start Time (local time) (EST for NYC, PST for SF)
  -- | start_station_name     | STRING            | Start Station Name
  -- | start_station_id       | INTEGER           | Start Station ID
  -- | start_station_latitude | FLOAT             | Start Station Latitude
  -- | start_station_longitude| FLOAT             | Start Station Longitude
  -- | start_station_capacity | INTEGER           | Total number docking points installed at the start station, both available and unavailable.
  -- | start_station_landmark | STRING            | City (San Francisco, Redwood City, Palo Alto, Mountain View, San Jose, 'New York City', 'Hoboken', 'Jersey City')
  -- | end_date               | DATETIME          | End Time (local time) (EST for NYC, PST for SF)
  -- | end_station_name       | STRING            | End Station Name
  -- | end_station_id         | INTEGER           | End Station ID
  -- | end_station_latitude   | FLOAT             | End Station Latitude
  -- | end_station_longitude  | FLOAT             | End Station Longitude
  -- | end_station_capacity   | INTEGER           | Total number docking points installed at the end station, both available and unavailable.
  -- | end_station_landmark   | STRING            | City (San Francisco, Redwood City, Palo Alto, Mountain View, San Jose, 'New York City', 'Hoboken', 'Jersey City')
  -- | bike_number            | INTEGER           | Bike ID
  -- | user_type              | STRING            | User Type (Subscriber or Customer)
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- PART 2) Attaching Additional Attributes
  -- To enhance the bikeshare data with Census attributes I levered the `bigquery-public-data.geo_us_boundaries.zip_codes` data table
  -- Step 1)
  -- Here I did reverse geocoding on latitude and longitude to generate zip codes for the Bike stations
  -- stored the data in a VIEW called `takehometest-349914.takehometest.NYC_SF_data_stacked_enhanced`
CREATE VIEW
  `takehometest-349914.takehometest.NYC_SF_data_stacked_enhanced` AS
SELECT
  trip_duration,
  start_date,
  start_station_name,
  start_station_id,
  start_station_latitude,
  start_station_longitude,
  start_station_capacity,
  start_station_landmark,
  zipcodes_start.zip_code AS start_station_zip_code,
  end_date,
  end_station_name,
  end_station_id,
  end_station_latitude,
  end_station_longitude,
  end_station_capacity,
  end_station_landmark,
  zipcodes_end.zip_code AS end_station_zip_code,
  bike_number,
  user_type
FROM
  `takehometest-349914.takehometest.NYC_SF_data_stacked` AS trips
JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` AS zipcodes_start
ON
  (ST_CONTAINS( zipcodes_start.zip_code_geom, ST_GEOGPOINT(trips.start_station_longitude, trips.start_station_latitude)) )
JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` AS zipcodes_end
ON
  (ST_CONTAINS( zipcodes_end.zip_code_geom, ST_GEOGPOINT(trips.end_station_longitude, trips.end_station_latitude)) ) ;
  -- Step 2)
  -- To further enhance the data I will join over certain attributes from the ACS Census Bureau data collection on Zip codes from 2018
  -- (`bigquery-public-data.census_bureau_acs.zip_codes_2018_5yr`)
  -- To add these columns I performed a INNER JOIN on zip_code = geo_id (for both start station and end station)
  -- The columns I joined over were:
  -- total_pop (population in a zip code) (for both start and end station)
  -- no_cars (# of people in the zip code that don't own a car or cars) (for both start and end station)
  -- pop_16_over (# of people in the zip code that over 16 years old) (for both start and end station)
CREATE OR REPLACE VIEW
  `takehometest-349914.takehometest.NYC_SF_data_stacked_enhanced_census` AS
SELECT
  trip_duration,
  start_date,
  start_station_name,
  start_station_id,
  start_station_latitude,
  start_station_longitude,
  start_station_capacity,
  start_station_landmark,
  start_station_zip_code,
  end_date,
  end_station_name,
  end_station_id,
  end_station_latitude,
  end_station_longitude,
  end_station_capacity,
  end_station_landmark,
  end_station_zip_code,
  bike_number,
  user_type,
  zipcodes_start.total_pop AS start_station_zip_code_total_pop,
  zipcodes_end.total_pop AS end_station_zip_code_total_pop,
  zipcodes_start.no_car AS start_station_no_car,
  zipcodes_end.no_car AS end_station_no_car,
  zipcodes_start.pop_16_over AS start_station_pop_16_over,
  zipcodes_end.pop_16_over AS end_station_pop_16_over
FROM
  `takehometest-349914.takehometest.NYC_SF_data_stacked_enhanced` AS trips
INNER JOIN
  `bigquery-public-data.census_bureau_acs.zip_codes_2018_5yr` AS zipcodes_start
ON
  trips.start_station_zip_code = zipcodes_start.geo_id
INNER JOIN
  `bigquery-public-data.census_bureau_acs.zip_codes_2018_5yr` AS zipcodes_end
ON
  trips.end_station_zip_code = zipcodes_end.geo_id;
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- FINAL UNIFORM SCHEMA (`takehometest-349914.takehometest.NYC_SF_data_stacked_enhanced_census`)
  -- | FIELD NAME                         | TYPE              | DESCRIPTION
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  -- | trip_duration                      | INTEGER           | Trip Duration (in seconds)
  -- | start_date                         | DATETIME          | Start Time (local time) (EST for NYC, PST for SF)
  -- | start_station_name                 | STRING            | Start Station Name
  -- | start_station_id                   | INTEGER           | Start Station ID
  -- | start_station_latitude             | FLOAT             | Start Station Latitude
  -- | start_station_longitude            | FLOAT             | Start Station Longitude
  -- | start_station_capacity             | INTEGER           | Total number docking points installed at the start station, both available and unavailable.
  -- | start_station_landmark             | STRING            | City (San Francisco, Redwood City, Palo Alto, Mountain View, San Jose, 'New York City', 'Hoboken', 'Jersey City')
  -- | end_date                           | DATETIME          | End Time (local time) (EST for NYC, PST for SF)
  -- | end_station_name                   | STRING            | End Station Name
  -- | end_station_id                     | INTEGER           | End Station ID
  -- | end_station_latitude               | FLOAT             | End Station Latitude
  -- | end_station_longitude              | FLOAT             | End Station Longitude
  -- | end_station_capacity               | INTEGER           | Total number docking points installed at the end station, both available and unavailable.
  -- | end_station_landmark               | STRING            | City (San Francisco, Redwood City, Palo Alto, Mountain View, San Jose, 'New York City', 'Hoboken', 'Jersey City')
  -- | bike_number                        | INTEGER           | Bike ID
  -- | user_type                          | STRING            | User Type (Subscriber or Customer)
  -- | start_station_zip_code_total_pop   | STRING            | Total population by Zip Code (Start Station)
  -- | end_station_zip_code_total_pop     | STRING            | Total population by Zip Code (End Station)
  -- | start_station_no_car               | FLOAT             | All people in a geographic area over the age of 16 who do not own a car.
  -- | end_station_no_car                 | FLOAT             | All people in a geographic area over the age of 16 who do not own a car.
  -- | start_station_pop_16_over          | FLOAT             | Total population by Zip Code over the age of 16 (Start Station)
  -- | end_station_pop_16_over            | FLOAT             | Total population by Zip Code over the age of 16 (End Station)
  ---------------------------------------------------------------------------------------------------------------------------------------------------