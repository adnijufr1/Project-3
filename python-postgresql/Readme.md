# Steps
1. Run `docker-compose up -d`
2. To look logs run `docker logs <container_name> -f`
3. To execute bash terminal `docker exec -it <container_name> bash`
4. Login to Postgresql CLI `psql -Upostgres`

# LOAD CSV USING COPY FROM
1. Create table youtube_stat
```
CREATE TABLE youtube_stat (
  rank INT,
  Youtuber TEXT,
  subscribers BIGINT,
  video_views FLOAT,
  category TEXT,
  Title TEXT,
  uploads BIGINT,
  Country TEXT,
  Abbreviation TEXT,
  channel_type TEXT,
  video_views_rank FLOAT,
  country_rank FLOAT,
  channel_type_rank FLOAT,
  video_views_for_the_last_30_days FLOAT,
  lowest_monthly_earnings FLOAT,
  highest_monthly_earnings FLOAT,
  lowest_yearly_earnings FLOAT,
  highest_yearly_earnings FLOAT,
  subscribers_for_last_30_days FLOAT,
  created_year FLOAT,
  created_month TEXT,
  created_date FLOAT,
  Gross_tertiary_education_enrollment FLOAT,
  Population FLOAT,
  Unemployment_rate FLOAT,
  Urban_population FLOAT,
  Latitude FLOAT,
  Longitude FLOAT
);
```
2. Using COPY FROM, run command `COPY youtube_stat FROM '/data/global_youtube_stat.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';`, where `'/data/global_youtube_stat.csv'` is directory path to file inside docker container

# Load CSV to Potgres using python
1. Using Python, run command `python load_data.py`

