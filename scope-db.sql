DROP DATABASE IF EXISTS scope3;
CREATE DATABASE scope3;
USE scope3;
DROP TABLE IF EXISTS countries;
CREATE TABLE countries (
  country VARCHAR(2),
  region VARCHAR(15),
  lat FLOAT,
  lon FLOAT,
  distance INT,
  co2emission INT,
  percentile FLOAT,
  size FLOAT,
  PRIMARY KEY(country)  
);

DROP TABLE IF EXISTS flows;
CREATE TABLE flows (
  origin VARCHAR(2),
  target VARCHAR(2),
  distance INT,
  co2emission INT,
  origin_lat FLOAT,
  origin_lon FLOAT,
  target_lat FLOAT,
  target_lon FLOAT,
  percentile FLOAT,
  size FLOAT 
);