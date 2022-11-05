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

DROP TABLE IF EXISTS trading_countries;
CREATE TABLE trading_countries (
  country VARCHAR(5),
  co2emission INT,
  amount FLOAT,
  trading_Country VARCHAR(5),
  in_out VARCHAR(5),
  flow_type VARCHAR(30)
);



DROP TABLE IF EXISTS targets;
CREATE TABLE targets (
  source VARCHAR(10),
  source_type VARCHAR(10),
  co2emission INT,
  amount FLOAT,
  category VARCHAR(30),
  in_outbound VARCHAR(10),
  destination VARCHAR(10),
  dest_type VARCHAR(20)
);
DROP TABLE IF EXISTS source;
CREATE TABLE source (
  source VARCHAR(10),
  source_type VARCHAR(10),
  co2emission INT,
  amount FLOAT,
  category VARCHAR(30),
  tn_outbound VARCHAR(10),
  destination VARCHAR(10),
  dest_type VARCHAR(20)
);


DROP TABLE IF EXISTS outdegree;
CREATE TABLE outdegree (
  id VARCHAR(15),
  outDegree INT,
  PRIMARY KEY(id)
);


DROP TABLE IF EXISTS indegree;
CREATE TABLE indegree (
  id VARCHAR(15),
  inDegree INT,
  PRIMARY KEY(id)
);


DROP TABLE IF EXISTS communities;
CREATE TABLE communities (
  id VARCHAR(15),
  label BIGINT,
  PRIMARY KEY(id)
);

DROP TABLE IF EXISTS intercompany;
CREATE TABLE intercompany (
  SiteA VARCHAR(20),
  atype VARCHAR(20),
  ab_C02Emission INT,
  ab_Amount INT,
  ab_Tech VARCHAR(20),
  ab_in_outbound INT,
  SiteB VARCHAR(20),
  btype VARCHAR(20),
  ba_C02Emission INT,
  ba_Amount INT,
  ba_Tech VARCHAR(20),
  CombinedCO2 INT,
  CombinedCO2_KG INT
);
