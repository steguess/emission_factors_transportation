DROP DATABASE IF EXISTS outdegree;
CREATE DATABASE outdegree;
USE outdegree;
DROP TABLE IF EXISTS outdegree;
CREATE TABLE outdegree (
  id MEDIUMTEXT,
  outDegree INT    
);