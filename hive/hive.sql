-- Run with: hive -f hive.sql

--------------------------------------------------------------------------------
-- Create schema and load data to tables ---------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE escuelas_pr(
  region      STRING,
  distrito    STRING,
  ciudad      STRING,
  id_escuela  INT,
  nombre      STRING,
  nivel       STRING,
  num_serie   INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH "./escuelasPR.csv" INTO TABLE escuelas_pr;

CREATE TABLE students_pr(
  region          STRING,
  distrito        STRING,
  id_escuela      INT,
  nombre_escuela  STRING,
  nivel           STRING,
  sexo            CHAR(1),
  id_estudiante   INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH "./studentsPR.csv" INTO TABLE students_pr;

--------------------------------------------------------------------------------
-- Hive-QL queries -------------------------------------------------------------
--------------------------------------------------------------------------------

-- 1) Find the total number of schools in each region, and city.
SELECT region, ciudad, count(*)
FROM escuelas_pr
GROUP BY region, ciudad;

-- 2) Find the total number of students per school.
SELECT id_escuela, count(*)
FROM students_pr
GROUP BY id_escuela;

-- 3) Find all the students that go to school in the city of Ponce or in Cabo Rojo.
SELECT id_estudiante
FROM students_pr as S, escuelas_pr as E
WHERE S.id_escuela = E.id_escuela
AND (ciudad = "Ponce" OR ciudad = "Cabo Rojo");

-- 4) Find the total number of students per region and city.
SELECT E.region, E.ciudad, count(*)
FROM students_pr as S, escuelas_pr as E
WHERE S.id_escuela = E.id_escuela
GROUP BY E.region, E.ciudad;
