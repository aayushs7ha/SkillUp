
-- Copying csv in a docker container for execution. 

/* (base) maverick@Aayushs-MacBook-Pro ~ % docker cp /Users/maverick/Downloads/BostonCrime/crime.csv sql:/var/opt/mssql/data/crime.csv 
                             Successfully copied 58MB to sql:/var/opt/mssql/data/crime.csv
(base) maverick@Aayushs-MacBook-Pro ~ % docker cp /Users/maverick/Downloads/BostonCrime/offense_codes.csv sql:/var/opt/mssql/data/offense_codes.csv 
                                             Successfully copied 21kB to sql:/var/opt/mssql/data/offense_codes.csv */



CREATE TABLE crime (
    INCIDENT_NUMBER VARCHAR(255),
    OFFENSE_CODE INT, -- Removed PRIMARY KEY constraint
    OFFENSE_CODE_GROUP VARCHAR(255),
    OFFENSE_DESCRIPTION VARCHAR(255),
    DISTRICT VARCHAR(255),
    REPORTING_AREA VARCHAR(255),
    OCCURRED_ON_DATE TIMESTAMP,
    YEAR VARCHAR(255),
    MONTH VARCHAR(255),
    DAY_OF_WEEK VARCHAR(255),
    HOUR VARCHAR(255),
    UCR_PART VARCHAR(255),
    STREET VARCHAR(255),
    Lat VARCHAR(255),
    Long VARCHAR(255)
);

CREATE TABLE offense_codes (
    CODE INT,
    NAME VARCHAR(255)
);
BULK INSERT crime
FROM '/var/opt/mssql/data/crimes.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2 -- Skips the header row
);
--drop table crime;


BULK INSERT offense_codes
FROM '/var/opt/mssql/data/offense_codes.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2 -- Skips the header row
);

select * from offense_codes;

select * from crime;



-- CTE to categorize incidents by Phase of Day and Month
-- CTE to categorize incidents by Phase of Day and Month
    WITH CategorizedData AS (
        SELECT
            CASE
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 6 AND 10 THEN 'Morning'
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 11 AND 16 THEN 'Noon'
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 17 AND 19 THEN 'Evening'
                ELSE 'Night'
            END AS PhaseOfDay,
            DATENAME(MONTH, OCCURRED_ON_DATE) AS MonthName,
            OFFENSE_DESCRIPTION
        FROM
            crime
    )

    -- Query to pivot data and calculate counts
    SELECT
        PhaseOfDay,
        SUM(CASE WHEN MonthName = 'January' THEN 1 ELSE 0 END) AS Jan,
        SUM(CASE WHEN MonthName = 'February' THEN 1 ELSE 0 END) AS Feb,
        SUM(CASE WHEN MonthName = 'March' THEN 1 ELSE 0 END) AS Mar,
        SUM(CASE WHEN MonthName = 'April' THEN 1 ELSE 0 END) AS Apr,
        SUM(CASE WHEN MonthName = 'May' THEN 1 ELSE 0 END) AS May,
        SUM(CASE WHEN MonthName = 'June' THEN 1 ELSE 0 END) AS Jun,
        SUM(CASE WHEN MonthName = 'July' THEN 1 ELSE 0 END) AS Jul,
        SUM(CASE WHEN MonthName = 'August' THEN 1 ELSE 0 END) AS Aug,
        SUM(CASE WHEN MonthName = 'September' THEN 1 ELSE 0 END) AS Sep,
        SUM(CASE WHEN MonthName = 'October' THEN 1 ELSE 0 END) AS Oct,
        SUM(CASE WHEN MonthName = 'November' THEN 1 ELSE 0 END) AS Nov,
        SUM(CASE WHEN MonthName = 'December' THEN 1 ELSE 0 END) AS Dec
    FROM (
        SELECT
            CASE
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 6 AND 10 THEN 'Morning'
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 11 AND 16 THEN 'Noon'
                WHEN DATEPART(HOUR, OCCURRED_ON_DATE) BETWEEN 17 AND 19 THEN 'Evening'
                ELSE 'Night'
            END AS PhaseOfDay,
            DATENAME(MONTH, OCCURRED_ON_DATE) AS MonthName,
            OFFENSE_DESCRIPTION
        FROM
            crime
    ) AS PhaseMonthData
    GROUP BY
        PhaseOfDay
    ORDER BY
        CASE
            WHEN PhaseOfDay = 'Morning' THEN 1
            WHEN PhaseOfDay = 'Noon' THEN 2
            WHEN PhaseOfDay = 'Evening' THEN 3
            WHEN PhaseOfDay = 'Night' THEN 4
        END;

--SELECT * from offense_codes;
-- drop TABLE offense_codes;


BULK INSERT offense_codes
FROM '/var/opt/mssql/data/offense_codes.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2 -- Skips the header row
);
/*
select * FROM offense_codes;

ALTER TABLE offense_codes
ALTER COLUMN code INT;

SELECT *
FROM offense_codes
WHERE TRY_CAST(code AS INT) IS NULL;

UPDATE offense_codes
SET code = TRY_CAST(code AS INT)
WHERE TRY_CAST(code AS INT) IS NULL;
*/

--2
WITH crime_offense_counts AS (
  SELECT
    c.DISTRICT,
    oc.NAME AS full_offense_name,
    COUNT(*) AS offense_count
  FROM crime c
  JOIN offense_codes oc ON c.OFFENSE_CODE = oc.CODE
  GROUP BY c.DISTRICT, oc.NAME
),

district_max_offense AS (
  SELECT
    DISTRICT,
    full_offense_name,
    MAX(offense_count) AS max_count
  FROM crime_offense_counts
  GROUP BY DISTRICT, full_offense_name
)

SELECT
  dmo.DISTRICT,
  dmo.full_offense_name,
  coc.offense_count
FROM district_max_offense dmo
JOIN crime_offense_counts coc ON dmo.DISTRICT = coc.DISTRICT
  AND dmo.full_offense_name = coc.full_offense_name
  AND dmo.max_count = coc.offense_count;


--3 

SELECT c.*,
  (
    SELECT MAX(OCCURRED_ON_DATE)
    FROM crime AS prev_crime
    WHERE prev_crime.DISTRICT = c.DISTRICT
      AND prev_crime.OCCURRED_ON_DATE < c.OCCURRED_ON_DATE
  ) AS DATE_OF_LAST_INCIDENT
FROM crime AS c
ORDER BY c.DISTRICT, c.OCCURRED_ON_DATE;



--4 

/*
-- Create table for users
CREATE TABLE iUsers (
    UserID INT,
    Name VARCHAR(255)
);

-- Create table for user programs
CREATE TABLE iUserProgram (
    UserID INT,
    ProgramID INT
);

-- Create table for programs
CREATE TABLE iPrograms (
    ProgramID INT,
    ProgramName VARCHAR(255)
);

-- Create table for miscellaneous fields
CREATE TABLE iMiscFields (
    UserID INT
    */
--4 
SELECT u.UserID, u.Name, p.ProgramName, mf.FieldValue
FROM iUsers AS u
LEFT JOIN iUserProgram AS up ON up.UserID = u.UserID
LEFT JOIN iPrograms AS p ON up.ProgramID = p.ProgramID
LEFT JOIN iMiscFields AS mf ON mf.UserID = u.UserID AND mf.ProgramID = p.ProgramID;


--5
SELECT p.ProgramName, u.UserID
FROM iPrograms AS p
LEFT JOIN iUserProgram AS up ON up.ProgramID = p.ProgramID
LEFT JOIN iUsers AS u ON u.UserID = up.UserID;
