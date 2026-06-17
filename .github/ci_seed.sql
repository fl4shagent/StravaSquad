-- CI seed: creates StravaDW database, schemas, and minimal test data
-- for dbt run + dbt test in GitHub Actions.

CREATE DATABASE StravaDW;
GO
USE StravaDW;
GO

CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

-- Silver tables (needed by dbt models)
CREATE TABLE silver.Activities (
    activity_id     BIGINT NOT NULL PRIMARY KEY,
    activity_type   NVARCHAR(30),
    athlete_id      BIGINT,
    athlete_name    NVARCHAR(100),
    date            DATE,
    start_time_utc  DATETIME2,
    duration_sec    INT,
    distance_km     FLOAT,
    elev_gain_m     FLOAT,
    avg_pace_sec_km FLOAT,
    source          NVARCHAR(50),
    hr_avg          FLOAT,
    hr_max          INT,
    cad_avg         FLOAT,
    cad_max         INT,
    elev_min        FLOAT,
    elev_max        FLOAT,
    total_ascent    FLOAT,
    total_descent   FLOAT,
    stride_len_m    FLOAT
);
GO

CREATE TABLE silver.RunStream (
    activity_id BIGINT NOT NULL,
    time_s      INT NOT NULL,
    lat         FLOAT,
    lon         FLOAT,
    dist_m      FLOAT,
    alt_m       FLOAT,
    hr_bpm      SMALLINT,
    cadence     SMALLINT,
    watts       INT,
    CONSTRAINT PK_silver_RunStream PRIMARY KEY (activity_id, time_s)
);
GO

-- Silver tables referenced as sources but not read by dbt models
CREATE TABLE silver.Athlete (
    athlete_id BIGINT NOT NULL PRIMARY KEY,
    city NVARCHAR(50), country NVARCHAR(50),
    firstname NVARCHAR(20), lastname NVARCHAR(20),
    sex NVARCHAR(2), weight INT
);
GO
CREATE TABLE silver.RunBest (
    id BIGINT NOT NULL PRIMARY KEY,
    name NVARCHAR(30), elapsed_time INT, moving_time INT,
    start_date DATETIME2, distance INT,
    activity_id BIGINT, athlete_id BIGINT
);
GO
CREATE TABLE silver.RunSegment (
    id BIGINT NOT NULL PRIMARY KEY,
    name NVARCHAR(200), elapsed_time INT, moving_time INT,
    start_date DATETIME2, start_date_local DATETIME2,
    distance FLOAT, start_index INT, end_index INT,
    average_cadence FLOAT, average_watts FLOAT,
    average_heartrate FLOAT, max_heartrate INT,
    activity_id BIGINT, athlete_id BIGINT
);
GO
CREATE TABLE silver.RunSplitKilometer (
    activity_id BIGINT NOT NULL,
    segment_number INT NOT NULL,
    segment_start_time INT, segment_end_time INT,
    start_lat FLOAT, start_lon FLOAT,
    cumulative_distance_m FLOAT, avg_hr_bpm FLOAT,
    avg_cadence FLOAT, avg_watts FLOAT,
    segment_distance_m FLOAT, duration_s INT,
    pace_sec_per_km FLOAT,
    CONSTRAINT PK_silver_RSK PRIMARY KEY (activity_id, segment_number)
);
GO

-- Gold prediction tables (written by Python, tested by dbt)
CREATE TABLE gold.PBPrediction (
    athlete_id BIGINT NOT NULL,
    distance_label NVARCHAR(30) NOT NULL,
    predicted_best_sec FLOAT,
    prediction_date DATE NOT NULL,
    r_squared FLOAT
);
GO
CREATE TABLE gold.RaceForecast (
    athlete_id BIGINT NOT NULL,
    race_activity_id BIGINT,
    target_distance_label NVARCHAR(30) NOT NULL,
    method NVARCHAR(30) NOT NULL,
    as_of_date DATE NOT NULL,
    source_distance_label NVARCHAR(30),
    source_pb_sec FLOAT,
    predicted_sec FLOAT,
    actual_sec FLOAT,
    error_sec FLOAT,
    error_pct FLOAT
);
GO

-- Minimal test data for silver (dbt models need rows to produce output)
INSERT INTO silver.Activities VALUES
    (1001, 'Run', 100, 'Test Runner', '2025-10-01', '2025-10-01 08:00:00', 3600, 10.0, 50.0, 360, 'strava', 150, 175, 85, 95, 10, 60, 50, 50, 1.2),
    (1002, 'Run', 100, 'Test Runner', '2025-10-02', '2025-10-02 08:00:00', 1800, 5.0, 25.0, 360, 'strava', 155, 180, 87, 97, 15, 65, 25, 25, 1.1);
GO

INSERT INTO silver.RunStream VALUES
    (1001, 0, 10.7, 106.6, 0, 10, 140, 80, NULL),
    (1001, 1, 10.7001, 106.6001, 3.0, 11, 142, 82, NULL),
    (1002, 0, 10.8, 106.7, 0, 15, 150, 85, NULL);
GO

-- Minimal test data for gold predictions (dbt tests need rows)
INSERT INTO gold.PBPrediction VALUES
    (100, '10K',  2400.0, '2025-10-15', 0.85),
    (101, '5K',   1200.0, '2025-10-15', 0.72);
GO

INSERT INTO gold.RaceForecast VALUES
    (100, 9001, 'Half-Marathon', 'riegel',             '2025-10-01', '10K', 2400, 5200, 5100, 100, 1.96),
    (100, 9001, 'Half-Marathon', 'vdot',               '2025-10-01', '10K', 2400, 5150, 5100,  50, 0.98),
    (100, 9001, 'Half-Marathon', 'elevation_adjusted',  '2025-10-01', '10K', 2400, 5250, 5100, 150, 2.94);
GO
