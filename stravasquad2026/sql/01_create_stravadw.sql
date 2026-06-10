-- StravaDW: new database for the bronze/silver/gold medallion architecture.
-- Run with: sqlcmd -S HANG -E -i stravasquad2026\sql\01_create_stravadw.sql

IF DB_ID('StravaDW') IS NULL
    CREATE DATABASE StravaDW;
GO

USE StravaDW;
GO

IF SCHEMA_ID('bronze') IS NULL EXEC('CREATE SCHEMA bronze');
GO
IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver');
GO
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

-- ============================================================
-- BRONZE — mirrors of the consolidated CSV exports, append-only
-- ============================================================

CREATE TABLE bronze.RunStream (
    time_s      INT,
    lat         FLOAT,
    lon         FLOAT,
    dist_m      FLOAT,
    alt_m       FLOAT,
    hr_bpm      SMALLINT,
    cadence     SMALLINT,
    watts       INT,
    activity_id BIGINT,
    ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_RunStream_activity_id ON bronze.RunStream (activity_id);
GO

CREATE TABLE bronze.RunBest (
    id           BIGINT,
    name         NVARCHAR(30),
    elapsed_time INT,
    moving_time  INT,
    start_date   DATETIME2,
    distance     INT,
    start_index  INT,
    end_index    INT,
    activity_id  BIGINT,
    athlete_id   BIGINT,
    ingested_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_RunBest_activity_id ON bronze.RunBest (activity_id);
GO

CREATE TABLE bronze.RunSegment (
    id                BIGINT,
    name              NVARCHAR(200),
    elapsed_time      INT,
    moving_time       INT,
    start_date        DATETIME2,
    start_date_local  DATETIME2,
    distance          FLOAT,
    start_index       INT,
    end_index         INT,
    average_cadence   FLOAT,
    device_watts      BIT,
    average_watts     FLOAT,
    average_heartrate FLOAT,
    max_heartrate     INT,
    visibility        NVARCHAR(30),
    hidden            BIT,
    activity_id       BIGINT,
    athlete_id        BIGINT,
    kom_rank          INT,
    ingested_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_RunSegment_activity_id ON bronze.RunSegment (activity_id);
GO

CREATE TABLE bronze.RunSplitKilometer (
    activity_id           BIGINT,
    segment_number        INT,
    segment_start_time    INT,
    segment_end_time      INT,
    start_lat             FLOAT,
    start_lon             FLOAT,
    cumulative_distance_m FLOAT,
    avg_hr_bpm            FLOAT,
    avg_cadence           FLOAT,
    avg_watts             FLOAT,
    segment_distance_m    FLOAT,
    duration_s            INT,
    pace_sec_per_km       FLOAT,
    ingested_at           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_RunSplitKilometer_activity_id ON bronze.RunSplitKilometer (activity_id);
GO

CREATE TABLE bronze.Activities (
    activity_id     BIGINT,
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
    stride_len_m    FLOAT,
    ingested_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_Activities_activity_id ON bronze.Activities (activity_id);
GO

CREATE TABLE bronze.Athlete (
    athlete_id  BIGINT,
    city        NVARCHAR(50),
    country     NVARCHAR(50),
    firstname   NVARCHAR(20),
    id          BIGINT,
    lastname    NVARCHAR(20),
    sex         NVARCHAR(2),
    weight      INT,
    ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_bronze_Athlete_athlete_id ON bronze.Athlete (athlete_id);
GO

-- ============================================================
-- SILVER — normalized: dropped duplicate/raw columns, PKs, indexes
-- ============================================================

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

CREATE TABLE silver.RunBest (
    id           BIGINT NOT NULL,
    name         NVARCHAR(30),
    elapsed_time INT,
    moving_time  INT,
    start_date   DATETIME2,
    distance     INT,
    activity_id  BIGINT,
    athlete_id   BIGINT,
    CONSTRAINT PK_silver_RunBest PRIMARY KEY (id)
);
CREATE INDEX IX_silver_RunBest_activity_id ON silver.RunBest (activity_id);
GO

CREATE TABLE silver.RunSegment (
    id                BIGINT NOT NULL,
    name              NVARCHAR(200),
    elapsed_time      INT,
    moving_time       INT,
    start_date        DATETIME2,
    start_date_local  DATETIME2,
    distance          FLOAT,
    start_index       INT,
    end_index         INT,
    average_cadence   FLOAT,
    average_watts     FLOAT,
    average_heartrate FLOAT,
    max_heartrate     INT,
    activity_id       BIGINT,
    athlete_id        BIGINT,
    CONSTRAINT PK_silver_RunSegment PRIMARY KEY (id)
);
CREATE INDEX IX_silver_RunSegment_activity_id ON silver.RunSegment (activity_id);
GO

CREATE TABLE silver.RunSplitKilometer (
    activity_id           BIGINT NOT NULL,
    segment_number        INT NOT NULL,
    segment_start_time    INT,
    segment_end_time      INT,
    start_lat             FLOAT,
    start_lon             FLOAT,
    cumulative_distance_m FLOAT,
    avg_hr_bpm            FLOAT,
    avg_cadence           FLOAT,
    avg_watts             FLOAT,
    segment_distance_m    FLOAT,
    duration_s            INT,
    pace_sec_per_km       FLOAT,
    CONSTRAINT PK_silver_RunSplitKilometer PRIMARY KEY (activity_id, segment_number)
);
GO

CREATE TABLE silver.Activities (
    activity_id     BIGINT NOT NULL,
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
    stride_len_m    FLOAT,
    CONSTRAINT PK_silver_Activities PRIMARY KEY (activity_id)
);
GO

CREATE TABLE silver.Athlete (
    athlete_id  BIGINT NOT NULL,
    city        NVARCHAR(50),
    country     NVARCHAR(50),
    firstname   NVARCHAR(20),
    lastname    NVARCHAR(20),
    sex         NVARCHAR(2),
    weight      INT,
    CONSTRAINT PK_silver_Athlete PRIMARY KEY (athlete_id)
);
GO
