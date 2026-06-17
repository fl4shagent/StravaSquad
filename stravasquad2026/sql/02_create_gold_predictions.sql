-- StravaDW gold tables for Step 5 (PB Prediction + Race Forecast).
-- Written/refreshed by stravasquad2026\predict_pb.py.
-- Run with: sqlcmd -S HANG -d StravaDW -E -i stravasquad2026\sql\02_create_gold_predictions.sql

USE StravaDW;
GO

IF OBJECT_ID('gold.PBPrediction', 'U') IS NULL
BEGIN
    CREATE TABLE gold.PBPrediction (
        athlete_id          BIGINT NOT NULL,
        distance_label      NVARCHAR(30) NOT NULL,
        predicted_best_sec  FLOAT,
        prediction_date     DATE NOT NULL,
        r_squared           FLOAT,
        CONSTRAINT PK_gold_PBPrediction PRIMARY KEY (athlete_id, distance_label, prediction_date)
    );
END
GO

IF OBJECT_ID('gold.RaceForecast', 'U') IS NULL
BEGIN
    CREATE TABLE gold.RaceForecast (
        athlete_id              BIGINT NOT NULL,
        race_activity_id        BIGINT,
        target_distance_label   NVARCHAR(30) NOT NULL,
        method                  NVARCHAR(30) NOT NULL,
        as_of_date              DATE NOT NULL,
        source_distance_label   NVARCHAR(30),
        source_pb_sec           FLOAT,
        predicted_sec           FLOAT,
        actual_sec              FLOAT,
        error_sec               FLOAT,
        error_pct               FLOAT,
        CONSTRAINT PK_gold_RaceForecast PRIMARY KEY (athlete_id, target_distance_label, method, as_of_date)
    );
END
GO
