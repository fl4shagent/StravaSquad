USE StravaProject;
GO

PRINT 'Before:';
SELECT COUNT(*) AS total_rows FROM RunStream;
GO

;WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY activity_id, time_s ORDER BY (SELECT NULL)) AS rn
    FROM RunStream
)
DELETE FROM ranked WHERE rn > 1;
GO

PRINT 'After:';
SELECT COUNT(*) AS total_rows FROM RunStream;
GO
