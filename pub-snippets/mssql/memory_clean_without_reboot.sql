USE MASTER;
IF OBJECT_ID(N'tempdb..##globaltemp') IS NOT NULL
BEGIN
    DROP TABLE ##globaltemp
END
SELECT TOP 1 value
INTO ##globaltemp
FROM sys.configurations
WHERE NAME LIKE '%server memory%'
ORDER BY NAME
OPTION (RECOMPILE);
EXEC sys.sp_configure N'max server memory (MB)'
    , N'16000'
GO
RECONFIGURE
WITH OVERRIDE
GO
DECLARE @dsql AS VARCHAR(20)
SELECT @dsql = cast(value AS NVARCHAR(20))
FROM ##globaltemp
EXEC sys.sp_configure N'max server memory (MB)'
    , @dsql
GO
RECONFIGURE
WITH OVERRIDE
GO

-- From <https://stackoverflow.com/questions/17291047/sql-server-not-releasing-memory-after-query-executes>
