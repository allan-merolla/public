SELECT *
FROM
    OPENROWSET(
        BULK 'https://lake.dfs.core.windows.net/parquet/userdata1.parquet',
        FORMAT='PARQUET'
    ) AS [result]
