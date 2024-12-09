IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat')
    CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat]
    WITH ( FORMAT_TYPE = PARQUET)
GO
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'dfs_core_windows_net')
    CREATE EXTERNAL DATA SOURCE [data]
    WITH (
        LOCATION   = 'https://data.dfs.core.windows.net/data',
    )
Go
CREATE EXTERNAL TABLE sharetest (
    [registration_dttm] datetime2(7),
    [id] int,
    [first_name] varchar(8000),
    [last_name] varchar(8000),
    [email] varchar(8000),
    [gender] varchar(8000),
    [ip_address] varchar(8000),
    [cc] varchar(8000),
    [country] varchar(8000),
    [birthdate] varchar(8000),
    [salary] float,
    [title] varchar(8000),
    [comments] varchar(8000)
    )
    WITH (
    LOCATION = 'landing/temp/userdata1.parquet',
    DATA_SOURCE = [data],
    FILE_FORMAT = [SynapseParquetFormat]
    )
GO
SELECT TOP 100 * FROM sharetest
GO
