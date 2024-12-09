-- create master key that will protect the credentials if not already exist:
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'password'
-- create credentials for containers in the storage account
CREATE DATABASE SCOPED CREDENTIAL sql_cred
WITH IDENTITY='SHARED ACCESS SIGNATURE',
SECRET = 'secret'
GO
--The external data source connection:
create external data source adls_pq
with (
    location = 'wasbs://parquet-root@test.blob.core.windows.net/',
    credential = sql_cred
    );
--Next, the external file type:
create external file format adls_pq
with (
    format_type = PARQUET,
    data_compression = 'org.apache.hadoop.io.compress.GzipCodec'
    );

--And finally, the external table definition:
create external table userdata_pq (
	registration_dttm datetime2,
	id int,
	first_name varchar(255),
	last_name  varchar(255),
	email  varchar(255),
	gender  varchar(255),
	ip_address varchar(255),
	cc   varchar(255),
	country  varchar(255),
	birthdate  varchar(255),
	salary  float,
	title  varchar(255),
	comments  varchar(4000)
    )
with
(
    location = '/*/*.parquet',
    data_source = adls_pq,
    file_format = adls_pq
    );

select count(*) from userdata_pq