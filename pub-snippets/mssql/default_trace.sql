
select * FROM (
select  f.LoginName as LoginName, db_name(c.dbid) as DBID, c.text as TEXT, a.last_execution_time as LAST_EXECUTION_DATE
from sys.dm_exec_query_stats a
cross apply sys.dm_exec_sql_text(a.sql_handle) c
cross apply sys.dm_exec_query_plan(a.plan_handle) e
inner join (
SELECT *
 FROM fn_trace_gettable('D:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\Log\log_1510.trc',default)
) f on a.last_execution_time > '2018-11-28 14:00:00' and a.last_execution_time < '2018-11-28 14:35:00'
) as b
Where b.LoginName = 'username'
