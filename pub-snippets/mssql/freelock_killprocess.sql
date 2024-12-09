USE master
GO
DECLARE @kill varchar(8000) = '';
SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'
FROM master..sysprocesses
WHERE dbid = db_id('<yourDbName>')
EXEC(@kill);

--From <https://stackoverflow.com/questions/14652923/set-database-from-single-user-mode-to-multi-user>
