EXEC sp_MSforeachdb '
DECLARE @sqlcommand nvarchar (500)
IF ''?'' NOT IN (''master'', ''model'', ''msdb'')
BEGIN
USE [?]
SELECT @sqlcommand = ''DBCC SHRINKFILE (N'''''' +
name
FROM [sys].[database_files]
WHERE type_desc = ''LOG''
SELECT @sqlcommand = @sqlcommand + '''''' , 0)''
EXEC sp_executesql @sqlcommand
END'
