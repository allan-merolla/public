%%sql
SELECT count(*)
FROM [db].[dbo].[tbluseractivity]
WHERE zPartKey = '2020-12-16'
-- 8 seconds
SELECT count(*)
FROM [db].[dbo].[tbluseractivitystream]
WHERE zPartKey = '2020-12-16'
-- 17 minutes 14 seconds


