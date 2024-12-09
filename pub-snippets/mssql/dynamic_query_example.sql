DECLARE @query nvarchar(max), @ParmDefinition NVARCHAR(1024);
DECLARE @table nvarchar(64);
DECLARE @tempTable table(userID int, type nvarchar(255), Favourite nvarchar(255))
SET @table = 'shoe';
SET @query = N'SELECT userID,''' +@table+ ''' as type,
        CASE
        WHEN count(a.brand) >2  THEN max(a.brand)
        WHEN (select count(r.brand) from (SELECT g.brand,count(g.brand) as [count],max(g.saleDate) as saleDate
                FROM ' +@table+ '_transactions  g
                where g.userID = a.userID
                and g.saleDate >= dateadd(YEAR,-3,GETDATE())
                group by g.brand
            ) r) > 0 THEN
            (select h.brand from (SELECT top 1
                userID,b.brand,
                count(b.brand) as [count],
                max(b.saleDate) as saleDate
                FROM ' +@table+ '_transactions  b
                where b.userID = a.userID
                group by b.userID,b.brand
                order by [count] desc) h )
        ELSE (select f.brand from (SELECT top 1
                userID,b.brand,
                count(b.brand) as [count],
                max(b.saleDate) as saleDate
                FROM ' +@table+ '_transactions  b
                where b.userID = a.userID
                group by b.userID,b.brand
                order by [count] desc) f )
    END as Favourite
    FROM ' +@table+ '_transactions a
    GROUP BY a.userID
    ORDER BY a.userID'

INSERT INTO @tempTable EXEC sp_executeSQL @query
SET @query = REPLACE(@query,'shoe','sunglass')
INSERT INTO @tempTable EXEC sp_executeSQL @query
SET @query = REPLACE(@query,'sunglass','hat')
INSERT INTO @tempTable EXEC sp_executeSQL @query


SELECT * FROM @tempTable t PIVOT(max(t.Favourite) FOR [type] IN ([shoe],[sunglass],[hat])) as pivottable