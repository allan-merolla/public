DECLARE @startnum INT=1000
DECLARE @endnum INT=1050
;
WITH gen AS (
    SELECT @startnum AS num
    UNION ALL
    SELECT num+1 FROM gen WHERE num+1<=@endnum
)
SELECT * FROM gen
option (maxrecursion 10000)

-- <https://stackoverflow.com/questions/21425546/how-to-generate-a-range-of-numbers-between-two-numbers>