

WITH cte_mau AS (
	SELECT
		month(loginDate) as Month,
        max(loginDate) as loginDate,
        cast(count(DISTINCT userID) as decimal(5,2)) as MAU,
        LAG(
           cast(count(DISTINCT userID) as decimal(5,2))
          ,1)
        OVER(
             ORDER BY month(loginDate)
           ) as PreviousMonthMAU
	FROM
		userLogin
	Group By
        month(loginDate)
  )

SELECT
    FORMAT(loginDate,'MMM') as "Month Name",
    MAU,
    FORMAT(((MAU - PreviousMonthMAU) / PreviousMonthMAU),'p') as "Percent Change vs. Previous Month"

 FROM cte_mau




DECLARE @rangeStart NUMERIC(10,2)
DECLARE @rangeEnd NUMERIC(10,2)
DECLARE @month DATETIME2
DECLARE @dateStart DATETIME2
DECLARE @dateEnd DATETIME2

select @rangeStart=min(Balance),@rangeEnd=max(Balance) from userBalance
select @dateStart=min(transactionDate),@dateEnd=max(transactionDate) from userBalance

;WITH months AS (
    SELECT
        CAST (datename(month,@dateStart)+'-'+ datename(year,@dateStart) as nvarchar(255)) as MonthYear,
        CAST(@dateStart as datetime2) AS startMonth
    UNION ALL
    SELECT
        CAST (datename(month,DATEADD(month, 1, startMonth))+'-'+ datename(year,DATEADD(month, 1, startMonth)) as nvarchar(255)) as MonthYear,
        DATEADD(month, 1, startMonth)

    FROM months WHERE DATEADD(month, 1, startMonth)<=@dateEnd
),
bands AS (

            SELECT cast('' as nvarchar(255)) as label, 0 as GroupNo, CAST (0 as numeric(10,2)) as startnum, cast(@rangeStart AS NUMERIC(10, 2)) AS endnum
            UNION ALL
            SELECT
                cast(cast(a.endnum  as nvarchar(255)) + '-' + cast(a.endnum + 5.0 as nvarchar(255)) as nvarchar(255)) as label,
                a.GroupNo + 1,
                Cast(a.endnum AS NUMERIC(10, 2)) as startnum,
                Cast(a.endnum + 5.0 AS NUMERIC(10, 2))
            FROM   bands a
            WHERE  a.endnum < @rangeEnd+5.0
),
productjoin AS
(
    SELECT b.MonthYear,b.startMonth,a.label as [range] from bands a CROSS JOIN months b where a.GroupNo != 0
),
monthbands AS
(
    SELECT * FROM productjoin inner join bands on productjoin.range = bands.label

)




SELECT b.MonthYear, max(b.label) as "Histogram Bar", count(a.userID) as "Total", REPLICATE('*', count(a.userID)) as BAR
FROM userBalance a
RIGHT JOIN monthbands b
ON a.Balance >= b.startnum and a.Balance < b.endnum
and month(b.startMonth) = month(a.transactionDate)
and year(b.startMonth) = year(a.transactionDate)
Group By b.MonthYear,b.startMonth,b.GroupNo
Order by b.startMonth

CREATE TABLE transactions
([userID] int, [brand] NVARCHAR(255), [saleDate] DATETIME);
CREATE TABLE hat_transactions
([userID] int, [brand] NVARCHAR(255), [saleDate] DATETIME);
CREATE TABLE shoe_transactions
([userID] int, [brand] NVARCHAR(255), [saleDate] DATETIME);

INSERT INTO transactions
([userID], [brand],[saleDate])
VALUES
('1', 'TOM FORD', '2020-01-13'),
('2', 'CARRERA', '2020-01-14'),
('3', 'RAY BAN', '2020-01-21'),
('6', 'RAY BAN','2012-01-26'),
('12', 'OAKLEY','2020-02-01'),
('1', 'TOM FORD','2018-02-10'),
('1', 'TOM FORD','2017-02-18'),
('18', 'OAKLEY','2020-02-17'),
('18', 'MICHAEL KORS', '2020-03-04'),
('12','OAKLEY','2020-03-06'),
('2', 'MUI MUI','2020-03-10'),
('6', 'MUI MUI','2020-03-12'),
('6', 'OAKLEY','2019-03-19'),
('12', 'OAKLEY','2020-04-22'),
('8', 'CARRERA','2020-04-23'),
('7', 'MICHAEL KORS','2020-04-25'),
('10', 'TOM FORD','2019-02-10'),
('11', 'OAKLEY','2017-02-18'),
('8', 'TOM FORD','2018-04-23'),
('7', 'MICHAEL KORS','2020-04-25'),
('10', 'TOM FORD','2019-02-10'),
('11', 'MUI MUI','2018-02-18')


INSERT INTO hat_transactions
([userID], [brand],[saleDate])
VALUES
('1', 'BAILEY', '2020-01-13'),
('2', 'AKUBRA', '2020-01-14'),
('3', 'BARBOUR', '2020-01-21'),
('6', 'BARBOUR','2012-01-26'),
('12', 'NEW ERA','2020-02-01'),
('1', 'NEW ERA','2018-02-10'),
('1', 'BAILEY','2017-02-18'),
('18', 'NEW ERA','2020-02-17'),
('18', 'ADAPT', '2020-03-04'),
('12','NEW ERA','2020-03-06'),
('2', 'ACE OF SOMETHING','2020-03-10'),
('6', 'ACE OF SOMETHING','2020-03-12'),
('6', 'NEW ERA','2019-01-19'),
('12', 'NEW ERA','2020-04-23'),
('8', 'AKUBRA','2020-04-23'),
('7', 'ADAPT','2020-04-30'),
('10', 'BAILEY','2019-03-12'),
('11', 'NEW ERA','2017-04-18'),
('8', 'BAILEY','2018-08-23'),
('7', 'ADAPT','2020-11-25'),
('10', 'BAILEY','2019-02-10'),
('11', 'ACE OF SOMETHING','2018-02-18');
INSERT INTO shoe_transactions
([userID], [brand],[saleDate])
VALUES
('1', 'NIKE', '2020-01-13'),
('2', 'HAVIANAS', '2020-01-14'),
('2', 'BARBOUR', '2020-01-21'),
('12', 'BARBOUR','2012-01-26'),
('12', 'SALAMON','2020-02-01'),
('1', 'SALAMON','2018-02-10'),
('1', 'NIKE','2017-02-18'),
('18', 'SALAMON','2020-01-17'),
('18', 'DR MARTENS', '2020-03-04'),
('18', 'SALAMON','2020-04-17'),
('18', 'DR MARTENS', '2019-03-04'),
('12','SALAMON','2020-03-06'),
('2', 'REEBOK','2020-03-10'),
('6', 'REEBOK','2020-03-12'),
('6', 'SALAMON','2019-01-19'),
('12', 'SALAMON','2020-04-23'),
('8', 'HAVIANAS','2020-04-23'),
('7', 'DR MARTENS','2020-04-30'),
('10', 'NIKE','2018-03-12'),
('11', 'SALAMON','2017-04-18'),
('8', 'NIKE','2018-08-23'),
('7', 'DR MARTENS','2020-11-25'),
('10', 'NIKE','2019-02-10'),
('11', 'REEBOK','2018-02-18');


