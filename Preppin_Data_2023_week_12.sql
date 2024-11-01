--The challenge this week is heavily inspired by a real life scenario and I'm sure many organisations will be able to relate to the quirky rules they have to follow when doing regulatory reporting. Often with the reasoning behind it being "because that's the way it's always been done!" 

--DSB must assign new customers to the next working day, even if they join at the weekend, or online on a public holiday. What's more, they need to report their total new customers for the month on the last working day of the month. This means any customers joining on that last working day will actually be counted in the following month. For example, 31st December 2021 was a Friday. The total number of customers for December would be reported on this day. Any customers joining on the day of 31st December 2021 itself will be counted in January's count of new customers. 

--What makes this even more confusing is trying to align with branches in Ireland. Ireland will of course have different Bank Holidays and so the definition of a working day becomes harder to define. For DSB, the UK reporting day supersedes the ROI reporting day. If the UK has a bank holiday where ROI does not, these customers will be reported on the next working day in the UK. If ROI has a bank holiday where the UK does not, the customer count will be 0 for ROI, but it will still be treated as a working day when assigning the reporting month start/end

--Requirements
--Fill down the years and create a date field for the UK bank holidays
--Combine with the UK New Customer dataset
--Create a Reporting Day flag
--UK bank holidays are not reporting days
--Weekends are not reporting days
--For non-reporting days, assign the customers to the next reporting day
--Calculate the reporting month, as per the definition above
--Filter out January 2024 dates
--Calculate the reporting day, defined as the order of days in the reporting month
--You'll notice reporting months often have different numbers of days!
--Now let's focus on ROI data. This has already been through a similar process to the above, but using the ROI bank holidays. We'll have to align it with the UK reporting schedule
--Rename fields so it's clear which fields are ROI and which are UK
--Combine with UK data
--For days which do not align, find the next UK reporting day and assign new customers to that day (for more detail, refer to the above description of the challenge)
--Make sure null customer values are replaced with 0's
--Create a flag to find which dates have differing reporting months when using the ROI/UK systems


SELECT *
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_ROI_NEW_CUSTOMERS; --Ireland New Customers

SELECT *
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_UK_BANK_HOLIDAYS; --U.K Bank Holidays

SELECT *
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_NEW_CUSTOMERS;  --UK New Customers



WITH FILLED_YEAR AS
(
SELECT MAX(YEAR) OVER (ORDER BY ROW_NUM) AS YEAR --Fill in the year
      ,DATE
      ,BANK_HOLIDAY
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_UK_BANK_HOLIDAYS
)
,
BANK_HOLDS AS
(
SELECT BANK_HOLIDAY
      ,DATE(CONCAT(DATE,'-',YEAR), 'DD-Mon-YYYY') AS BANK_DATE  --could have been date||'-'||year for date concat
FROM FILLED_YEAR
       WHERE DATE != ''  --19
)
,
REPORTING_FLAG AS
(
SELECT DATE(DATE, 'DD/MM/YYYY') as CUSTOMER_DATE
      ,dayname(DATE(DATE, 'DD/MM/YYYY')) as DAY --Get name to flag weekends 
      ,case
      when left(dayname(DATE(DATE, 'DD/MM/YYYY')),1)='S' or BANK_HOLIDAY IS NOT NULL THEN 'Y' --use flag to indicate weekends and holidays
      ELSE 'N'
      END AS WEEKEND_HOLIDAY
      ,NEW_CUSTOMERS
      ,BH.BANK_HOLIDAY
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_NEW_CUSTOMERS UK --left join from uk.customers to bank holiday to indicate holidays
LEFT JOIN BANK_HOLDS BH ON BH.BANK_DATE=DATE(UK.DATE, 'DD/MM/YYYY') --731
--WHERE WEEKEND_HOLIDAY = 'N'
)
,
NON_REPORTING_DATES AS
(
SELECT DISTINCT CUSTOMER_DATE AS NON_REPORTING_DATE
               --,BANK_HOLIDAY
FROM   REPORTING_FLAG
WHERE WEEKEND_HOLIDAY = 'Y'  --select distinct non reporting days which are weekends and holidays using the key 'Y' 229
--Keep eye on 31 and 30/12/2023
ORDER BY 1
)
,
NON_REPORTING_LOOKUP AS
(
SELECT NON_REPORTING_DATE
      ,MIN(CUSTOMER_DATE) AS NEXT_REPORTING_DATE
FROM REPORTING_FLAG R
INNER JOIN NON_REPORTING_DATES NR ON NR.NON_REPORTING_DATE<R.CUSTOMER_DATE --join that to the main repoting flag table to find the next day possible for reporting ('min' in this case)
--and stating the nrd < customer date and also making sure the customer dates are not holidays or weekends by using the key 'N'
--this would lead to duplicates since we gave room using nrd < customer date, so we need to group by nrd 
--227 we loose 31 and 30/12/2023 since they will be reported next year 
WHERE R.WEEKEND_HOLIDAY = 'N'
GROUP BY 1
ORDER BY 1
) --2,3,4,5 are all shifted to 6 june
,
UK_REPORT AS
(
SELECT COALESCE(next_reporting_date,customer_date) Date--this is saying when next_reporting_date ISNULL then replace it with customer_date!! The NULLS 
      ,MONTHNAME(coalesce(next_reporting_date,customer_date)) || '-' || year(coalesce(next_reporting_date,customer_date)) as month_year
      ,SUM(new_customers)  AS NEW_CUSTOMERS
FROM REPORTING_FLAG R
LEFT JOIN NON_REPORTING_LOOKUP NRL ON NRL.NON_REPORTING_DATE=CUSTOMER_DATE
--where next_reporting_date is not null
group by 1,2
order by 1
)
,UK_LAST_DAY_OF_MONTH_YEAR AS
(
SELECT MONTH_YEAR AS MONTH_YEAR2
      ,MAX(DATE) AS LAST_DATE
FROM UK_REPORT
GROUP BY 1
)
,UK_REPORTING_MONTH AS 
(
SELECT CASE
       WHEN LAST_DATE IS NULL THEN MONTHNAME(DATE) || '-' || YEAR(DATE)
       ELSE MONTHNAME(DATEADD('month',1,DATE)) || '-' || YEAR(DATEADD('month',1,DATE))
       END AS REPORTING_MONTH_YEAR
       --,DATEADD('month',1,DATE) AS DATEADDED
       ,DATE
       ,ROW_NUMBER() OVER (PARTITION BY
       (CASE
       WHEN LAST_DATE IS NULL THEN MONTHNAME(DATE) || '-' || YEAR(DATE)
       ELSE MONTHNAME(DATEADD('month',1,DATE)) || '-' || YEAR(DATEADD('month',1,DATE))
       END) ORDER BY DATE) AS RN
       ,NEW_CUSTOMERS
       --*
FROM UK_REPORT UK
LEFT JOIN UK_LAST_DAY_OF_MONTH_YEAR ULD ON UK.DATE=ULD.LAST_DATE
WHERE DATE < '2023-12-29' --LAST WORKING DAY OF THE YEAR
ORDER BY 2 --501 till this point
)
,
ROI_DATA AS
(
SELECT REPORTING_MONTH AS ROI_REPORTING_MONTH
      ,REPORTING_DAY AS ROI_REPORTING_DAY
      ,NEW_CUSTOMERS AS ROI_NEW_CUSTOMERS
      ,DATE(REPORTING_DATE, 'DD/MM/YYYY') AS ROI_REPORTING_DATE
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK12_ROI_NEW_CUSTOMERS --501
order by 4
)
,
UK_DATES AS
(
SELECT  REPORTING_MONTH_YEAR AS REPORTING_MONTH
       ,DATE AS REPORTING_DATE
       ,RN AS UK_REPORTING_DAY 
       ,NEW_CUSTOMERS AS UK_NEW_CUSTOMERS
       ,IFNULL(ROI_NEW_CUSTOMERS,0) AS ROI_NEW_CUSTOMERS
       --,ROI_REPORTING_MONTH
       ,case
       WHEN ROI_REPORTING_MONTH IS NULL AND REPORTING_DATE='2022-06-06'
       THEN REPLACE(REPORTING_MONTH,'20','')
       ELSE ROI_REPORTING_MONTH
       END AS ROI_REPORTING_MONTH  --solved
FROM UK_REPORTING_MONTH U
LEFT JOIN ROI_DATA R ON R.ROI_REPORTING_DATE=U.DATE  
--where ROI_REPORTING_MONTH is null
order by 2
)
,
ROI_DATES AS
(
SELECT
      ROI_REPORTING_MONTH
     ,ROI_REPORTING_DATE 
     ,ROI_REPORTING_DAY 
     ,ROI_NEW_CUSTOMERS 
     --,U.DATE
     ,MIN(U2.DATE) as NEXT_UK_DATE
FROM ROI_DATA R
LEFT JOIN UK_REPORTING_MONTH U ON R.ROI_REPORTING_DATE=U.DATE
LEFT JOIN UK_REPORTING_MONTH U2 ON R.ROI_REPORTING_DATE<U2.DATE
WHERE U.DATE IS NULL  --since we using null here, we can bring a second join which will bring in UK_REPORTING MONTH WITH A DIFFERENT ALIAS
GROUP BY 1,2,3,4  ORDER BY 2--this will give us the posible dates (duplicates) greater than the ROI dates to be used and we can then select the min or append row numbers and chose least.
) --10 WITH TWO 6-6-22 with total customers 6
,COMBINED AS
(
SELECT REPORTING_MONTH_YEAR AS REPORTING_MONTH
      ,DATE AS REPORTING_DATE
      ,RN AS REPORTING_DAY
      ,0 AS UK_NEW_CUSTOMERS
      ,ROI_NEW_CUSTOMERS
      ,ROI_REPORTING_MONTH
FROM ROI_DATES RD
JOIN UK_REPORTING_MONTH UK ON UK.DATE=RD.NEXT_UK_DATE

UNION ALL

SELECT * 
FROM UK_DATES
order by 2
)
--,FINAL AS
--(
SELECT CASE
      WHEN ROI_REPORTING_MONTH IS NULL THEN 'x'
      WHEN LEFT(REPORTING_MONTH,3)!=LEFT(ROI_REPORTING_MONTH,3) THEN 'x'
      ELSE '' 
      END AS MISALIGMENT_FLAG
      ,REPORTING_MONTH
      ,min(REPORTING_DAY) REPORTING_DAY
      ,REPORTING_DATE
      ,SUM(UK_NEW_CUSTOMERS) AS UK_NEW_CUSTOMERS
      ,SUM(ROI_NEW_CUSTOMERS) AS ROI_NEW_CUSTOMERS
      ,ROI_REPORTING_MONTH
FROM COMBINED
GROUP BY 1,2,4,7
ORDER BY 4;

/*SELECT REPORTING_DATE
       ,COUNT(*) AS C
       FROM FINAL
       GROUP BY 1
       HAVING C>1;









