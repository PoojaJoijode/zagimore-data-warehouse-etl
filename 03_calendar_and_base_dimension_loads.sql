-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 03_calendar_and_base_dimension_loads.sql
-- Source File: DELIMITER $$.sql
-- Purpose: Contains stored procedure examples and base dimension loading logic.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================

DELIMITER $$

CREATE PROCEDURE p()

BEGIN

  DECLARE i INT DEFAULT 1;   

myloop: LOOP

    SET i=i+1;

	SELECT CONCAT('I can count to ', i);

    IF i=10 then

            LEAVE myloop;

    END IF;

END LOOP myloop;



END;









DELIMITER $$

CREATE PROCEDURE populate_Calendar()

BEGIN

  DECLARE i INT DEFAULT 0;  

myloop: LOOP

   
    INSERT INTO Calendar_Dimension(FullDate)

    SELECT DATE_ADD('2013-01-01', INTERVAL i DAY);

    SET i=i+1;

    IF i=8000 then

            LEAVE myloop;

    END IF;

END LOOP myloop;

UPDATE Calendar_Dimension

SET MonthYear = MONTH(FullDate), Year = YEAR(FullDate);

END








DELIMITER $$

CREATE PROCEDURE Update_Calendar()

BEGIN

UPDATE Calendar_Dimension
SET MonthYear = LPAD(CONCAT(Month(FullDate),YEAR(FullDate)), 6, '0'), CalendarYear = YEAR(FullDate), CalendarWeekDay = DAYNAME(FullDate);

END;





--code for extracting the date in zagimore

INSERT INTO joijodp_zagimore_DS26.Customer_Dimension(customerid,customername,customerzip)
SELECT c.customerid, c.customername, c.customerzip
FROM joijodp_zagimore2026.customer c;



--code for extracting the date from store and region tables in zagimore into the store dimension
INSERT INTO joijodp_zagimore_DS26.Store_Dimension(StoreId,	Storezip,	regionId,	RegionName)
SELECT s.storekey,	s.storeid,	s.storezip,	s.regionid,	s.regionname	
FROM joijodp_zagimore2026_.region r
JOIN joijodp_zagimore2026_.store s ON r.RegionID = s.RegionID;



--code for extracting data from various table from zagimore inot the product dimension table zagimore ds
SELECT p.productid, p.productname,	p.categoryid,	p.vendorid.	v.vendorname,	c.categoryname, 'S',	p.productprice, NULL, NULL
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026 c
WHERE p.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid



--code for extracting data from various table from zagimore inot the rental dimension table zagimore ds
SELECT r.productid, r.productname,	r.categoryid,	r.vendorid.	v.vendorname,	c.categoryname, 'R',NULL,	r.productpricedaily, r.productpriceweekly
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026 c
WHERE r.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid