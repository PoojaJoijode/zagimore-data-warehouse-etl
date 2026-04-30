-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 04_initial_etl_load.sql
-- Source File: ELT_part2.sql
-- Purpose: Contains initial ETL/ELT logic for loading dimensions and warehouse tables.
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
SELECT p.productid, p.productname,	p.categoryid,	p.vendorid,	v.vendorname,	c.categoryname, 'S',	p.productprice, NULL, NULL
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE p.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid




INSERT INTO joijodp_zagimore_DS26.Product_Dimension(ProductId,	ProductName,	CategoryId,	VendorId,	VendorName,	CategoryName,	ProductType,	ProductSalesPrice,	ProductDailyPrice,	ProductWeeklyPrice)
SELECT r.productid, r.productname,	r.categoryid,	r.vendorid,	v.vendorname,	c.categoryname, 'R',NULL,	r.productpricedaily, r.productpriceweekly
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE r.Categoryid=c.Categoryid
AND v.Vendorid=r.Vendorid
union
SELECT p.productid, p.productname,	p.categoryid,	p.vendorid,	v.vendorname,	c.categoryname, 'S',	p.productprice, NULL, NULL
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE p.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid



--code for extracting data from various table from zagimore inot the rental dimension table zagimore ds
--union of rental and sales products to get the product details in the rental dimension table
SELECT r.productid, r.productname,	r.categoryid,	r.vendorid,	v.vendorname,	c.categoryname, 'R',NULL,	r.productpricedaily, r.productpriceweekly
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE r.Categoryid=c.Categoryid
AND v.Vendorid=r.Vendorid
union
SELECT p.productid, p.productname,	p.categoryid,	p.vendorid,	v.vendorname,	c.categoryname, 'S',	p.productprice, NULL, NULL
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE p.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid


--extractring intermediate revenue fact table
--sales revenue
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid

--rental revenue
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Rental' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.rentalproduct rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid


--rental revenue, daliy rental
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'


--rental revenue, weekly rental
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'




--All revenue
CREATE TABLE IF NOT EXISTS IFT AS 
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid
UNION
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'
UNION
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'


-- Changing the collation of the Revenue Type column in IFT

ALTER TABLE `IFT` CHANGE `revenuetype` `revenuetype` VARCHAR(14) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL;


--POPULATING THE REVENUE FACT TABLE
INSERT INTO joijodp_zagimore_DS26.Revenue_fact_table(DollarAmount, Tid, RevenueType, CustomerKey, ProductKey, StoreKey, CalendatKey)
SELECT i.DollarAmount, i.tid, i.revenueType, cu.CustomerKey, pd.ProductKey, sd.StoreKey, cd.CalendarKey
FROM IFT i, Customer_Dimension cu, Product_Dimension pd, Store_Dimension sd, Calendar_Dimension cd
WHERE i.customerid = cu.customerid
AND i.productid = pd.productid
AND LEFT(revenuetype, 1) = pd.ProductType
AND i.storeid = sd.storeid
AND i.tdate = cd.FullDate


--loading dimensions into DW
INSERT INTO joijodp_zagimore_DW26.Customer_Dimension(CustomerKey, customerid, customername, customerzip)
SELECT cd.CustomerKey, cd.customerid, cd.customername, cd.customerzip
FROM joijodp_zagimore_DS26.Customer_Dimension cd


INSERT INTO joijodp_zagimore_DW26.Store_Dimension(StoreKey, StoreId,	Storezip,	regionId,	RegionName)
SELECT sd.StoreKey,	sd.StoreId,	sd.Storezip, sd.regionId, sd.RegionName	
FROM joijodp_zagimore_DS26.Store_Dimension sd

INSERT INTO joijodp_zagimore_DW26.Product_Dimension(ProductKey, ProductId, ProductName, CategoryId, VendorId, VendorName, CategoryName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice)
SELECT pd.ProductKey, pd.ProductId, pd.ProductName, pd.CategoryId, pd.VendorId, pd.VendorName, pd.CategoryName, pd.ProductType, pd.ProductSalesPrice, pd.ProductDailyPrice, pd.ProductWeeklyPrice
FROM joijodp_zagimore_DS26.Product_Dimension pd


INSERT INTO joijodp_zagimore_DW26.Calendar_Dimension(CalendarKey, FullDate, MonthYear, CalendarYear)
SELECT ca.CalendarKey, ca.FullDate, ca.MonthYear, ca.CalendarYear
FROM joijodp_zagimore_DS26.Calendar_Dimension ca


INSERT INTO joijodp_zagimore_DW26.Revenue_fact_table()
SELECT r.DollarAmount, r.tid, r.revenueType, r.CustomerKey, r.ProductKey, r.StoreKey, r.CalendarKey
FROM joijodp_zagimore_DS26.Revenue_fact_table r





--creating product cat dimension
SELECT DISTINCT CategoryId, CategoryName
FROM Product_Dimension


CREATE TABLE CategoryDimension
(CategoryKey INT AUTO_INCREMENT,
Categoryid CHAR(2),
CategoryName VARCHAR(25),
PRIMARY KEY(CategoryKey)) 


INSERT INTO joijodp_zagimore_DS26.CategoryDimension(Categoryid, CategoryName)
SELECT DISTINCT CategoryId, CategoryName
FROM Product_Dimension

SELECT SUM(r.DollarAmount), r.CustomerKey, r.StoreKey, r.CalendarKey, cad.CategoryKey
FROM Revenue_fact_table r, CategoryDimension cad, Product_Dimension pd
WHERE cad.CategoryName = pd.CategoryName
AND r.ProductKey = pd.ProductKey
GROUP BY r.CustomerKey, r.StoreKey, r.CalendarKey, cad.CategoryKey


CREATE TABLE onewayaggregratebyCategory AS
SELECT SUM(r.DollarAmount), r.CustomerKey, r.StoreKey, r.CalendarKey, cad.CategoryKey
FROM Revenue_fact_table r, CategoryDimension cad, Product_Dimension pd
WHERE cad.CategoryName = pd.CategoryName
AND r.ProductKey = pd.ProductKey
GROUP BY r.CustomerKey, r.StoreKey, r.CalendarKey, cad.CategoryKey


 
CREATE TABLE joijodp_zagimore_DW26.CategoryDimension AS 
SELECT * FROM CategoryDimension



CREATE TABLE joijodp_zagimore_DW26.onewayaggregratebyCategory AS
SELECT * FROM onewayaggregratebyCategory



ALTER TABLE joijodp_zagimore_DW26.CategoryDimension 
ADD PRIMARY KEY (CategoryKey)


ALTER TABLE joijodp_zagimore_DW26.onewayaggregratebyCategory
(
ADD FOREIGN KEY (CustomerKey) REFERENCES joijodp_zagimore_DW26.Customer_Dimension(CustomerKey),
ADD FOREIGN KEY (StoreKey) REFERENCES joijodp_zagimore_DW26.Store_Dimension(StoreKey)
ADD FOREIGN KEY (CalendarKey) REFERENCES joijodp_zagimore_DW26.Calendar_Dimension(CalendarKey)
ADD FOREIGN KEY (CategoryKey) REFERENCES joijodp_zagimore_DW26.CategoryDimension(CategoryKey)
)









SELECT SUM(r.DollarAmount) AS TotalRevenue, COUNT(DISTINCT r.TID) AS TotalNumberTransactions, AVG(r.DollarAmount) AS AverageRevenuePerLineItem, SUM(r.DollarAmount)/COUNT(DISTINCT r.TID) AS AverageRevenuePerTransaction, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r
GROUP BY r.StoreKey, r.CalendarKey

CREATE TABLE DailyStoreSnapshot AS
SELECT SUM(r.DollarAmount) AS TotalRevenue, COUNT(DISTINCT r.TID) AS TotalNumberTransactions, AVG(r.DollarAmount) AS AverageRevenuePerLineItem, SUM(r.DollarAmount)/COUNT(DISTINCT r.TID) AS AverageRevenuePerTransaction, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r
GROUP BY r.StoreKey, r.CalendarKey



CREATE TABLE RegionDimension
(
    RegionKey INT AUTO_INCREMENT,
    RegionId CHAR(1),
    RegionName VARCHAR(25),
    PRIMARY KEY (RegionKey)
);


INSERT INTO joijodp_zagimore_DS26RegionDimension (RegionId, RegionName)
SELECT DISTINCT regionId, RegionName
FROM Store_Dimension;

SELECT SUM(r.DollarAmount) AS TotalDollarAmount, r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey
FROM Revenue_fact_table r, Store_Dimension sd, RegionDimension rd
WHERE r.StoreKey = sd.StoreKey
AND sd.regionId = rd.RegionId
GROUP BY r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey;



CREATE TABLE onewayaggregatebyregion AS
SELECT SUM(r.DollarAmount) AS TotalDollarAmount, r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey
FROM Revenue_fact_table r, Store_Dimension sd, RegionDimension rd
WHERE r.StoreKey = sd.StoreKey
AND sd.regionId = rd.RegionId
GROUP BY r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey;

CREATE TABLE joijodp_zagimore_DW26.RegionDimension AS
SELECT * FROM RegionDimension;

CREATE TABLE joijodp_zagimore_DW26.onewayaggregatebyregion AS
SELECT * FROM onewayaggregatebyStoreRegion;


ALTER TABLE joijodp_zagimore_DW26.RegionDimension
ADD PRIMARY KEY (RegionKey);


ALTER TABLE joijodp_zagimore_DW26.onewayaggregatebyregion
ADD FOREIGN KEY (CustomerKey) REFERENCES joijodp_zagimore_DW26.Customer_Dimension(CustomerKey),
ADD FOREIGN KEY (ProductKey) REFERENCES joijodp_zagimore_DW26.Product_Dimension(ProductKey),
ADD FOREIGN KEY (CalendarKey) REFERENCES joijodp_zagimore_DW26.Calendar_Dimension(CalendarKey),
ADD FOREIGN KEY (RegionKey) REFERENCES joijodp_zagimore_DW26.RegionDimension(RegionKey);