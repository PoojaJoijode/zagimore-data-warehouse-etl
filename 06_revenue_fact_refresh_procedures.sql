-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 06_revenue_fact_refresh_procedures.sql
-- Source File: ZAGIMORE ETL FACT REFRESH.sql
-- Purpose: Contains refresh logic for revenue fact table updates.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================

INSERT INTO `salestransaction` (`tid`, `customerid` , `storeid`, `tdate`) VALUES ('BBB', '3-4-555', 'S2', '2026-03-31')
INSERT INTO `soldvia` (`productid`, `tid` , `numofitems`) VALUES ('3X1', 'BBB', '2')
INSERT INTO `soldvia` (`productid`, `tid` , `numofitems`) VALUES ('2X4', 'BBB', '1')
INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('CCC', '1-2-333', 'S7', '2026-03-31');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('1X1', 'CCC', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('4X4', 'CCC', 'W', '7');


--ADDING 2 NEW COLUMNS TO DATA STAGING
ALTER TABLE joijodp_zagimore_DS26.Revenue_fact_table
ADD ExtractionTimeStamp TimeStamp, 
ADD F_Loaded BOOLEAN


UPDATE joijodp_zagimore_DS26.Revenue_fact_table
SET ExtractionTimeStamp  = NOW() - INTERVAL 14 DAY 


UPDATE Revenue_fact_table
SET F_Loaded = TRUE



--code for daily fact refresh
DROP TABLE IFT;

CREATE TABLE IFT AS 
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid
AND st.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'
AND rt.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'
AND rt.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table)


ALTER TABLE `IFT` CHANGE `revenuetype` `revenuetype` VARCHAR(14) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL;


--POPULATING THE REVENUE FACT TABLE IN DS
INSERT INTO joijodp_zagimore_DS26.Revenue_fact_table(DollarAmount, Tid, revenuetype, CustomerKey, ProductKey, StoreKey, CalendarKey, ExtractionTimeStamp, F_Loaded)
SELECT i.DollarAmount, i.tid, i.revenuetype, cu.CustomerKey, pd.ProductKey, sd.StoreKey, cd.CalendarKey, NOW(), FALSE
FROM IFT i, Customer_Dimension cu, Product_Dimension pd, Store_Dimension sd, Calendar_Dimension cd
WHERE i.customerid = cu.customerid
AND i.productid = pd.productid
AND LEFT(i.revenuetype, 1) = pd.ProductType
AND i.storeid = sd.storeid
AND i.tdate = cd.FullDate


--loadin revenue fact table from ds to dw
INSERT INTO joijodp_zagimore_DW26.Revenue_fact_table(DollarAmount, tid, RevenueType, CustomerKey, ProductKey, StoreKey, CalendarKey)
SELECT r.DollarAmount, r.tid, r.revenuetype, r.CustomerKey, r.ProductKey, r.StoreKey, r.CalendarKey
FROM joijodp_zagimore_DS26.Revenue_fact_table r
WHERE F_Loaded = FALSE

UPDATE joijodp_zagimore_DS26.Revenue_fact_table
SET F_Loaded = TRUE
WHERE F_Loaded = False



--creating daily fact refresh procedure
CREATE procedure DailyFactRefresh()
BEGIN

DROP TABLE IFT;

CREATE TABLE IFT AS 
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid
AND st.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'
AND rt.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'
AND rt.tdate > (SELECT Max(ExtractionTimeStamp) FROM Revenue_fact_table);


ALTER TABLE `IFT` CHANGE `revenuetype` `revenuetype` VARCHAR(14) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL;


--POPULATING THE REVENUE FACT TABLE IN DS
INSERT INTO joijodp_zagimore_DS26.Revenue_fact_table(DollarAmount, Tid, revenuetype, CustomerKey, ProductKey, StoreKey, CalendarKey, ExtractionTimeStamp, F_Loaded)
SELECT i.DollarAmount, i.tid, i.revenuetype, cu.CustomerKey, pd.ProductKey, sd.StoreKey, cd.CalendarKey, NOW(), FALSE
FROM IFT i, Customer_Dimension cu, Product_Dimension pd, Store_Dimension sd, Calendar_Dimension cd
WHERE i.customerid = cu.customerid
AND i.productid = pd.productid
AND LEFT(i.revenuetype, 1) = pd.ProductType
AND i.storeid = sd.storeid
AND i.tdate = cd.FullDate;


--loadin revenue fact table from ds to dw
INSERT INTO joijodp_zagimore_DW26.Revenue_fact_table(DollarAmount, tid, RevenueType, CustomerKey, ProductKey, StoreKey, CalendarKey)
SELECT r.DollarAmount, r.tid, r.revenuetype, r.CustomerKey, r.ProductKey, r.StoreKey, r.CalendarKey
FROM joijodp_zagimore_DS26.Revenue_fact_table r
WHERE F_Loaded = FALSE;

UPDATE joijodp_zagimore_DS26.Revenue_fact_table
SET F_Loaded = TRUE
WHERE F_Loaded = False;

END



--homework
--testing procedure
INSERT INTO `salestransaction` (`tid`, `customerid` , `storeid`, `tdate`) VALUES ('FFF', '3-4-555', 'S2', '2026-04-01')
INSERT INTO `soldvia` (`productid`, `tid` , `noofitems`) VALUES ('3X1', 'FFF', '2')
INSERT INTO `soldvia` (`productid`, `tid` , `noofitems`) VALUES ('2X4', 'FFF', '1')
INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('GGG', '1-2-333', 'S7', '2026-04-01');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('1X1', 'GGG', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('4X4', 'GGG', 'W', '7');


Call DailyFactRefresh()


--late arriving facts
INSERT INTO `salestransaction` (`tid`, `customerid` , `storeid`, `tdate`) VALUES ('LLL', '3-4-555', 'S4', '2026-03-28')
INSERT INTO `soldvia` (`productid`, `tid` , `noofitems`) VALUES ('3X1', 'LLL', '2')



CREATE TABLE IFT AS 
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS revenueType, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid=sv.productid
AND sv.tid = st.tid
AND st.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'
AND rt.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table)


UNION
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS revenuetype, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid=rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'
AND rt.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table);



--creating procedure for late arrival facts







INSERT INTO `salestransaction` (`tid`, `customerid` , `storeid`, `tdate`) VALUES ('NFL', '3-4-555', 'S2', '2026-03-15')
INSERT INTO `soldvia` (`productid`, `tid` , `noofitems`) VALUES ('3X1', 'NFL', '2')
INSERT INTO `soldvia` (`productid`, `tid` , `noofitems`) VALUES ('2X4', 'NFL', '1')
INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('GGG', '1-2-333', 'S7', '2026-03-17');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('1X1', 'NBA', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('4X4', 'NBA', 'W', '7');









--Creation of procedure for late arriving facts
INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('AAA', '3-4-555', 'S6', '2026-03-28');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('1X2', 'AAA', '2');

CREATE PROCEDURE LateArrivingFactRefresh()
BEGIN
DROP TABLE IF EXISTS IFT;

CREATE TABLE IFT AS 
SELECT sv.noofitems*p.productprice AS DollarAmount, sv.tid, 'Sales' AS RevenueSource, st.customerid, sv.productid, st.storeid, st.tdate
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.soldvia sv, joijodp_zagimore2026.salestransaction st
WHERE p.productid = sv.productid
AND sv.tid = st.tid
AND st.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table)
UNION
SELECT rv.duration*rp.productpricedaily AS DollarAmount, rv.tid, 'Rental, daily' AS RevenueSource, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid = rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'
AND rt.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table)
UNION
SELECT rv.duration*rp.productpriceweekly AS DollarAmount, rv.tid, 'Rental, weekly' AS RevenueSource, rt.customerid, rv.productid, rt.storeid, rt.tdate
FROM joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.rentvia rv, joijodp_zagimore2026.rentaltransaction rt
WHERE rp.productid = rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'
AND rt.tid NOT IN (SELECT DISTINCT tid FROM Revenue_fact_table);

ALTER TABLE IFT
CHANGE RevenueSource RevenueSource VARCHAR(14) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL;


INSERT INTO joijodp_zagimore_DS26.Revenue_fact_table(DollarAmount, Tid, revenuetype, CustomerKey, ProductKey, StoreKey, CalendarKey, ExtractionTimeStamp, F_Loaded)
SELECT i.DollarAmount, i.tid, i.RevenueSource, cu.CustomerKey, pd.ProductKey, sd.StoreKey, cd.CalendarKey, NOW(), FALSE
FROM IFT i, Customer_Dimension cu, Product_Dimension pd, Store_Dimension sd, Calendar_Dimension cd
WHERE i.customerid = cu.customerid
AND i.productid = pd.ProductId
AND LEFT(i.RevenueSource, 1)=pd.ProductType
AND i.storeid = sd.StoreID
AND i.tdate = cd.FullDate;



INSERT INTO joijodp_zagimore_DW26.Revenue_fact_table(DollarAmount, Tid, revenuetype, CustomerKey, ProductKey, StoreKey, CalendarKey)
SELECT r.DollarAmount, r.Tid, r.revenuetype, r.CustomerKey, r.ProductKey, r.StoreKey, r.CalendarKey
FROM joijodp_zagimore_DS26.Revenue_fact_table r
WHERE r.f_loaded = 0;

UPDATE joijodp_zagimore_DS26.Revenue_fact_table
SET f_loaded = True 
WHERE f_loaded = False;

END


--testing new procedure
INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('NNN', '3-4-555', 'S6', '2026-03-01');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('1X1', 'NNN', '2');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('2X2', 'NNN', '3');
INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('EEE', '2-3-444', 'S7', '2026-03-01');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('2X2', 'EEE', 'D', '3');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('5X5', 'EEE', 'W', '2');



--Checking for accuracy of fact refresh
SELECT COUNT(*) AS NumberofRows, 'soldvia rows' AS SourceTable
FROM joijodp_zagimore2026.soldvia
UNION
SELECT COUNT(*) AS NumberofRows, 'rentvia rows' AS SourceTable
FROM joijodp_zagimore2026.rentvia
UNION
SELECT COUNT(*) AS NumberofRows, 'rows in ds' AS SourceTable
FROM joijodp_zagimore_DS26.Revenue_fact_table
UNION
SELECT COUNT(*) AS NumberofRows, 'rows in dw' AS SourceTable
FROM joijodp_zagimore_DW26.Revenue_fact_table


INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('XYZ', '3-4-555', 'S6', '2026-03-05');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('1X1', 'XYZ', '2');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('2X2', 'XYZ', '3');
INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('NHL', '2-3-444', 'S7', '2026-03-05');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('2X2', 'NHL', 'D', '3');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('5X5', 'NHL', 'W', '2');


--product dimesion refresh
INSERT INTO joijodp_zagimore2026.product(productid, productname, productprice, vendorid, categoryid)
VALUES ('2Z2', 'Test Product', 100.00, 'PG', 'CP')
INSERT INTO joijodp_zagimore2026.rentalProducts(productid, productname, vendorid, categoryid, productpricedaily, productpriceweekly)
VALUES ('2Z2', 'Test Rental Product', 'PG', 'CP', 10.00, 70.00,)


--Adding two more columns to the product dimension
ALTER TABLE joijodp_zagimore_DS26.Product_Dimension
ADD ExtractionTimeStamp timestamp,
ADD pd_loaded boolean;

UPDATE Product_Dimension
SET ExtractionTimeStamp = NOW() - INTERVAL 14 DAY;

UPDATE Product_Dimension
SET pd_loaded = True

--
CREATE PROCEDURE ProductDimensionRefresh()
BEGIN

INSERT INTO joijodp_zagimore_DS26.Product_Dimension(ProductId, ProductName, CategoryID, VendorID, CategoryName, VendorName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice, ExtractionTimeStamp, pd_loaded)
SELECT r.productid, r.productname, r.categoryid, r.vendorid, c.categoryname, v.vendorname, 'R', NULL, r.productpricedaily, r.productpriceweekly, NOW(), FALSE 
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c 
WHERE r.categoryid = c.categoryid 
AND v.vendorid = r.vendorid 
AND r.productid NOT IN (SELECT DISTINCT productid FROM joijodp_zagimore_DS26.Product_Dimension WHERE ProductType = 'R')
UNION 
SELECT p.productid, p.productname, p.categoryid, p.vendorid, c.categoryname, v.vendorname, 'S', p.productprice, NULL, NULL, NOW(), FALSE 
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c 
WHERE p.categoryid = c.categoryid
AND v.vendorid = p.vendorid
AND p.productid NOT IN (SELECT DISTINCT productid FROM joijodp_zagimore_DS26.Product_Dimension WHERE ProductType = 'S');

--
INSERT INTO joijodp_zagimore_DW26.Product_Dimension(ProductKey, ProductId, ProductName, CategoryID, VendorID, CategoryName, VendorName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice)
SELECT pd.ProductKey, pd.ProductId, pd.ProductName, pd.CategoryID, pd.VendorID, pd.CategoryName, pd.VendorName, pd.ProductType, pd.ProductSalesPrice, pd.ProductDailyPrice, pd.ProductWeeklyPrice
FROM joijodp_zagimore_DS26.Product_Dimension pd
WHERE pd.pd_loaded = 0;

UPDATE joijodp_zagimore_DS26.Product_Dimension
SET pd_loaded = True
WHERE pd_loaded = False;

END

CREATE Prodecure RowCountCheck()
BEGIN
SELECT COUNT(*), 'product rows' AS SourceTable
FROM joijodp_zagimore2026.product
UNION
SELECT COUNT(*), 'rental product rows' AS SourceTable
FROM joijodp_zagimore2026.rentalProducts
UNION
SELECT COUNT(*), 'rows in ds' AS SourceTable
FROM joijodp_zagimore_DS26.Product_Dimension
UNION
SELECT COUNT(*), 'rows in dw' AS SourceTable
FROM joijodp_zagimore_DW26.Product_Dimension
END

--Creating procedure for Customer dimension
ALTER TABLE joijodp_zagimore_DS26.Customer_Dimension
ADD ExtractionTimeStamp timestamp,
ADD c_loaded boolean;

UPDATE Customer_Dimension
SET ExtractionTimeStamp = NOW() - INTERVAL 14 DAY;

UPDATE Customer_Dimension
SET c_loaded = True

INSERT INTO `customer` (`customerid`, `customername`, `customerzip`) VALUES ('1-1-117', 'Kim', '55499');

CREATE PROCEDURE CustomerDimensionRefresh()
BEGIN

INSERT INTO joijodp_zagimore_DS26.Customer_Dimension(CustomerId, Customername, customerzip, ExtractionTimeStamp, c_loaded)
SELECT c.customerid, c.customername, c.customerzip, NOW(), False
FROM joijodp_zagimore2026.customer c
WHERE c.customerid NOT IN (SELECT DISTINCT customerid FROM joijodp_zagimore_DS26.Customer_Dimension);

--
INSERT INTO joijodp_zagimore_DW26.Customer_Dimension(CustomerId, Customername, customerzip, CustomerKey)
SELECT c.CustomerId, c.Customername, c.customerzip, c.CustomerKey
FROM joijodp_zagimore_DS26.Customer_Dimension c
WHERE c.c_loaded = 0;

UPDATE joijodp_zagimore_DS26.Customer_Dimension
SET c_loaded = True
WHERE c_loaded = False;

END


--creating procedure for Store Dimension
ALTER TABLE joijodp_zagimore_DS26.Store_Dimension
ADD ExtractionTimeStamp timestamp,
ADD s_loaded boolean;

UPDATE Store_Dimension
SET ExtractionTimeStamp = NOW() - INTERVAL 14 DAY;

UPDATE Store_Dimension
SET s_loaded = True

INSERT INTO `store` (`storeid`, `storezip`, `regionid`) VALUES ('S17', '47374', 'I');

CREATE PROCEDURE StoreDimensionRefresh()
BEGIN

INSERT INTO joijodp_zagimore_DS26.Store_Dimension(StoreID, Storezip, regionid, regionname, ExtractionTimeStamp, s_loaded)
SELECT s.storeid, s.storezip, s.regionid, r.regionname, NOW(), False
FROM joijodp_zagimore2026.store s, joijodp_zagimore2026.region r
WHERE s.regionid = r.regionid
AND s.storeid NOT IN (SELECT DISTINCT StoreID FROM joijodp_zagimore_DS26.Store_Dimension);

--
INSERT INTO joijodp_zagimore_DW26.Store_Dimension(StoreKey, StoreID, Storezip, regionid, regionname)
SELECT s.StoreKey, s.StoreID, s.Storezip, s.regionid, s.regionname
FROM joijodp_zagimore_DS26.Store_Dimension s
WHERE s.s_loaded = 0;

UPDATE joijodp_zagimore_DS26.Store_Dimension
SET s_loaded = True
WHERE s_loaded = False;

END