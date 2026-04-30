-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 05_revenue_fact_initial_load.sql
-- Source File: ETL_part3-Assignment.sql
-- Purpose: Builds intermediate and initial revenue fact loading queries.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================

--extractring intermediate revenue fact table


SELECT r.productid, r.productname,	r.categoryid,	r.vendorid,	v.vendorname,	c.categoryname, 'R',NULL,	r.productpricedaily, r.productpriceweekly
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE r.Categoryid=c.Categoryid
AND v.Vendorid=r.Vendorid
union
SELECT p.productid, p.productname,	p.categoryid,	p.vendorid,	v.vendorname,	c.categoryname, 'S',	p.productprice, NULL, NULL
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
WHERE p.Categoryid=c.Categoryid
AND v.Vendorid=p.Vendorid


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
INSERT INTO joijodp_zagimore_DS26.Revenue_fact_table(DollarAmount, Tid, RevenueType, CustomerKey, ProductKey, StoreKey, CalendarKey)
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

--store
INSERT INTO joijodp_zagimore_DW26.Store_Dimension(StoreKey, StoreId,	Storezip,	regionId,	RegionName)
SELECT sd.StoreKey,	sd.StoreId,	sd.Storezip, sd.regionId, sd.RegionName	
FROM joijodp_zagimore_DS26.Store_Dimension sd

--product
INSERT INTO joijodp_zagimore_DW26.Product_Dimension(ProductKey, ProductId, ProductName, CategoryId, VendorId, VendorName, CategoryName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice)
SELECT pd.ProductKey, pd.ProductId, pd.ProductName, pd.CategoryId, pd.VendorId, pd.VendorName, pd.CategoryName, pd.ProductType, pd.ProductSalesPrice, pd.ProductDailyPrice, pd.ProductWeeklyPrice
FROM joijodp_zagimore_DS26.Product_Dimension pd

--calendar
INSERT INTO joijodp_zagimore_DW26.Calendar_Dimension(CalendarKey, FullDate, MonthYear, CalendarYear)
SELECT ca.CalendarKey, ca.FullDate, ca.MonthYear, ca.CalendarYear
FROM joijodp_zagimore_DS26.Calendar_Dimension ca

--revenue
INSERT INTO joijodp_zagimore_DW26.Revenue_fact_table(DollarAmount, tid, revenueType, CustomerKey, ProductKey, StoreKey, CalendarKey)
SELECT r.DollarAmount, r.tid, r.revenueType, r.CustomerKey, r.ProductKey, r.StoreKey, r.CalendarKey
FROM joijodp_zagimore_DS26.Revenue_fact_table r

