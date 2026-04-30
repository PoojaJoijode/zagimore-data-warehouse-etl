-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 08_category_and_daily_store_aggregates.sql
-- Source File: ETL-part4.sql
-- Purpose: Creates category-level and daily store aggregate tables.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================


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
ADD FOREIGN KEY (CustomerKey) REFERENCES joijodp_zagimore_DW26.Customer_Dimension(CustomerKey),
ADD FOREIGN KEY (StoreKey) REFERENCES joijodp_zagimore_DW26.Store_Dimension(StoreKey)
ADD FOREIGN KEY (CalendarKey) REFERENCES joijodp_zagimore_DW26.Calendar_Dimension(CalendarKey)
ADD FOREIGN KEY (CategoryKey) REFERENCES joijodp_zagimore_DW26.CategoryDimension(CategoryKey)


SELECT SUM(r.DollarAmount) AS TotalRevenue, COUNT(DISTINCT r.TID) AS TotalNumberTransactions, AVG(r.DollarAmount) AS AverageRevenuePerLineItem, SUM(r.DollarAmount)/COUNT(DISTINCT r.TID) AS AverageRevenuePerTransaction, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r
GROUP BY r.StoreKey, r.CalendarKey

CREATE TABLE DailyStoreSnapshot AS
SELECT SUM(r.DollarAmount) AS TotalRevenue, COUNT(DISTINCT r.TID) AS TotalNumberTransactions, AVG(r.DollarAmount) AS AverageRevenuePerLineItem, SUM(r.DollarAmount)/COUNT(DISTINCT r.TID) AS AverageRevenuePerTransaction, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r
GROUP BY r.StoreKey, r.CalendarKey










ALTER TABLE DailyStoreSnapshot
ADD TotalFootwearRevenue DECIMAL(10,2)
ADD TotalNumHighRevTransaction int,
ADD TotalLocalRevenue DECIMAL(10,2)


--totalfootwearrevenue
CREATE VIEW FR AS
SELECT SUM(r.DollarAmount) AS TotalFootwearRevenue, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r, Product_Dimension p
WHERE r.ProductKey = p.ProductKey
AND p.CategoryName = "Footwear"
GROUP BY r.StoreKey, r.CalendarKey


--totalnumof 100$+  transaction
CREATE VIEW HT AS
SELECT  COUNT(DISTINCT r.TID) AS TotalNumHighRevTransaction, r.StoreKey, r.CalendarKey 
FROM RT r
WHERE TotalRevenuePerTransaction > 100
GROUP BY r.StoreKey, r.CalendarKey


CREATE VIEW RT AS
SELECT  SUM(r.DollarAmount) AS TotalRevenuePerTransaction, r.TID, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r
GROUP BY r.StoreKey, r.CalendarKey, r.TID


--localrevenue
CREATE VIEW LR AS
SELECT SUM(r.DollarAmount) AS TotalLocalRevenue, r.StoreKey, r.CalendarKey 
FROM Revenue_fact_table r, Store_Dimension s, Customer_Dimension c
WHERE r.StoreKey = s.StoreKey
AND r.CustomerKey = c.CustomerKey
AND LEFT(c.Customerzip, 2) = LEFT(s.Storezip, 2)
GROUP BY r.StoreKey, r.CalendarKey


UPDATE DailyStoreSnapshot ds, FR
SET ds.TotalFootwearRevenue = FR.TotalFootwearRevenue
WHERE ds.StoreKey = FR.StoreKey
AND ds.CalendarKey = FR.CalendarKey

UPDATE DailyStoreSnapshot ds
SET ds.TotalFootwearRevenue = 0
WHERE ds.TotalFootwearRevenue IS NULL


UPDATE DailyStoreSnapshot ds, HT
SET ds.TotalNumHighRevTransaction = HT.TotalNumHighRevTransaction
WHERE ds.StoreKey = HT.StoreKey
AND ds.CalendarKey = HT.CalendarKey


UPDATE DailyStoreSnapshot ds
SET ds.TotalNumHighRevTransaction = 0
WHERE ds.TotalNumHighRevTransaction IS NULL



UPDATE DailyStoreSnapshot ds, LR
SET ds.TotalLocalRevenue = LR.TotalLocalRevenue
WHERE ds.StoreKey = LR.StoreKey
AND ds.CalendarKey = LR. CalendarKey


UPDATE DailyStoreSnapshot ds
SET ds.TotalLocalRevenue = 0
WHERE ds.TotalLocalRevenue IS NULL



CREATE TABLE joijodp_zagimore_DW26.DailyStoreSnapshot AS
SELECT * FROM DailyStoreSnapshot


ALTER TABLE joijodp_zagimore_DW26.DailyStoreSnapshot
ADD PRIMARY KEY(StoreKey, CalendarKey),
ADD FOREIGN KEY(StoreKey) REFERENCES Store_Dimension(StoreKey),
ADD FOREIGN KEY(CalendarKey) REFERENCES Calendar_Dimension(CalendarKey)


ALTER TABLE joijodp_zagimore_DW26.onewayaggregratebyCategory
ADD PRIMARY KEY(StoreKey, CalendarKey, CustomerKey, CategoryKey),
ADD FOREIGN KEY(StoreKey) REFERENCES Store_Dimension(StoreKey),
ADD FOREIGN KEY(CalendarKey) REFERENCES Calendar_Dimension(CalendarKey),
ADD FOREIGN KEY(CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
ADD FOREIGN KEY(CategoryKey) REFERENCES CategoryDimension(CategoryKey)




ALTER TABLE joijodp_zagimore_DW26.onewayaggregatebyregion
ADD PRIMARY KEY(CalendarKey, CustomerKey, ProductKey, RegionKey),
ADD FOREIGN KEY(CalendarKey) REFERENCES Calendar_Dimension(CalendarKey),
ADD FOREIGN KEY(CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
ADD FOREIGN KEY(ProductKey) REFERENCES Product_Dimension(ProductKey),
ADD FOREIGN KEY(RegionKey) REFERENCES RegionDimension(RegionKey)