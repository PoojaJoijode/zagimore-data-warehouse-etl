-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 09_region_aggregate.sql
-- Source File: ETL-part4b.sql
-- Purpose: Creates region dimension and region-based revenue aggregate logic.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================

CREATE TABLE RegionDimension
(
    RegionKey INT AUTO_INCREMENT,
    RegionId CHAR(1),
    RegionName VARCHAR(25),
    PRIMARY KEY (RegionKey)
);


INSERT INTO joijodp_zagimore_DS26.RegionDimension (RegionId, RegionName)
SELECT DISTINCT regionId, RegionName
FROM Store_Dimension;

SELECT SUM(r.DollarAmount), r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey
FROM Revenue_fact_table r, Store_Dimension sd, RegionDimension rd
WHERE r.StoreKey = sd.StoreKey
AND sd.regionId = rd.RegionId
GROUP BY r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey;



CREATE TABLE onewayaggregatebyregion AS
SELECT SUM(r.DollarAmount), r.CustomerKey, r.ProductKey, r.CalendarKey, rd.RegionKey
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