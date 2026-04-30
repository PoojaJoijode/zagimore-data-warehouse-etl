-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 07_product_dimension_refresh_scd_type2.sql
-- Source File: ZAGIMORE ETL PRODUCT DIMENSION REFRESH 5.sql
-- Purpose: Contains product dimension refresh and slowly changing dimension Type 2 logic.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================


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
SET ExtractionTimeStamp = ''NOW() - INTERVAL 14 DAY'';

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










ALTER TABLE Product_Dimension
ADD dvf DATE,
ADD dvu DATE,
ADD  currentstatus BOOLEAN

UPDATE Product_Dimension
SET dvf = '2013-01-01',
dvu = '2035-01-01',
currentstatus = TRUE



BEGIN

INSERT INTO joijodp_zagimore_DS26.Product_Dimension(ProductId, ProductName, CategoryID, VendorID, CategoryName, VendorName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice, ExtractionTimeStamp, pd_loaded, dvu, dvf, currentstatus)
SELECT r.productid, r.productname, r.categoryid, r.vendorid, c.categoryname, v.vendorname, 'R', NULL, r.productpricedaily, r.productpriceweekly, NOW(), FALSE, '2035-01-01', NOW(), TRUE 
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c 
WHERE r.categoryid = c.categoryid 
AND v.vendorid = r.vendorid 
AND r.productid NOT IN (SELECT DISTINCT productid FROM joijodp_zagimore_DS26.Product_Dimension WHERE ProductType = 'R')
UNION 
SELECT p.productid, p.productname, p.categoryid, p.vendorid, c.categoryname, v.vendorname, 'S', p.productprice, NULL, NULL, NOW(), FALSE, '2035-01-01', NOW(), TRUE 
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c 
WHERE p.categoryid = c.categoryid
AND v.vendorid = p.vendorid
AND p.productid NOT IN (SELECT DISTINCT productid FROM joijodp_zagimore_DS26.Product_Dimension WHERE ProductType = 'S');


INSERT INTO joijodp_zagimore_DW26.Product_Dimension(ProductKey, ProductId, ProductName, CategoryID, VendorID, CategoryName, VendorName, ProductType, ProductSalesPrice, ProductDailyPrice, ProductWeeklyPrice, dvu, dvf, currentstatus)
SELECT pd.ProductKey, pd.ProductId, pd.ProductName, pd.CategoryID, pd.VendorID, pd.CategoryName, pd.VendorName, pd.ProductType, pd.ProductSalesPrice, pd.ProductDailyPrice, pd.ProductWeeklyPrice, pd.dvu, pd.dvf, pd.currentstatus
FROM joijodp_zagimore_DS26.Product_Dimension pd
WHERE pd.pd_loaded = 0;

UPDATE joijodp_zagimore_DS26.Product_Dimension
SET pd_loaded = True
WHERE pd_loaded = False;

END



UPDATE `product` SET `productname` = 'sleeping Bag' WHERE `product`.`productid` = '1X1';
UPDATE `product` SET `productprice` = '155.00' WHERE `product`.`productid` = '1X2';


CREATE PROCEDURE productdimensiontype2refresh()

BEGIN
--inserting new rows in ension witproduct dimh updated values that have changed
INSERT INTO joijodp_zagimore_DS26.Product_Dimension(productid, productname, categoryid, vendorid, categoryname, vendorname, ProductType, productsalesprice,
productdailyprice, productweeklyprice, ExtractionTimeStamp, pd_loaded, dvu, dvf, currentstatus)
SELECT p.productid, p.productname, p.categoryid, p.vendorid, c.categoryname, v.vendorname, 'S', p.productprice, NULL, NULL, NOW(), FALSE, '2035-01-01', NOW(), TRUE 
FROM joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c, joijodp_zagimore_DS26.Product_Dimension pd
WHERE p.categoryid = c.categoryid
AND v.vendorid = p.vendorid
AND p.productid = pd.productid
AND (p.productprice != pd.productsalesprice OR p.productname != pd.productname OR c.categoryname != pd.categoryname
OR v.vendorid != pd.vendorid OR v.vendorname != pd.vendorname)
AND pd.ProductType = 'S'
AND pd.currentstatus = TRUE


UNION

SELECT r.productid, r.productname, r.categoryid, r.vendorid, c.categoryname, v.vendorname, 'R', NULL, r.productpricedaily, r.productpriceweekly, NOW(), FALSE, '2035-01-01', NOW(), TRUE 
FROM joijodp_zagimore2026.rentalProducts r, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c, joijodp_zagimore_DS26.Product_Dimension pd
WHERE c.categoryid = r.categoryid 
AND v.vendorid = r.vendorid 
AND r.productid = pd.productid
AND pd.ProductType = 'R'
AND (r.productpricedaily != pd.productdailyprice OR r.productpriceweekly != pd.productweeklyprice OR c.categoryname != pd.categoryname OR pd.productname != r.productname
OR v.vendorid != pd.vendorid OR v.vendorname != pd.vendorname)
AND pd.currentstatus = TRUE;




--updaeting existing corespondaning rows in product dimesnion for instanses of product dimension that have changed the  attribute
--making old version of productdimension , not current 

UPDATE joijodp_zagimore_DS26.Product_Dimension pd, joijodp_zagimore2026.product p, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
SET pd.dvu = NOW() - INTERVAL 1 DAY,
pd.currentstatus = FALSE,
pd.pd_loaded = FALSE
WHERE p.categoryid = c.categoryid
AND v.vendorid = p.vendorid
AND p.productid = pd.productid
AND (p.productprice != pd.productsalesprice OR p.productname != pd.productname OR c.categoryname != pd.categoryname
OR v.vendorid != pd.vendorid OR v.vendorname != pd.vendorname)
AND pd.ProductType = 'S'
AND pd.pd_loaded= TRUE;



UPDATE joijodp_zagimore_DS26.Product_Dimension pd, joijodp_zagimore2026.rentalProducts rp, joijodp_zagimore2026.vendor v, joijodp_zagimore2026.category c
SET pd.dvu = NOW() - INTERVAL 1 DAY,
pd.currentstatus = FALSE,
pd.pd_loaded = FALSE
WHERE c.categoryid = rp.categoryid
AND v.vendorid = rp.vendorid
AND rp.productid = pd.productid
AND (rp.productpricedaily != pd.productdailyprice OR rp.productpriceweekly != pd.productweeklyprice OR c.categoryname != pd.categoryname OR pd.productname != rp.productname
OR v.vendorid != pd.vendorid OR v.vendorname != pd.vendorname)
AND pd.ProductType = 'R'
AND pd.pd_loaded= TRUE;



--loading new instances of prduct dim into pd table in dw 
--updating old instances of pd in pd table that are not current anymore
REPLACE INTO joijodp_zagimore_DW26.Product_Dimension(productkey, productid, productname, categoryid, vendorid, categoryname, vendorname, ProductType, productsalesprice,
productdailyprice, productweeklyprice, dvu, dvf, currentstatus)
SELECT productkey, productid, productname, categoryid, vendorid, categoryname, vendorname, ProductType, productsalesprice,
productdailyprice, productweeklyprice, dvu, dvf, currentstatus
FROM joijodp_zagimore_DS26.Product_Dimension pd
WHERE pd.pd_loaded = FALSE;

UPDATE joijodp_zagimore_DS26.Product_Dimension
SET pd_loaded = True
WHERE pd_loaded = False;

END





--customer dimesnion type 2 change 

ALTER TABLE Customer_Dimension
ADD dvf DATE,
ADD dvu DATE,
ADD  currentstatus BOOLEAN

UPDATE Customer_Dimension
SET dvf = '2013-01-01',
dvu = '2035-01-01',
currentstatus = TURE


UPDATE `customer` SET `customername` = 'Tony_1' WHERE `customer`.`customerid` = '2-3-444';




CREATE PROCEDURE Customerdimensiontype2refresh()
BEGIN
--inserting new rows in product dimension with updated values that have changed
INSERT INTO joijodp_zagimore_DS26.Customer_Dimension(CustomerId, CustomerName, CustomerZip, ExtractionTimeStamp, c_loaded, dvf, dvu,
currentstatus )
SELECT c.CustomerId, c.CustomerName, c.CustomerZip,NOW(), FALSE , NOW(), '2035-01-01', currentstatus
FROM joijodp_zagimore2026.customer c, joijodp_zagimore_DS26.Customer_Dimension cd
WHERE c.customerid=cd.CustomerID
AND cd.CurrentStatus=TRUE
AND (c.customername!=cd.CustomerName OR c.customerzip!=cd.CustomerZip);


UPDATE Customer_Dimension cd1, Customer_Dimension cd2
SET cd1.DVU = DATE(NOW())-INTERVAL 1 DAY,
    cd1.CurrentStatus = FALSE
WHERE cd1.CustomerID = cd2.CustomerID
AND cd1.DVF < cd2.DVF
AND cd1.CurrentStatus = TRUE;


REPLACE INTO joijodp_zagimore_DW26.Customer_Dimension(CustomerKey, CustomerId, CustomerName, CustomerZip, DVF, DVU, CurrentStatus)
    SELECT CustomerKey, CustomerID, CustomerName, CustomerZip, DVF, DVU, CurrentStatus
    FROM joijodp_zagimore_DS26.Customer_Dimension;

    UPDATE joijodp_zagimore_DS26.Customer_Dimension
    SET c_loaded = TRUE
    WHERE c_loaded = FALSE;
END








--type 2 change for store dimension



ALTER TABLE Store_Dimension
ADD dvf DATE,
ADD dvu DATE,
ADD  currentstatus BOOLEAN

UPDATE Store_Dimension
SET dvf = '2013-01-01',
dvu = '2035-01-01',
currentstatus = TURE



CREATE PROCEDURE storedimensiontype2refresh()
BEGIN
--inserting new rows in product dimension with updated values that have changed
INSERT INTO joijodp_zagimore_DS26.Store_Dimension(StoreId , Storezip, regionId, RegionName, ExtractionTimeStamp, s_loaded, dvf, dvu, currentstatus )
SELECT s.storeId, s.StoreZip, r.regionID, r.regionname, NOW(), FALSE , NOW(), '2035-01-01', currentstatus
FROM joijodp_zagimore2026.store s, joijodp_zagimore_DS26.Store_Dimension sd, joijodp_zagimore2026.region r
WHERE s.storeid=sd.StoreID
AND s.regionid=r.regionid
AND (s.storezip!=sd.StoreZip OR s.regionid!=sd.RegionID)
AND sd.CurrentStatus=TRUE;



UPDATE Store_Dimension sd1, Store_Dimension sd2
SET sd1.DVU = DATE(NOW())-INTERVAL 1 DAY,
    sd1.CurrentStatus = FALSE
WHERE sd1.StoreID = sd2.StoreID
AND (sd1.DVF < sd2.DVF
    OR(
        sd1.DVF = sd2.DVF
        AND sd1.ExtractionTimeStamp < sd2.ExtractionTimeStamp
    )

) 
AND sd1.CurrentStatus = TRUE;


REPLACE INTO joijodp_zagimore_DW26.Store_Dimension(StoreKey, StoreID, StoreZip, RegionID, RegionName, DVF, DVU, CurrentStatus)
SELECT StoreKey, StoreID, StoreZip, RegionID, RegionName, DVF, DVU, CurrentStatus
FROM joijodp_zagimore_DS26.Store_Dimension;

UPDATE joijodp_zagimore_DS26.Store_Dimension
SET s_loaded = TRUE
WHERE s_loaded = FALSE;
END

END

















INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('FCP', '2-3-444', 'S2', '2026-04-28');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('2Y3', 'OO1', '5'), ('2X7', 'AA1', '2');

CALL `daily_fact_refresh`();
CALL `validate_fact_row_count`();


--fact table late arriving fact refresh


INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('PP2', '4-3-555', 'S3', '2016-07-10');
INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) VALUES ('2Y3', 'OO1', '5'), ('2X7', 'AA1', '2');

INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('PP3', '2-4-899', '210', '2022-09-18');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('1V2', 'PP3', 'W', '3'), ('2X4', 'PP3', 'X', '2');

CALL `late_arriving_fact_refresh`();
CALL `validate_fact_row_count`();

-product dimension refresh

INSERT INTO `rentalProducts` (`productid`, `productname`, `vendorid`, `categoryid`, `productpricedaily`, `productpriceweekly`) VALUES ('5Y9', 'testETL', 'WR', 'TY', '12', '40');
INSERT INTO `product` (`productid`, `productname`, `productprice`, `vendorid`, `categoryid`) VALUES ('9Q7', 'testETLsalea', '62', 'WP', 'TL');

CALL `product_dimension_refresh`();
CALL `validate_product_dimension_row_count`();

-- store dimension refresh
INSERT INTO `store` (`storeid`, `storezip`, `regionid`) VALUES ('S41', '60235', 'B');
CALL `store_dimension_refresh`();
CALL `validate_store_dimension_row_count`();

--customer dimension refresh
INSERT INTO `customer` (`customerid`, `customername`, `customerzip`) VALUES ('7-3-955', 'testETLp', '18955');
CALL `customer_dimension_refresh`();
CALL `validate_customer_dimension_row_count`();

-- customer type2 change refresh
UPDATE `customer` SET `customerzip` = '53955' WHERE `customer`.`customerid` = '7-3-955';
UPDATE `customer` SET `customername` = 'Molton' WHERE `customer`.`customerid` = '9-0-111';

CALL `customer_dimension_type2_refresh`();
CALL `validate_customer_dimension_row_count`();







