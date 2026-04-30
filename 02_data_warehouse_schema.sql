-- ============================================================
-- Project: Zagimore Data Warehouse ETL SQL Project
-- File: 02_data_warehouse_schema.sql
-- Source File: zagimore_DW.sql
-- Purpose: Creates the data warehouse schema with dimensional model tables.
-- Notes: Original SQL logic preserved; comments and filenames were organized for GitHub presentation.
-- ============================================================

CREATE TABLE Customer_Dimension
(
  CustomerId CHAR(7) NOT NULL,
  CustomerName VARCHAR(15) NOT NULL,
  CustomerZip CHAR(5) NOT NULL,
  CustomerKey INT NOT NULL ,
  PRIMARY KEY (CustomerKey)
);

CREATE TABLE Product_Dimension
(
  ProductKey INT NOT NULL ,
  ProductId CHAR(3) NOT NULL,
  ProductName VARCHAR(25) NOT NULL,
  CategoryId CHAR(2) NOT NULL,
  VendorId CHAR(2) NOT NULL,
  VendorName VARCHAR(25) NOT NULL,
  CategoryName VARCHAR(25) NOT NULL,
  ProductType CHAR(1) NOT NULL,
  ProductSalesPrice NUMERIC(7,2),
  ProductDailyPrice NUMERIC(7,2),
  ProductWeeklyPrice NUMERIC(7,2),
  PRIMARY KEY (ProductKey)
);

CREATE TABLE Store_Dimension
(
  StoreKey INT NOT NULL,
  StoreId VARCHAR(3) NOT NULL,
  Storezip CHAR(5) NOT NULL,
  regionId CHAR(1) NOT NULL,
  RegionName VARCHAR(25) NOT NULL,
  PRIMARY KEY (StoreKey)
);

CREATE TABLE Calendar_Dimension
(
  CalendarKey INT NOT NULL,
  FullDate DATE NOT NULL,
  MonthYear CHAR(6) NOT NULL,
  CalendarYear CHAR(4) NOT NULL,
  PRIMARY KEY (CalendarKey)
);

CREATE TABLE IF NOT EXISTS Revenue_fact_table
(
  DollarAmount INT NOT NULL,
  Tid VARCHAR(8) NOT NULL,
  RevenueSource VARCHAR(20) NOT NULL,
  CustomerKey INT NOT NULL,
  ProductKey INT NOT NULL,
  StoreKey INT NOT NULL,
  CalendarKey INT NOT NULL,
  PRIMARY KEY (CustomerKey, ProductKey, StoreKey, CalendarKey, Tid, RevenueSource),
  FOREIGN KEY (CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
  FOREIGN KEY (ProductKey) REFERENCES Product_Dimension(ProductKey),
  FOREIGN KEY (StoreKey) REFERENCES Store_Dimension(StoreKey),
  FOREIGN KEY (CalendarKey) REFERENCES Calendar_Dimension(CalendarKey)
);


ALTER TABLE Revenue_fact_table
DEL