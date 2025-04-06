/*
===========================================
Create Database and Schemas
==================
Script Purpose:
  This script creates a new database named "DataWareHouse" after checking if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
  within the database: "bronze", "silver" and "gold".*/

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO


-- Create the 'DataWarehouse' database
create database DataWareHouse;
go

use DataWareHouse;
go

-- Create Schemas
create schema bronze;
go

create schema silver;
go

create schema gold;

