-- Create Northwind database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Northwind')
BEGIN
    CREATE DATABASE Northwind;
END
GO

USE Northwind;
GO

-- Create Employees table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Employees')
BEGIN
    CREATE TABLE Employees (
        EmployeeID INT PRIMARY KEY,
        LastName NVARCHAR(50) NOT NULL,
        FirstName NVARCHAR(50) NOT NULL,
        Title NVARCHAR(50),
        TitleOfCourtesy NVARCHAR(25),
        BirthDate DATETIME,
        HireDate DATETIME,
        Address NVARCHAR(60),
        City NVARCHAR(15),
        Region NVARCHAR(15),
        PostalCode NVARCHAR(10),
        Country NVARCHAR(15),
        HomePhone NVARCHAR(24),
        Extension NVARCHAR(4),
        Photo VARBINARY(MAX),
        Notes NVARCHAR(MAX),
        ReportsTo INT,
        PhotoPath NVARCHAR(255),
        SyncTime BIGINT DEFAULT 0
    );
END
GO

-- Insert sample employees
IF NOT EXISTS (SELECT * FROM Employees WHERE EmployeeID = 1)
BEGIN
    INSERT INTO Employees (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, SyncTime)
    VALUES 
    (1, 'Davolio', 'Nancy', 'Sales Representative', 'Ms.', '1968-12-08', '1992-05-01', '507 - 20th Ave. E. Apt. 2A', 'Seattle', 'WA', '98122', 'USA', '(206) 555-9857', '5467', 0),
    (2, 'Fuller', 'Andrew', 'Vice President, Sales', 'Dr.', '1952-02-19', '1992-08-14', '908 W. Capital Way', 'Tacoma', 'WA', '98401', 'USA', '(206) 555-9482', '3457', 0),
    (3, 'Leverling', 'Janet', 'Sales Representative', 'Ms.', '1963-08-30', '1992-04-01', '722 Moss Bay Blvd.', 'Kirkland', 'WA', '98033', 'USA', '(206) 555-3412', '3355', 0),
    (4, 'Peacock', 'Margaret', 'Sales Representative', 'Mrs.', '1958-09-19', '1993-05-03', '4110 Old Redmond Rd.', 'Redmond', 'WA', '98052', 'USA', '(206) 555-8122', '5176', 0),
    (5, 'Buchanan', 'Steven', 'Sales Manager', 'Mr.', '1955-03-04', '1993-10-17', '14 Garrett Hill', 'London', NULL, 'SW1 8JR', 'UK', '(71) 555-4848', '3453', 0);
END
GO

PRINT 'Northwind database initialized successfully';
