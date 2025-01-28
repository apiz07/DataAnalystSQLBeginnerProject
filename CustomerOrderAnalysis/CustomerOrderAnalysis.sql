-- Customer Order Analysis
-- Analyze a dataset of customer orders to answer
-- business questions like total sales, most popular products, and customer spending patterns.

-- Data Cleaning


-- Setup new table so wont have to work or alter raw data which will cause more loss
-- Start from table customers

-- This query is to select all column from table customers

SELECT *
FROM customers;

-- This query is to create new table to for the table customers

CREATE TABLE stag_cust
LIKE customers;

-- This query is to select all column from the new table

SELECT *
FROM stag_cust;

-- This query is to insert all the data from table customers

INSERT stag_cust
SELECT *
FROM customers;

-- Now to check and remove duplicates from table stag_cust

-- This query is to create a column row number using windows function to see whether there are duplicates row or not

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY CustomerName, Email, City, Country) AS row_num
FROM stag_cust;

-- Next create CTE to query row which will have more than 1

WITH duplicate_cte_cust AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY CustomerName, Email, City, Country) AS row_num
FROM stag_cust
)
SELECT *
FROM duplicate_cte_cust
WHERE row_num > 1;

-- Analysis for table customers -  There are no duplicate in table customers

-- Next, we query for table orders

SELECT *
FROM orders;

-- Create new table to keep the raw data of table orders

CREATE TABLE stag_order
LIKE orders;

-- Select all from new table stag_orders

SELECT *
FROM stag_order;

-- Insert all data from table orders to stag_order

INSERT stag_order
SELECT *
FROM orders;

-- Then check on table stag_order to see if there any duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY OrderID, CustomerID, OrderDate, TotalAmount, OrderStatus) AS row_num
FROM stag_order;

WITH duplicate_cte_order AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY OrderID, CustomerID, OrderDate, TotalAmount, OrderStatus) AS row_num
FROM stag_order
)
SELECT *
FROM duplicate_cte_order
WHERE row_num > 1;

-- Same like table customers, there are no duplicates in table orders

-- Next repeat step on table products and order_details

SELECT *
FROM products;

CREATE TABLE stag_product
LIKE products;

SELECT *
FROM stag_product;

INSERT stag_product
SELECT *
FROM products;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ProductID, ProductName, Category, Price) AS row_num
FROM stag_product;

WITH duplicate_cte_product AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ProductID, ProductName, Category, Price) AS row_num
FROM stag_product
)
SELECT *
FROM duplicate_cte_product
WHERE row_num > 1;

SELECT *
FROM order_details;

CREATE TABLE stag_orderdetails
LIKE order_details;

SELECT *
FROM stag_orderdetails;

INSERT stag_orderdetails
SELECT *
FROM order_details;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY DetailID, OrderID, ProductID, Quantity, UnitPrice, TotalPrice) AS row_num
FROM stag_orderdetails;

WITH duplicate_cte_orderdetails AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY DetailID, OrderID, ProductID, Quantity, UnitPrice, TotalPrice) AS row_num
FROM stag_orderdetails
)
SELECT *
FROM duplicate_cte_orderdetails
WHERE row_num > 1;

-- Analysis for customerorder schema - All raw data is not duplicate therefore no need for data cleaning

-- Now to do EDA and the task

-- 1. Retrieve the total number of orders.

SELECT COUNT(*) AS total_order
FROM stag_order;

-- Analysis on total order = There are 2000 total number of orders

-- 2. Find the top 5 customers who spent the most.

SELECT stag_cust.CustomerName, SUM(stag_order.TotalAmount) AS Total_Spent
FROM stag_cust
JOIN stag_order ON stag_cust.CustomerID = stag_order.CustomerID
GROUP BY stag_cust.CustomerName
ORDER BY Total_Spent DESC
LIMIT 5;

-- As I was doing EDA, I found out that there are two customer with same name but different ID

SELECT *
FROM stag_cust
WHERE CustomerName = 'David Perkins';

-- Therefore I decide to create a CTE

SELECT CustomerID, SUM(TotalAmount) AS total_spent
FROM orders
GROUP BY CustomerID;

WITH customer_detail AS
(
SELECT CustomerID, SUM(TotalAmount) AS total_spent
FROM orders
GROUP BY CustomerID
)
SELECT stag_cust.CustomerID, stag_cust.CustomerName, ROUND(customer_detail.total_spent, 2)
FROM customer_detail
JOIN stag_cust ON stag_cust.CustomerID = customer_detail.CustomerID
ORDER BY Total_Spent DESC
LIMIT 5;

-- Analysis on top 5 customer for total spent = Joshua Roberts, David Perkins, Brandon Brown, Henry Santiago, and Derek Wright

-- 3. Calculate the average order value.

SELECT *
FROM stag_order;

SELECT ROUND(AVG(TotalAmount), 2) AS avg_order_value
FROM stag_order;

-- Analysis on average order value = it is 2511.74

-- 4. Identify the most popular product (by quantity sold).

SELECT *
FROM stag_product;

SELECT *
FROM stag_orderdetails;

SELECT *,
DENSE_RANK() OVER(
PARTITION BY DetailID, OrderID, ProductID, Quantity, UnitPrice, TotalPrice) AS Ranking
FROM stag_orderdetails;

WITH product_rank AS
(
SELECT ProductID, SUM(Quantity) AS Quantities
FROM stag_orderdetails
GROUP BY ProductID
)
SELECT stag_product.ProductName, stag_product.ProductID, product_rank.Quantities
FROM product_rank
JOIN stag_product ON stag_product.ProductID = product_rank.ProductID
ORDER BY Quantities DESC
LIMIT 1;

-- Analysis on most popular product = most popular product is puzzle which have 1082 quantities being order.

-- 5. Find orders placed in May 2023 and June 2023, then what is the total order for that month. Lastly compare both month.

SELECT *
FROM stag_order;

SELECT COUNT(*) AS Total_Order
FROM stag_order
WHERE OrderDate BETWEEN '2023-05-01' AND '2023-05-31'
ORDER BY OrderDate ASC;

SELECT COUNT(*) AS Total_Order
FROM stag_order
WHERE OrderDate BETWEEN '2023-06-01' AND '2023-06-30'
ORDER BY OrderDate ASC;

-- Analysis = there are 91 total order for May 2023 and 69 total order for June 2023. There are decreasing of order between both month

-- 6. Group orders by country and calculate total sales per country.

SELECT *
FROM stag_cust;

SELECT *
FROM stag_order;

WITH country_sale AS
(
SELECT CustomerID, SUM(TotalAmount) AS TotalSales
FROM stag_order
GROUP BY CustomerID
)
SELECT stag_cust.Country, ROUND(SUM(country_sale.TotalSales), 2) AS TotalSales
FROM country_sale
JOIN stag_cust ON stag_cust.CustomerID = country_sale.CustomerID
GROUP BY stag_cust.Country
ORDER BY TotalSales DESC;

-- Analysis = UK have the most total sales at 1123100.65



