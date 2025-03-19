-- Databricks notebook source
-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.types import*
-- MAGIC from pyspark.sql.functions import*
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Problem: Retrieve a list of all employees and their department names from the employees and departments tables. If an employee does not belong to a department, still include them in the result.
-- MAGIC
-- MAGIC Tables: employees (employee_id, name, department_id), departments (department_id, department_name)
-- MAGIC
-- MAGIC SELECT e.name, d.department_name 
-- MAGIC FROM employees e
-- MAGIC LEFT JOIN departments d
-- MAGIC ON e.department_id = d.department_id;

-- COMMAND ----------

/*
%sql
Problem: Retrieve a list of departments along with the highest salary paid in each department and the name of the employee who received it. Ensure that departments with no employees are still included.

Tables: employees (employee_id, name, department_id, salary), departments (department_id, department_name)

with employee_data AS(
  SELECT d.department_id,e.name,e.salary,d.department_name FROM Employees e
  RIGHT JOIN Departments d
  ON e.department_id=d.department_id
)
, rankings AS(
  SELECT ed.name,ed.salary,ed.department_name,
                           ROW_NUMBER() OVER(PARTITION BY ed.department_id ORDER BY ed.salary DESC) AS rank
                           from employee_data ed
)
SELECT r.name,r.salary,r.department_name,r.rank FROM rankings r WHERE r.rank=1
*/

-- COMMAND ----------

/*
Problem: Find the second highest salary in the company using a subquery. Ensure you account for duplicate salaries.

Table: employees (employee_id, name, salary)
Answer:
SELECT name, salary
FROM (
      SELECT e.name, e.salary, RANK() OVER(ORDER BY e.salary DESC) AS rank
      FROM employees e    
) AS ranked_employees
WHERE rank = 2;
#another approach
SELECT MAX(salary) AS second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
#another approach to same result
*/

-- COMMAND ----------

/*
Problem: Use a CTE to identify employees who have been with the company for more than 5 years and have had a total sales amount greater than the company average. Return their name, hire date, and total sales.

Tables: employees (employee_id, name, hire_date), sales (sale_id, employee_id, sale_amount)
Answer:
WITH EmployeeDetails AS(
    SELECT e.employee_id,e.name,e.hire_date,sum(s.sale_amount) AS total_sales
    FROM Employees e
    INNER JOIN Sales s
    ON s.employee_id=e.employee_id 
    GROUP BY e.employee_id
),
CompanyAverage AS (
    SELECT AVG(total_sales) AS company_average
    FROM EmployeeDetails
), 
ExperiencedEmployees AS(
     SELECT 
     ed.employee_id,
     ed.name,
     ed.hire_date,
     ed.total_sales,
     year(hire_date)-year(getcurrent_date()) AS experience_years
     FROM EmployeeDetails ed
)
SELECT name,hire_date,total_sales 
FROM ExperiencedEmployee ee,CompanyAverage ca
WHERE ee.total_sales > ca.company_average 
AND ee.experience_years > 5;

#another approach

WITH EmployeeDetails AS (
    SELECT e.employee_id, e.name, e.hire_date, SUM(s.sale_amount) AS total_sales
    FROM employees e
    INNER JOIN sales s ON s.employee_id = e.employee_id 
    GROUP BY e.employee_id, e.name, e.hire_date
),
ExperiencedEmployees AS (
    SELECT ed.employee_id, ed.name, ed.hire_date, ed.total_sales,
           YEAR(CURRENT_DATE) - YEAR(ed.hire_date) AS experience_years,
           AVG(total_sales) OVER () AS company_average -- Calculate overall average without a subquery
    FROM EmployeeDetails ed
)

SELECT name, hire_date, total_sales 
FROM ExperiencedEmployees
WHERE total_sales > company_average 
AND experience_years > 5;
#Another approach to filter employees only who have gretaer than 5 years of experience
WITH EmployeeDetails AS (
    SELECT e.employee_id, e.name, e.hire_date, SUM(s.sale_amount) AS total_sales
    FROM employees e
    INNER JOIN sales s ON s.employee_id = e.employee_id 
    GROUP BY e.employee_id, e.name, e.hire_date
),
ExperiencedEmployees AS (
    SELECT ed.employee_id, ed.name, ed.hire_date, ed.total_sales,
           YEAR(CURRENT_DATE) - YEAR(ed.hire_date) AS experience_years
    FROM EmployeeDetails ed
    WHERE YEAR(CURRENT_DATE) - YEAR(ed.hire_date) > 5
),
CompanyAverage AS (
    SELECT AVG(total_sales) AS company_average
    FROM EmployeeDetails
)

SELECT ee.name, ee.hire_date, ee.total_sales 
FROM ExperiencedEmployees ee, CompanyAverage ca
WHERE ee.total_sales > ca.company_average;
*/

-- COMMAND ----------

/*
Find the total number of orders and the total revenue for each product category in a specific year. Also, include categories where no orders were placed in that year.

Tables:

orders (order_id, order_date, product_id, quantity, total_price)
products (product_id, product_name, category_id)
categories (category_id, category_name)*/
Answer:
/*
WITH orders_statistics AS(
 SELECT
  p.category_id,
  DATE_TRUNC('year',o.order_date) AS year,
  SUM(o.total_price) AS total_revenue,
  COUNT(o.order_id) AS total_number_of_orders
FROM
 Orders o
 inner join Products p
 on p.product_id=o.product_id
 GROUP BY p.category_id ,year
)
, categories AS(
   SELECT category_id FROM categories c
)
SELECT 
  os.year,
  c.category_id,
  coalesce(os.total_revenue,0) AS total_revenue,
  coalesce(os.total_number_of_orders,0) AS total_number_of_orders
FROM categories c
LEFT JOIN orders_statistics os
on os.category_id=c.category_id
ORDER BY c.category_id
*/    

-- COMMAND ----------

/*
Problem: Show the difference between each employee's salary and the average salary in their department using window functions. Include the department name, employee name, salary, and difference from the department average.

Tables: employees (employee_id, name, department_id, salary), departments (department_id, department_name)
Answer:
WITH employees_statistics AS(
  SELECT e.employee_id,
         e.name AS employee_name,
         d.department_name,
         e.salary,
         AVG(e.salary) OVER(PARTITION BY e.department_id) AS department_average_salary,
         e.salary-department_average_salary AS difference
         FROM Employees e
         INNER JOIN Departments d ON e.department_id=d.department_id
)
SELECT department_name,salary,employee_name,difference FROM employees_statistics
*/

-- COMMAND ----------

/*
Problem: Rank products based on total sales in descending order, but restart the ranking for each product category.

Tables: products (product_id, product_name, category_id), sales (sale_id, product_id, amount)
Answer:
SELECT 
   p.product_id,
   p.product_name,
   p.category_id,
   SUM(s.amount)AS total_sales,
   RANK() OVER(PARTITION BY p.category_id ORDER BY total_sales DESC) AS product_rank
FROM
   Products p
INNER JOIN 
   Sales s ON p.product_id=s.product_id
GROUP BY 
   p.product_id,p.product_name,p.category_id
ORDER BY 
   p.category_id,product_rank;

Another approach

SELECT 
    p.product_id,
    p.product_name,
    p.category_id,
    SUM(s.amount) OVER (PARTITION BY p.product_id) AS total_sales,
    RANK() OVER (PARTITION BY p.category_id ORDER BY SUM(s.amount) OVER (PARTITION BY p.product_id) DESC) AS product_rank
FROM 
    products p
INNER JOIN 
    sales s ON p.product_id = s.product_id
ORDER BY 
    p.category_id, product_rank;
  */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Complex COALESCE and Conditional Logic

-- COMMAND ----------

/*
Problem: Retrieve the list of employees and their total sales. If an employee has no sales, return 0 for the total sales amount. If an employee does not exist in the sales table, return 'No Sales' for the sales date.

Tables: employees (employee_id, name), sales (sale_id, employee_id, sale_date, sale_amount)
Answer:
WITH employee_sales_details AS(
SELECT 
    e.employee_id,
    MAX(s.sale_date) AS sale_date,
    SUM(s.sale_amount) AS total_sales
FROM
   Employees e
   LEFT JOIN Sales s
   ON e.employee_id=s.employee_id
   GROUP BY e.employee_id
)
SELECT 
  employee_id,
  coalesce(total_sales,0)AS total_sales,
  coalesce(sale_date,'No Sales') AS sale_date
FROM
  employee_sales_details  
*/

-- COMMAND ----------

/*
Problem: Create a query that retrieves customers and their most recent purchase date. If they have never made a purchase, return ‘No Purchases’ for the purchase date.

Tables: customers (customer_id, customer_name), sales (sale_id, customer_id, sale_date)
Answer:
WITH customer_purchase_details AS(
  SELECT c.customer_id,
    c.customer_name,
    MAX(s.sale_date) AS recent_purchase_date
    FROM Customers c
    LEFT JOIN Sales s
    ON c.customer_id=s.customer_id
    GROUP BY c.customer_id
)
SELECT customer_id,
       customer_name,
       coalesce(recent_purchase_date,'No Purchases')
       FROM customer_purchase_details
*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC UNION with Complex Conditions

-- COMMAND ----------

/*
 UNION with Complex Conditions
Problem: Use UNION to retrieve a list of products that are either sold online or offline. If a product is sold in both places, include the total sales from both channels.

Tables: online_sales (sale_id, product_id, amount), offline_sales (sale_id, product_id, amount)
Answer:
SELECT product_id,sum(total_sales) AS total_sales from(
  SELECT product_id,sum(amount)AS total_sales from online group by product_id
union all
SELECT product_id,sum(amount)AS total_sales from offline group by product_id
)AS combined_sales
GROUP BY product_id

*/