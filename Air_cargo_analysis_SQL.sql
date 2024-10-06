create database air_cargo_analysis;
CREATE TABLE route_details (
    route_id INT PRIMARY KEY UNIQUE,
    flight_num VARCHAR(10) CHECK (flight_num LIKE '1%'),
    origin_airport VARCHAR(50),
    destination_airport VARCHAR(50),
    aircraft_id INT,
    distance_miles INT CHECK (distance_miles > 0)
);


select * from passengers_on_flights                   -- display all the passengers (customers) who have travelled in routes 01 to 25
where route_id between 1 and 25;


SELECT                                                -- identify the number of passengers and total revenue in business class 
    COUNT(no_of_tickets) AS num_passengers,
    SUM(price_per_ticket * no_of_tickets) AS total_revenue
FROM 
    ticket_details
WHERE 
    class_id = 'Bussiness';
    
    
SELECT CONCAT(first_name, ' ', last_name) AS full_name -- display the full name of the customer 
FROM customer;


SELECT c.customer_id, c.first_name, c.last_name            -- extract the customers who have registered and booked a ticket
FROM customer c
JOIN ticket_details t ON c.customer_id = t.customer_id;


SELECT c.first_name, c.last_name                          -- identify the customer’s first name and last name based on their customer ID and brand (Emirates)
FROM customer c
JOIN ticket_details t ON c.customer_id = t.customer_id
WHERE t.brand = 'Emirates';


SELECT class_id, COUNT(*) AS num_passengers   -- identify the customers who have travelled by Economy Plus class using Group By and Having clause
FROM passengers_on_flights
WHERE class_id = 'Economy Plus'
GROUP BY class_id
HAVING COUNT(*) > 0;


SELECT IF(SUM(price_per_ticket * no_of_tickets) > 10000, 'Revenue Crossed 10000', 'Revenue Below 10000') AS revenue_status  -- identify whether the revenue has crossed 10000 using the IF clause 
FROM ticket_details;


CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password123';   -- create and grant access to a new user to perform operations on a database
GRANT SELECT, INSERT, UPDATE ON air_cargo_analysis.* TO 'newuser'@'localhost';
FLUSH PRIVILEGES;

SELECT              -- find the maximum ticket price for each class using window functions
    class_id,
    price_per_ticket,
    MAX(price_per_ticket) OVER (PARTITION BY class_id) AS max_ticket_price    
FROM 
    ticket_details;
    
    
CREATE INDEX idx_route_id ON passengers_on_flights(route_id);   -- find the maximum ticket price for each class using window functions
Select 
route_id,	depart,	arrival,	seat_num,	class_id,	travel_date,	flight_num,aircraft_id
from passengers_on_flights
where route_id = 4;

EXPLAIN                  -- extract the passengers whose route ID is 4 by improving the speed and performance
SELECT 
    aircraft_id,                -- Specify the columns you want to retrieve
    customer_id,
    depart,
    arrival,
    seat_num,
    class_id,
    travel_date,
    flight_num
FROM 
    passengers_on_flights
WHERE 
    route_id = 4;              -- Filter for route ID 4idx_route_id
    
    
SELECT 
    customer_id,                                    -- calculate the total price of all tickets booked by a customer across different aircraft IDs using rollup function. 
    aircraft_id,                                  
    SUM(price_per_ticket * no_of_tickets) AS total_price
FROM 
    ticket_details
GROUP BY 
    customer_id, aircraft_id WITH ROLLUP;



create view business_class_brand As   -- create a view with only business class customers along with the brand of airlines
select 
class_id,
brand
from 
ticket_details
where
class_id = "bussiness";

select * from business_class_brand



DELIMITER //											-- create a stored procedure to get the details of all passengers flying between a range of routes defined in run time. Also, return an error message if the table doesn't exist.

CREATE PROCEDURE get_passengers_by_route_range(
    IN start_route INT,            
    IN end_route INT              
)
BEGIN
   
    IF (SELECT COUNT(*)
        FROM information_schema.tables 
        WHERE table_name = 'passengers_on_flights') = 0 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Error: Table passengers_on_flights does not exist.';
    ELSE
        
        SELECT *
        FROM passengers_on_flights
        WHERE route_id BETWEEN start_route AND end_route;
    END IF;
END //

DELIMITER ;

call get_passengers_by_route_range(1,10);



DELIMITER //  -- create a stored procedure that extracts all the details from the routes table where the travelled distance is more than 2000 miles.

CREATE PROCEDURE get_long_distance_routes()
BEGIN
    -- Query to extract all routes where distance is more than 2000 miles
    SELECT *
    FROM routes
    WHERE distance_miles > 2000;  -- Filter for distances greater than 2000 miles
END //

DELIMITER ;
call get_long_distance_routes();



Delimiter //          -- create a stored procedure that groups the distance travelled by each flight into three categories. The categories are, short distance travel (SDT) for >=0 AND <= 2000 miles, intermediate distance travel (IDT) for >2000 AND <=6500, and long-distance travel (LDT) for >6500.

create procedure get_distance_category()
begin 
select flight_num, distance_miles,
		case 
			when distance_miles >=0 AND distance_miles <= 2000 then 'SDT'
            when distance_miles >=2000 AND distance_miles <= 6500 then 'IDT'
            when distance_miles >=6500 then 'LDT'
            else 'Unknown'
            END As travel_categories
from routes;

End //

delimiter ;
call get_distance_category()


DELIMITER //

CREATE FUNCTION get_complimentary_services(class_id VARCHAR(50))
RETURNS VARCHAR(3) 
DETERMINISTIC  -- Add DETERMINISTIC to ensure it complies with binary logging requirements
BEGIN
    DECLARE service_status VARCHAR(3);
    
    IF class_id IN ('Business', 'Economy Plus') THEN
        SET service_status = 'Yes';
    ELSE
        SET service_status = 'No';
    END IF;

    RETURN service_status;
END //
DELIMITER ;
DELIMITER //

CREATE PROCEDURE get_ticket_details_with_services()
BEGIN
    -- Query to extract ticket purchase date, customer ID, class ID and complimentary services
    SELECT 
        p_date AS ticket_purchase_date,            -- Ticket purchase date
        customer_id,                                -- Customer ID
        class_id,                                   -- Class ID
        get_complimentary_services(class_id) AS complimentary_services  -- Call the function to get service status
    FROM 
        ticket_details;                             -- Source table
END //

DELIMITER ;

call get_ticket_details_with_services()



DELIMITER //

CREATE PROCEDURE get_first_customer_scott()
BEGIN
    DECLARE done INT DEFAULT 0;               -- Variable to check if we have fetched the record
    DECLARE customer_id INT;                  -- Variable to hold customer ID
    DECLARE first_name VARCHAR(50);           -- Variable to hold first name
    DECLARE last_name VARCHAR(50);            -- Variable to hold last name

    -- Declare the cursor
    DECLARE customer_cursor CURSOR FOR
    SELECT customer_id, first_name, last_name
    FROM customer
    WHERE last_name LIKE '%Scott';             -- Filter for last names ending with 'Scott'

    -- Declare a CONTINUE HANDLER to handle the end of the cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- Open the cursor
    OPEN customer_cursor;

    -- Fetch the first record
    FETCH customer_cursor INTO customer_id, first_name, last_name;

    -- Check if the record was fetched
    IF NOT done THEN
        SELECT customer_id, first_name, last_name;  -- Display the fetched record
    ELSE
        SELECT 'No customer found whose last name ends with Scott' AS message;  -- Handle no result case
    END IF;

    -- Close the cursor
    CLOSE customer_cursor;
END //

DELIMITER ;

call get_first_customer_scott()