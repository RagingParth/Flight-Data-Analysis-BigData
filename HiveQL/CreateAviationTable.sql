CREATE TABLE aviation(
year INT,
month INT,
flight_num INT,
origin STRING,
destination STRING,
cancelled INT,
cancel_code INT,
diversion INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;