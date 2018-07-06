-- Pig Latin script to find out the top 5 most Visited Destinations.


REGISTER '/home/airline_usecase/piggybank.jar';

delayedFlights = LOAD '/home/airline_usecase/DelayedFlights.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedData = FOREACH delayedFlights GENERATE (int)$1 AS year, (int)$10 AS flight_num, (chararray)$17 AS origin,(chararray) $18 AS dest;

filteredData = FILTER mappedData BY dest is not null;

flightsByDestination = GROUP filteredData BY dest;

numOfFlightsByDestination = FOREACH flightsByDestination GENERATE group, COUNT(filteredData.dest);

sortedData = ORDER numOfFlightsByDestination BY $1 DESC;

result = LIMIT F 5;

airports = LOAD '/home/airline_usecase/airports.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedAirports = FOREACH airports GENERATE (chararray)$0 AS dest, (chararray)$2 AS city, (chararray)$4 AS country;

finalResult = JOIN result BY $0, mappedAirports BY dest;

DUMP finalResult;