-- Pig Latin script to find out the month with most number of cancellations due to bad weather.


REGISTER '/home/airline_usecase/piggybank.jar';

delayedFlights = LOAD '/home/airline_usecase/DelayedFlights.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedData = FOREACH delayedFlights GENERATE (int)$2 AS month,(int)$10 AS flight_num,(int)$22 AS cancelled,(chararray)$23 AS cancel_code;

cancelledFlights = FILTER mappedData BY cancelled == 1 AND cancel_code =='B';

cancelledFlightsByMonth = GROUP cancelledFlights BY month;

numOfCancelledFlightsByMonth = FOREACH cancelledFlightsByMonth GENERATE group, COUNT(C.cancelled);

sortedNumofCancelledFlightsByMonth = ORDER numOfCancelledFlightsByMonth BY $1 DESC;

result = LIMIT sortedNumofCancelledFlightsByMonth 1;

DUMP result;