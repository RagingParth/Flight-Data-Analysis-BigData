-- Pig Latin script to find which route has seen the maximum diversion.


REGISTER '/home/airline_usecase/piggybank.jar';

delayedFlights = LOAD '/home/airline_usecase/DelayedFlights.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedData = FOREACH delayedFlights GENERATE (chararray)$17 AS origin, (chararray)$18 AS dest, (int)$24 AS diversion;

divertedFlights = FILTER mappedData BY (origin is not null) AND (dest is not null) AND (diversion == 1);

divertedFlightsByRoutes = GROUP divertedFlights BY (origin, dest);

numOfDivertedFlightsByRoutes = FOREACH divertedFlightsByRoutes GENERATE group, COUNT(C.diversion);

sortedData = ORDER numOfDivertedFlightsByRoutes BY $1 DESC;

result = LIMIT sortedData 10;

DUMP result;