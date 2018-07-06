-- Pig Latin script to find out the top 10 Origins with highest average departure delay.


REGISTER '/home/airline_usecase/piggybank.jar';

delayedFlights = LOAD '/home/airline_usecase/DelayedFlights.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedData = FOREACH delayedFlights GENERATE (int)$16 AS dep_delay, (chararray)$17 AS origin;

depDelayData = FILTER mappedData BY (dep_delay is not null) AND (origin is not null);

depDelayedFlights = GROUP depDelayData BY origin;

avgDepDelay = FOREACH depDelayedFlights GENERATE group, AVG(depDelayData.dep_delay);

result = ORDER avgDepDelay BY $1 DESC;

top10 = LIMIT result 10;

airports = LOAD '/home/airline_usecase/airports.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedAirports = FOREACH airports GENERATE (chararray)$0 AS origin, (chararray)$2 AS city, (chararray)$4 AS country;

joinedResult = JOIN mappedAirports BY origin, top10 BY $0;

reducedData = FOREACH joinedResult GENERATE $0,$1,$2,$4;

finalResult = ORDER reducedData BY $3 DESC;

DUMP finalResult;