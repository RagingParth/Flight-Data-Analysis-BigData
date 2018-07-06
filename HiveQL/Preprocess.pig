REGISTER /home/pig/piggybank.jar;

delayedFlights = LOAD '/DelayedFlights.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

mappedData = FOREACH delayedFlights GENERATE (int)$1 AS year, (int)$2 AS month, (int)$10 AS flight_num, (chararray)$17 AS origin, (chararray)$18 AS dest, (int)$22 AS cancelled, (chararray)$23 AS cancel_code, (int)$24 AS diversion;

reducedData = FILTER mappedData BY (year IS NOT NULL) AND (month IS NOT NULL) AND (flight_num IS NOT NULL) AND (origins IS NOT NULL) AND (dest IS NOT NULL) AND (cancelled IS NOT NULL) AND (cancel_code IS NOT NULL) AND (diversion IS NOT NULL);

STORE reducedData INTO 'aviation'
USING org.apache.hive.hcatalog.pig.HCatStorer();