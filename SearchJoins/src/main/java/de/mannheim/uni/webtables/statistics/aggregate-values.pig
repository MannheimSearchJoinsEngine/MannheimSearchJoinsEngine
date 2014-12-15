SET job.priority HIGH;
-- use gzip compression in result and intermediary files
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

-----------------------------------------------------------------------------
-- AGGREGATE value data
-----------------------------------------------------------------------------

values = LOAD '$values' USING PigStorage('|') AS (value:chararray,file:chararray);

values = DISTINCT values;
valueGrp = GROUP values BY value;
valueCount = FOREACH valueGrp GENERATE group as value, COUNT(values) as count;
valueCount = ORDER valueCount BY count DESC;

STORE valueCount INTO '$results/valueCount';