SET job.priority HIGH;
-- use gzip compression in result and intermediary files
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

-----------------------------------------------------------------------------
-- AGGREGATE column data
-----------------------------------------------------------------------------

columns = LOAD '$columns' USING PigStorage('|') AS (header:chararray,type:chararray,file:chararray);

headerGrp = GROUP columns BY header;
headerCount = FOREACH headerGrp GENERATE group as header, COUNT(columns) as count;
headerCount = ORDER headerCount BY count DESC;

typeGrp = GROUP columns BY type;
typeCount = FOREACH typeGrp GENERATE group as type, COUNT(columns) as count;
typeCount = ORDER typeCount BY count DESC;

STORE headerCount INTO '$results/headerCount';
STORE typeCount INTO '$results/typeCount';