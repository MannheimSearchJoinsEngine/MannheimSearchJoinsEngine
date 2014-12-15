SET job.priority HIGH;
-- use gzip compression in result and intermediary files
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

-----------------------------------------------------------------------------
-- AGGREGATE table data
-----------------------------------------------------------------------------

tables = LOAD '$tables' USING PigStorage('|') AS (url:chararray,tld:chararray,rows:chararray,cols:chararray,file:chararray);

rowGrp = GROUP tables BY rows;
rowCount = FOREACH rowGrp GENERATE group as numRows, COUNT(tables) as count;
rowCount = ORDER rowCount BY count DESC;

colGrp = GROUP tables BY cols;
colCount = FOREACH colGrp GENERATE group as numCols, COUNT(tables) as count;
colCount = ORDER colCount BY count DESC;

STORE rowCount INTO '$results/rowCount';
STORE colCount INTO '$results/colCount';