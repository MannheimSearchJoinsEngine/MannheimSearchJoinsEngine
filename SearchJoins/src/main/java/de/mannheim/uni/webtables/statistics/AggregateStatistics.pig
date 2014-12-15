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

-----------------------------------------------------------------------------
-- AGGREGATE value data
-----------------------------------------------------------------------------

values = LOAD '$values' USING PigStorage('|') AS (value:chararray,file:chararray);

values = DISTINCT values;
valueGrp = GROUP values BY value;
valueCount = FOREACH valueGrp GENERATE group as value, COUNT(values) as count;
valueCount = ORDER valueCount BY count DESC;

STORE valueCount INTO '$results/valueCount';