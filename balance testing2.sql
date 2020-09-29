USE [Unik_DW] 
GO 
SET ANSI_NULLS ON 
GO 
SET QUOTED_IDENTIFIER ON 
GO 
ALTER PROCEDURE [recon].[Compare_Extract_DW_With_indicators] 
AS 
DECLARE 
@no_of_rows int = 0, 
@i int = 0, 
@threshold_pct int, 
@offset int = 0, 
@NumberOfRows int, 
@id int = 1, 
@alterTable nvarchar(MAX) 

BEGIN 

IF OBJECT_ID('tempdb..#TEMP_CopyEDW') IS NOT NULL --Remove temp table1 here 
DROP TABLE #TEMP_CopyEDW -- Remoeve "tempdb.table1" 

IF OBJECT_ID('tempdb..#TEMP_TABLE_WITH_INDICATOR') IS NOT NULL --Remove temp table1 here 
DROP TABLE #TEMP_TABLE_WITH_INDICATOR -- Remoeve "tempdb.table1" 

-- creating temp table that is copy of compare source and target table 
CREATE TABLE #TEMP_CopyEDW (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, ect_tbl_name nvarchar (250), ect_rows int, unik_dw_table nvarchar(250), unik_rows int)
--SET @i = (select count(*) from [Unik_DW].[dbo].[Compare_Extract_And_Unik_DW] WHERE [Extract_Table_Number_Of_Rows] = 0) 

-- Insert into copy of compare table into temp table 
INSERT into #TEMP_CopyEDW (ect_tbl_name,ect_rows,unik_dw_table,unik_rows) 
SELECT [Extract_Table_Name] 
,[Extract_Table_Number_Of_Rows] 
,[Unik_DW_Table_Name] 
,[Unik_DW_Number_Of_Rows] 
FROM [Unik_DW].[recon].[Compare_Extract_And_Unik_DW] 
SET @no_of_rows = @no_of_rows+1; 
SET @i = @i+1; 

--select * from #TEMP_CopyEDW 
-- creating temp table to insert indicator 
CREATE TABLE #TEMP_TABLE_WITH_INDICATOR (id INT, ect_tbl_name nvarchar (250), ect_rows int, unik_dw_table nvarchar(250), unik_rows int, act_diff int, threshold_pct decimal (10, 000), indicator nvarchar(50)) 
--SET @i = (select count(*) from [Unik_DW].[dbo].[Compare_Extract_And_Unik_DW] WHERE [Extract_Table_Number_Of_Rows] = 0 
-- Loop through the temp tab (copyEDW) until the end of the record 
While (@no_of_rows <= (select count(*) from #TEMP_CopyEDW)) 
BEGIN 
-- check if the source table record is empty or null 
if ((select [#TEMP_CopyEDW].[ect_rows] from #TEMP_CopyEDW ORDER BY ect_tbl_name OFFSET @i ROWS FETCH NEXT 1 ROWS ONLY)!=0) 
BEGIN 
-- insert into temp table with indicator 
insert into #TEMP_TABLE_WITH_INDICATOR (id, ect_tbl_name, ect_rows, unik_dw_table, unik_rows, act_diff, threshold_pct) 
values ((SELECT [id] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [ect_tbl_name] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [ect_rows] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [unik_dw_table] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [unik_rows] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
-- Calculate the actual difference in absolute value 
(SELECT abs([unik_rows]-[ect_rows]) as actual_differene FROM #TEMP_CopyEDW Where id = @no_of_rows), 
-- Calculate the threshold percent and round it to third decimal point 
(SELECT cast(round(cast(abs([unik_rows]-[ect_rows]) as decimal(10,3))/CAST([ect_rows]AS decimal(10,3))*100,3) as decimal(10,3)) FROM #TEMP_CopyEDW Where id = @no_of_rows) 
) 
END 
ELSE IF ((select [#TEMP_CopyEDW].[ect_rows] from #TEMP_CopyEDW ORDER BY ect_tbl_name OFFSET @i ROWS FETCH NEXT 1 ROWS ONLY)=0) 
BEGIN 
-- insert into temp table with indicator
insert into #TEMP_TABLE_WITH_INDICATOR (id, ect_tbl_name, ect_rows, unik_dw_table, unik_rows, act_diff, threshold_pct) 
values ((SELECT [id] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [ect_tbl_name] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [ect_rows] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [unik_dw_table] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(SELECT [unik_rows] FROM #TEMP_CopyEDW Where id = @no_of_rows), 
-- Calculate the actual difference in absolute value 
(SELECT abs([unik_rows]-[ect_rows]) as actual_differene FROM #TEMP_CopyEDW Where id = @no_of_rows), 
(0) 
) 
END 
SET @no_of_rows = @no_of_rows+1; 
SET @i = @i+
END 
--select * from #TEMP_TABLE_WITH_INDICATOR 

SET @NumberOfRows = (Select COUNT(*) from #TEMP_TABLE_WITH_INDICATOR) 
WHILE (@offset <= @NumberOfRows) 
BEGIN 
-- Get the threshold percent by id 
SET @threshold_pct = (select [act_diff] from #TEMP_TABLE_WITH_INDICATOR where id = @id) 
--print @threshold_pct 
IF (@threshold_pct = 0) -- if threshold percentage is 0, update indicator column with green 
UPDATE #TEMP_TABLE_WITH_INDICATOR 
SET #TEMP_TABLE_WITH_INDICATOR.indicator = 'green' 
where id = @id 
ELSE IF (@threshold_pct <= 5) 
-- if threshold percentage is less than or equal to 5, update indicator column with yellow 
UPDATE #TEMP_TABLE_WITH_INDICATOR 
SET #TEMP_TABLE_WITH_INDICATOR.indicator = 'yellow' 
where id = @id 
ELSE 
-- Else (greater than 5, update indicator column with red 
UPDATE #TEMP_TABLE_WITH_INDICATOR 
SET #TEMP_TABLE_WITH_INDICATOR.indicator = 'red' 
where id = @id 
SET @offset = @offset + 1; 
SET @id = @id + 1; 
END 
-- truncate table in the database to insert new records 
Truncate table [Unik_DW].[recon] .[Compare_Extract_And_Unik_DW_WITH_INDICATOR]; 
-- insert into real table in database from the temp table with indicator 
Insert into [Unik_DW].[recon] .[Compare_Extract_And_Unik_DW_WITH_INDICATOR] ([id],[ect_tbl_name],[ect_rows]
,[unik_dw_table] 
,[unik_rows] 
,[act_diff] 
,[threshold_pct] 
,[indicator]) 
Select * from #TEMP_TABLE_WITH_INDICATOR 
END