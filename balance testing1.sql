USE [Unik_DW] 
GO 
SET ANSI_NULLS ON 
GO 
SET QUOTED_IDENTIFIER ON 
GO 

ALTER PROCEDURE [recon].[sp_compare_Extract_Unik_DW_Databases] 
( 
    @Extract_table_name nvarchar(100), 
    @Unik_DW_table_name nvarchar(100) 
) 
    
AS 
DECLARE 
@Extract_table_name nvarchar(100) = 'Extract_AnsogSel', 
@Unik_DW_table_name nvarchar(100), 
@NumberOfTables int, 
@Unik_DW_NumberOfTables int, 
@x int = 0, 
@y int = 0, 
@TablesRecords nvarchar(MAX), 
@Unik_DW_TablesRecords nvarchar(MAX), 
@TempTableRecords nvarchar(250), 
@Unik_DW_TempTableRecords nvarchar(250), 
@QueryToReturnRows nvarchar(MAX), 
@Unik_DW_QueryToReturnRows nvarchar(MAX), 
@Params nvarchar(250), 
@Unik_DW_Params nvarchar(MAX), 
@TotalNumberOfRowsPerTables int, 
@Unik_DW_TotalNumberOfRowsPerTables int, 
@Output_Compare_Result nvarchar(Max), 
@Final_Output_Compareed nvarchar(Max) 
PRINT 'START'; 
PRINT SYSDATETIME(); 
IF OBJECT_ID('tempdb..#TEMPTABLE1') IS NOT NULL --Remove temp table1 here 
DROP TABLE #TEMPTABLE1 -- Remoeve "tempdb.table1" 

IF OBJECT_ID('tempdb..#TEMPTABLE2') IS NOT NULL --Remove temp table2 here 
DROP TABLE #TEMPTABLE2 -- Remoeve "tempdb.table2" 

IF OBJECT_ID('tempdb..#TEMPTABLE3') IS NOT NULL --Remove temp table3 here 
DROP TABLE #TEMPTABLE3 -- Remoeve "tempdb.table3" 

IF OBJECT_ID('tempdb..#TEMPTABLE4') IS NOT NULL --Remove temp table4 here 
DROP TABLE #TEMPTABLE4 -- Remoeve "tempdb.table4" 

BEGIN 
--On Extract database
PRINT SYSDATETIME(); 
PRINT 'BEGIN'; 
-- Get table if exist in Extract database ( Source database) 
SET @NumberOfTables = (SELECT count(TABLE_NAME) 
FROM Extract.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Extract_table_name); 

-- if the table exist in Extract database (Source Database), insert into temp table 1 
If NOT (@NumberOfTables is null or @NumberOfTables = '') 
SELECT TABLE_NAME into #TEMPTABLE1 FROM Extract.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Extract_table_name -- Creatingg temp table1 
CREATE TABLE #TEMPTABLE2 ( id int, tbl_name nvarchar (250), no_of_rows int); -- Creating temp table2 

-- Create insert query to insert table name and no. of rows in that table 61 SET @TablesRecords = 'Insert into #TEMPTABLE2 (id, tbl_name, no_of_rows) VALUES '; 
PRINT SYSDATETIME(); 
WHILE (@x < @NumberOfTables) 
BEGIN 

SET @TempTableRecords = ''; 
-- Return One table at a time from temp. table 1 
SET @TempTableRecords = (select TABLE_NAME from #TEMPTABLE1 ORDER BY TABLE_NAME OFFSET @x ROWS FETCH NEXT 1 ROWS ONLY); 
PRINT SYSDATETIME(); 
PRINT 'After Fetch Extract'; 
-- check if the table exist in temp table 1 
If NOT (@TempTableRecords is null or @TempTableRecords = '') 
-- Get the number of rows from the given table 
SET @QueryToReturnRows = concat('Select @TotalNumberOfRowsPerTables = count(*) from [Extract].[dbo].[', @TempTableRecords, ']'); 
-- executing query to return number of rows from the table 
EXECUTE sp_executesql 
@Query = @QueryToReturnRows 
, @Params = N'@TotalNumberOfRowsPerTables INT OUTPUT' 
, @TotalNumberOfRowsPerTables = @TotalNumberOfRowsPerTables OUTPUT 
print @TotalNumberOfRowsPerTables; 

-- Appending to the insert query (Concatenation) 
SET @TablesRecords = CONCAT(@TablesRecords, '(', @x+1, ',', concat('"', @TempTableRecords, '"'), ',', @TotalNumberOfRowsPerTables, '),'); 

SET @x = @x + 1; 

END 
-- removing last comma and replacing with semicolon (;) in the insert query

SET @TablesRecords = (select substring(@TablesRecords, 1, (len(@TablesRecords) - 1))); 
SET @TablesRecords = concat(@TablesRecords, ';'); 
SET @TablesRecords = replace(@TablesRecords, '"', ''''); 
PRINT SYSDATETIME(); 
PRINT 'Before exec SQL query'; 
-- execute insert query 
EXECUTE sp_executesql @TablesRecords; 

--For Unik_DW dataware house 
-- Get table form the target database 
SET @Unik_DW_NumberOfTables = (SELECT count(TABLE_NAME) 
FROM Unik_DW.INFORMATION_SCHEMA.TABLES); 
PRINT SYSDATETIME(); 
-- Check if the table exist in the target database 
If NOT (@Unik_DW_NumberOfTables is null or @Unik_DW_NumberOfTables = '') 
-- Insert table name in temp table 3 if exists 
SELECT TABLE_NAME into #TEMPTABLE3 FROM Unik_DW.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'dbo' -- Creatingg temp table1 
-- Create temp table 4 to insert in table name and no of rows 
CREATE TABLE #TEMPTABLE4 ( id int, tbl_name nvarchar (250), no_of_rows int); -- Creating temp table2 

SET @Unik_DW_TablesRecords = 'Insert into #TEMPTABLE4 (id, tbl_name, no_of_rows) VALUES '; 
PRINT SYSDATETIME(); 
WHILE (@y <= @Unik_DW_NumberOfTables) 
BEGIN 
PRINT SYSDATETIME(); 
PRINT 'START BEGIN SQL'; 
SET @Unik_DW_TempTableRecords = ''; 
-- Get table name from temp table 3 
SET @Unik_DW_TempTableRecords = (select TABLE_NAME from #TEMPTABLE3 ORDER BY TABLE_NAME OFFSET @y ROWS FETCH NEXT 1 ROWS ONLY); 
PRINT SYSDATETIME(); 
PRINT 'After Fetch'; 
If NOT (@Unik_DW_TempTableRecords is null or @Unik_DW_TempTableRecords = '' OR @Unik_DW_TempTableRecords = 'config') 
if(@Unik_DW_TempTableRecords = 'DimAnsøgerselskab') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimAnsøgerselskab] where [Ansøgerselskabsnavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_AnsøgerselskabID] NOT IN (SELECT [SK_AnsøgerselskabID] FROM [DimAnsøgerselskab] WHERE [SK_AnsøgerselskabID] = -1 or [Ansøgerselskabsnavn] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END

else if(@Unik_DW_TempTableRecords = 'DimBygningsdel') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimBygningsdel] where [Bygningsdelsnavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_BygningsdelID] NOT IN (SELECT [SK_BygningsdelID] FROM [DimBygningsdel] WHERE [SK_BygningsdelID] = -1 or [Bygningsdelsnavn] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimAnsøger') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimAnsøger] where [Ansøgerstreng] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_AnsøgerID] NOT IN (SELECT [SK_AnsøgerID] FROM [DimAnsøger] WHERE [SK_AnsøgerID] = -1 or [Ansøgerstreng] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimEjendom') 
BEGIN 
PRINT SYSDATETIME(); 
PRINT 'Before SQL query'; 
If (select COUNT(*) from [Unik_DW].[dbo].[DimEjendom] where [FinSelskab] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_EjendomID] NOT IN (SELECT [SK_EjendomID] FROM [DimEjendom] WHERE [SK_EjendomID] = -1 or [FinSelskab] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
PRINT SYSDATETIME(); 
PRINT 'After SQL query'; 
END 
else if(@Unik_DW_TempTableRecords = 'DimEnhedskonto') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimEnhedskonto] where [Enhedskontonavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_EnhedskontoID] NOT IN (SELECT [SK_EnhedskontoID] FROM [DimEnhedskonto] WHERE [SK_EnhedskontoID] = -1 or [Enhedskontonavn] = ''Ukendt'')'); 
else
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimFinansenhed') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimFinansenhed] where [Finansenhedsnavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_FinansenhedID] NOT IN (SELECT [SK_FinansenhedID] FROM [DimFinansenhed] WHERE [SK_FinansenhedID] = -1 or [Finansenhedsnavn] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimKonto') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimKonto] where [Kontonavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_KontoID] NOT IN (SELECT [SK_KontoID] FROM [DimKonto] WHERE [SK_KontoID] = -1 or [Kontonavn] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimKunde') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimKunde] where [DebitorStatusTxt] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_KundeID] NOT IN (SELECT [SK_KundeID] FROM [DimKunde] WHERE [SK_KundeID] = -1 or [DebitorStatusTxt] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimOpkrType') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimOpkrType] where [OpkrTypeNavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_OpkrTypeID] NOT IN (SELECT [SK_OpkrTypeID] FROM [DimOpkrType] WHERE [SK_OpkrTypeID] = -1 or [OpkrTypeNavn] = ''Ukendt'')'); 

else
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimOrganisation') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimOrganisation] where [BrugerNavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_OrganisationID] NOT IN (SELECT [SK_OrganisationID] FROM [DimOrganisation] WHERE [SK_OrganisationID] = -1 or [BrugerNavn] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimRegnskabsår') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimRegnskabsår] where [Regnskabsår] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_RegnskabsårID] NOT IN (SELECT [SK_RegnskabsårID] FROM [DimRegnskabsår] WHERE [SK_RegnskabsårID] = -1 or [Regnskabsår] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimRykkertype') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimRykkertype] where [Rykkergruppe] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_RykkertypeID] NOT IN (SELECT [SK_RykkertypeID] FROM [DimRykkertype] WHERE [SK_RykkertypeID] = -1 or [Rykkergruppe] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimSelskab') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimSelskab] where [Selskabsnavn] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_SelskabID] NOT IN (SELECT [SK_SelskabID] FROM [DimSelskab] WHERE [SK_SelskabID] = -1 or [Selskabsnavn] = ''Ukendt'')'); 
else
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else if(@Unik_DW_TempTableRecords = 'DimSag') 
BEGIN 
If (select COUNT(*) from [Unik_DW].[dbo].[DimSag] where [SagNr] = 'Ukendt') = 1 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']', ' WHERE [SK_SagID] NOT IN (SELECT [SK_SagID] FROM [DimSag] WHERE [SK_SagID] = -1 or [SagNr] = ''Ukendt'')'); 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
END 
else 
SET @Unik_DW_QueryToReturnRows = concat('Select @Unik_DW_TotalNumberOfRowsPerTables = count(*) from [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, ']'); 
-----SET @Unik_DW_QueryToReturnRows = concat('Select ( SELECT COUNT(*) FROM [Unik_DW].[dbo].[', @Unik_DW_TempTableRecords, '] ) AS @Unik_DW_TotalNumberOfRowsPerTables1, ( SELECT COUNT(*) FROM from [Unik_DW].[accoCALC].[', @Unik_DW_TempTableRecords, ']) AS @Unik_DW_TotalNumberOfRowsPerTables2 FROM dual' ); 
--print @Unik_DW_QueryToReturnRows; 
EXECUTE sp_executesql 
@Query = @Unik_DW_QueryToReturnRows 
, @Unik_DW_Params = N'@Unik_DW_TotalNumberOfRowsPerTables INT OUTPUT' 
, @Unik_DW_TotalNumberOfRowsPerTables = @Unik_DW_TotalNumberOfRowsPerTables OUTPUT 
SET @Unik_DW_TablesRecords = CONCAT(@Unik_DW_TablesRecords, '(', @y+1, ',', concat('"', @Unik_DW_TempTableRecords, '"'), ',', @Unik_DW_TotalNumberOfRowsPerTables, '),'); 
---print @Unik_DW_TablesRecords; 
SET @y = @y + 1; 
END 
PRINT SYSDATETIME(); 
-- Removing the last element (comman) and replacing with semi-colon (;) 
SET @Unik_DW_TablesRecords = (select substring(@Unik_DW_TablesRecords, 1, (len(@Unik_DW_TablesRecords) - 1))); 
SET @Unik_DW_TablesRecords = concat(@Unik_DW_TablesRecords, ';'); 
SET @Unik_DW_TablesRecords = replace(@Unik_DW_TablesRecords, '"', ''''); 
-- execute the insert query 
EXECUTE sp_executesql @Unik_DW_TablesRecords; 
--select * from #TEMPTABLE4; 
--SET @Extract_table_name = 'Extract_Ansoger'; 
--SET @Unik_DW_table_name = 'DimAnsøger';
-- Final insert query in real table in database taking data from temp table 2 (source) and temp table 4 (target) 
SET @Output_Compare_Result = 'Insert into [Unik_DW].[recon].[Compare_Extract_And_Unik_DW] Select #TEMPTABLE2.tbl_name, #TEMPTABLE2.no_of_rows, #TEMPTABLE4.tbl_name, #TEMPTABLE4.no_of_rows from #TEMPTABLE2, #TEMPTABLE4 where #TEMPTABLE2.tbl_name = '''; 
SET @Output_Compare_Result = CONCAT(@Output_Compare_Result, @Extract_table_name, ''' and #TEMPTABLE4.tbl_name = ''', @Unik_DW_table_name); 
SET @Output_Compare_Result = concat(@Output_Compare_Result, ''';'); 
-- execute the insert query 
EXECUTE sp_executesql @Output_Compare_Result; 
PRINT SYSDATETIME(); 
-- query to print the result from the final table 
SET @Final_Output_Compareed = 'Select * from [Unik_DW].[recon].[Compare_Extract_And_Unik_DW]'; 
EXECUTE sp_executesql @Final_Output_Compareed; 
PRINT SYSDATETIME(); 
END