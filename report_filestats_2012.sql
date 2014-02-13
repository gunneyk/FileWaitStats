USE [tempdb]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID(N'[dbo].[report_file_stats_2012]') and OBJECTPROPERTY([object_id], N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[report_file_stats_2012] ;
GO
CREATE PROCEDURE [dbo].[report_file_stats_2012] 
( @BeginTime DATETIME = NULL
, @EndTime DATETIME = NULL
, @TopNN INT = NULL
, @DBID INT = NULL )

/*
 --  @BeginTime = Date( & time) of the nearest first sample to use. Defaults to very first
 --  @EndTime   = Date( & time) of the nearest last sample to use. Defaults to very last
 --  @Topnn     = Returns only the Top nn rows by TotalIOStallms DESC. Defaults to all rows
 --  @DBID      = Only return this Database ID. Defaults to all db's
 
    20140207   AJK - Fixed Begin & End time sampling logic. 
                     Swapped input params. 
                     Added TOP optional parameter

    20140210   AJK - Added optional DB ID Parameter. 

 */

AS

SET NOCOUNT ON ;

DECLARE @Days VARCHAR(5) = '';
DECLARE @Hours INT = 0;

IF OBJECT_ID( N'[dbo].[file_stats]',N'U') IS NULL
BEGIN
		RAISERROR('Error [dbo].[file_stats] table does not exist', 16, 1) WITH NOWAIT ;
		RETURN ;
END

DECLARE @file_stats TABLE (
	    [database_id] [smallint] NOT NULL,
	    [file_id] [smallint] NOT NULL,
	    [num_of_reads] [bigint] NOT NULL,
	    [num_of_bytes_read] [bigint] NOT NULL,
	    [io_stall_read_ms] [bigint] NOT NULL,
	    [num_of_writes] [bigint] NOT NULL,
	    [num_of_bytes_written] [bigint] NOT NULL,
	    [io_stall_write_ms] [bigint] NOT NULL,
	    [io_stall] [bigint] NOT NULL,
	    [size_on_disk_bytes] [bigint] NOT NULL,
        [capture_time] [datetime] NOT NULL
        )  ;

--  If no time was specified then use the latest sample minus the first sample
IF @BeginTime IS NULL
    SET @BeginTime = (SELECT MIN([capture_time]) FROM [dbo].[file_stats]) ;
ELSE
BEGIN
    --  If the time was not specified exactly find the closest one
    IF NOT EXISTS(SELECT * FROM [dbo].[file_stats] WHERE [capture_time] = @BeginTime)
    BEGIN
        DECLARE @FT DATETIME ;
        SET @FT = @BeginTime ;

        SET @BeginTime = (SELECT MAX([capture_time]) FROM [dbo].[file_stats] WHERE [capture_time] <= @FT) ;

        IF @BeginTime IS NULL
            SET @BeginTime = (SELECT MIN([capture_time]) FROM [dbo].[file_stats] WHERE [capture_time] >= @FT) ;

    END
END

IF @EndTime IS NULL
    SET @EndTime = (SELECT MAX([capture_time]) FROM [dbo].[file_stats]) ;
ELSE
BEGIN
    --  If the time was not specified exactly find the closest one
    IF NOT EXISTS(SELECT * FROM [dbo].[file_stats] WHERE [capture_time] = @EndTime)
    BEGIN
        DECLARE @ET DATETIME ;
        SET @ET = @EndTime ;

        SET @EndTime = (SELECT MAX([capture_time]) FROM [dbo].[file_stats] WHERE [capture_time] <= @ET) ;

        IF @EndTime IS NULL
            SET @EndTime = (SELECT MIN([capture_time]) FROM [dbo].[file_stats] WHERE [capture_time] >= @ET) ;
    END
END


INSERT INTO @file_stats
      ([database_id],[file_id],[num_of_reads],[num_of_bytes_read],[io_stall_read_ms]
      ,[num_of_writes],[num_of_bytes_written],[io_stall_write_ms]
      ,[io_stall],[size_on_disk_bytes],[capture_time])
SELECT [database_id],[file_id],[num_of_reads],[num_of_bytes_read],[io_stall_read_ms]
      ,[num_of_writes],[num_of_bytes_written],[io_stall_write_ms]
      ,[io_stall],[size_on_disk_bytes],[capture_time]
FROM [dbo].[file_stats] 
    WHERE ([capture_time] = @EndTime AND @DBID IS NULL) OR ([capture_time] = @EndTime AND database_id = @DBID) ;

IF @@ROWCOUNT = 0
BEGIN
    RAISERROR('Error, there are no waits for the specified DateTime', 16, 1) WITH NOWAIT ;
    RETURN ;
END

--  Subtract the starting numbers from the end ones to find the difference for that time period
UPDATE fs
        SET fs.[num_of_reads] = (fs.[num_of_reads] - a.[num_of_reads])
       , fs.[num_of_bytes_read] = (fs.[num_of_bytes_read] - a.[num_of_bytes_read])
       , fs.[io_stall_read_ms] = (fs.[io_stall_read_ms] - a.[io_stall_read_ms])
       , fs.[num_of_writes] = (fs.[num_of_writes] - a.[num_of_writes])
       , fs.[num_of_bytes_written] = (fs.[num_of_bytes_written] - a.[num_of_bytes_written])
       , fs.[io_stall_write_ms] = (fs.[io_stall_write_ms] - a.[io_stall_write_ms])
       , fs.[io_stall] = (fs.[io_stall] - a.[io_stall])
FROM @file_stats AS fs INNER JOIN (SELECT b.[database_id],b.[file_id],b.[num_of_reads],b.[num_of_bytes_read],b.[io_stall_read_ms]
                                        ,b.[num_of_writes],b.[num_of_bytes_written],b.[io_stall_write_ms],b.[io_stall]
                                    FROM [dbo].[file_stats] AS b
                                        WHERE b.[capture_time] = @BeginTime) AS a
                    ON (fs.[database_id] = a.[database_id] AND fs.[file_id] = a.[file_id]) ;

--  Get the time diff from the first to last sample
SET @Hours = DATEDIFF(hh,@BeginTime,@EndTime) ;
IF @Hours < 24
    SET @Days = '00' ;
ELSE
    SET @Days = CAST(@Hours / 24 AS VARCHAR(2)) ; 


IF DATALENGTH(@Days) < 2
    SET @Days = '0' + @Days + ':';
ELSE
    SET @Days = @Days + ':';
    
SELECT CONVERT(varchar(50),@BeginTime,120) AS [Start Time], CONVERT(varchar(50),@EndTime,120) AS [End Time]
    ,@Days + CONVERT(varchar(50),@EndTime - @BeginTime,108) AS [Duration (dd:hh:mm:ss)] ;

IF @TopNN IS NULL
BEGIN

    SELECT fs.[database_id] AS [Database ID], fs.[file_id] AS [File ID], fs.[num_of_reads] AS [NumberReads]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_read] / 1048576.0)) AS [MBs Read]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_read] / 1024.0) / CASE fs.[num_of_reads] WHEN 0 THEN 1 ELSE  fs.[num_of_reads] END ) AS [Avg KB Per Read]
        ,fs.[io_stall_read_ms] AS [IoStallReadMS]
        ,CONVERT(NUMERIC(10,2), fs.[io_stall_read_ms] / CASE fs.[num_of_reads] WHEN 0 THEN 1 ELSE  fs.[num_of_reads] END) AS [AVG IoStallReadMS]
        ,fs.[num_of_writes] AS [NumberWrites]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_written] / 1048576.0)) AS [MBs Written]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_written] / 1024.0) / CASE fs.[num_of_writes] WHEN 0 THEN 1 ELSE fs.[num_of_writes] END ) AS [Avg KB Per Write]
        ,fs.[io_stall_write_ms] AS [IoStallWriteMS]
        ,CONVERT(NUMERIC(10,2), fs.[io_stall_write_ms] / CASE fs.[num_of_writes] WHEN 0 THEN 1 ELSE  fs.[num_of_writes] END) AS [AVG IoStallWriteMS]
        ,fs.[io_stall] AS [IoStallMS]
        ,CONVERT(NUMERIC(10,2),(fs.[size_on_disk_bytes] / 1048576.0)) AS [MBsOnDisk]
        ,(SELECT c.[name] FROM [master].[sys].[databases] AS c WHERE c.[database_id] = fs.[database_id]) AS [DB Name]
        ,(SELECT RIGHT(d.[physical_name],CHARINDEX('\',REVERSE(d.[physical_name]))-1) 
                FROM [master].[sys].[master_files] AS d 
                    WHERE d.[file_id] = fs.[file_id] AND d.[database_id] = fs.[database_id]) AS [File Name]
        ,fs.[capture_time] AS [Last Sample]
    FROM @file_stats AS fs
        ORDER BY fs.[database_id], fs.[file_id] ;

END ;
ELSE 
BEGIN
    --  Make sure we have a valid TOP value
    IF @TopNN < 1
        SET @TopNN = 1000 ;

    SELECT TOP (@TopNN) fs.[database_id] AS [Database ID], fs.[file_id] AS [File ID], fs.[num_of_reads] AS [NumberReads]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_read] / 1048576.0)) AS [MBs Read]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_read] / 1024.0) / CASE fs.[num_of_reads] WHEN 0 THEN 1 ELSE  fs.[num_of_reads] END ) AS [Avg KB Per Read]
        ,fs.[io_stall_read_ms] AS [IoStallReadMS]
        ,CONVERT(NUMERIC(10,2), fs.[io_stall_read_ms] / CASE fs.[num_of_reads] WHEN 0 THEN 1 ELSE  fs.[num_of_reads] END) AS [AVG IoStallReadMS]
        ,fs.[num_of_writes] AS [NumberWrites]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_written] / 1048576.0)) AS [MBs Written]
        ,CONVERT(NUMERIC(10,2),(fs.[num_of_bytes_written] / 1024.0) / CASE fs.[num_of_writes] WHEN 0 THEN 1 ELSE fs.[num_of_writes] END ) AS [Avg KB Per Write]
        ,fs.[io_stall_write_ms] AS [IoStallWriteMS]
        ,CONVERT(NUMERIC(10,2), fs.[io_stall_write_ms] / CASE fs.[num_of_writes] WHEN 0 THEN 1 ELSE  fs.[num_of_writes] END) AS [AVG IoStallWriteMS]
        ,fs.[io_stall] AS [IoStallMS]
        ,CONVERT(NUMERIC(10,2),(fs.[size_on_disk_bytes] / 1048576.0)) AS [MBsOnDisk]
        ,(SELECT c.[name] FROM [master].[sys].[databases] AS c WHERE c.[database_id] = fs.[database_id]) AS [DB Name]
        ,(SELECT RIGHT(d.[physical_name],CHARINDEX('\',REVERSE(d.[physical_name]))-1) 
                FROM [master].[sys].[master_files] AS d 
                    WHERE d.[file_id] = fs.[file_id] AND d.[database_id] = fs.[database_id]) AS [File Name]
        ,fs.[capture_time] AS [Last Sample]
    FROM @file_stats AS fs
        ORDER BY [IoStallMS] DESC ;

END ;

GO
