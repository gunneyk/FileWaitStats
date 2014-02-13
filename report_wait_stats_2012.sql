USE [tempdb]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID(N'[dbo].[report_wait_stats_2012]') and OBJECTPROPERTY([object_id], N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[report_wait_stats_2012] ;
GO
CREATE PROCEDURE [dbo].[report_wait_stats_2012] 
( @BeginTime DATETIME = NULL
, @EndTime DATETIME = NULL
, @TopNN INT = NULL )

/*
 --  @BeginTime = Date( & time) of the nearest first sample to use. Defaults to very first
 --  @EndTime   = Date( & time) of the nearest last sample to use. Defaults to very last
 --  @Topnn     = Returns only the Top nn rows by Total Wait Time ms DESC. Defaults to all rows
 
   20140207   AJK - Fixed Begin & End time sampling logic
                    Added optional TOP parameter

*/
AS

SET NOCOUNT ON ;

DECLARE @Days VARCHAR(5) = '', @Hours INT;

IF OBJECT_ID( N'[dbo].[wait_stats]',N'U') IS NULL
BEGIN
		RAISERROR('Error [dbo].[wait_stats] table does not exist', 16, 1) WITH NOWAIT ;
		RETURN ;
END

DECLARE @Total_Wait numeric(28,2), @Total_SignalWait numeric(28,2), @Total_ResourceWait numeric(28,2)
	, @Total_Requests Bigint ;

DECLARE @Waits TABLE ([wait_type] nvarchar(60) not null, 
    [waiting_tasks_count] bigint not null,
    [wait_time_ms] bigint not null,
    [max_wait_time_ms] bigint not null,
    [signal_wait_time_ms] bigint not null,
    [capture_time] datetime not null) ;

--  If no First time was specified then use the First sample
IF @BeginTime IS NULL
    SET @BeginTime = (SELECT MIN([capture_time]) FROM [dbo].[wait_stats]) ;
ELSE
BEGIN
    --  If the time was not specified exactly find the closest one
    IF NOT EXISTS(SELECT * FROM [dbo].[wait_stats] WHERE [capture_time] = @BeginTime) 
    BEGIN
        DECLARE @FT DATETIME ;
        SET @FT = @BeginTime ;

        SET @BeginTime = (SELECT MAX([capture_time]) FROM [dbo].[wait_stats] WHERE [capture_time] <= @FT) ;
        IF @BeginTime IS NULL
            SET @BeginTime = (SELECT MIN([capture_time]) FROM [dbo].[wait_stats] WHERE [capture_time] >= @FT) ;
    END
END

--  If no Last time was specified then use the latest sample
IF @EndTime IS NULL
    SET @EndTime = (SELECT MAX([capture_time]) FROM [dbo].[wait_stats]) ;
ELSE
BEGIN
    --  If the time was not specified exactly find the closest one
    IF NOT EXISTS(SELECT * FROM [dbo].[wait_stats] WHERE [capture_time] = @EndTime)
    BEGIN
        DECLARE @LT DATETIME ;
        SET @LT = @EndTime ;

        SET @EndTime = (SELECT MAX([capture_time]) FROM [dbo].[wait_stats] WHERE [capture_time] <= @LT) ;
        IF @EndTime IS NULL
            SET @EndTime = (SELECT MIN([capture_time]) FROM [dbo].[wait_stats] WHERE [capture_time] >= @LT) ;
    END
END


--  Get the relevant waits
INSERT INTO @Waits ([wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms], [capture_time])
    SELECT [wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms], [capture_time]
        FROM [dbo].[wait_stats] WHERE [capture_time] = @EndTime ;

IF @@ROWCOUNT = 0
BEGIN
    RAISERROR('Error, there are no waits for the specified DateTime', 16, 1) WITH NOWAIT ;
    RETURN ;
END
    

--  Delete some of the misc types of waits
    DELETE FROM @Waits 
        WHERE [wait_type] IN ('CLR_SEMAPHORE','LAZYWRITER_SLEEP','RESOURCE_QUEUE','SLEEP_TASK','BROKER_EVENTHANDLER'
	,'SLEEP_SYSTEMTASK','SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP','WAITFOR','LOGMGR_QUEUE','CHECKPOINT_QUEUE','BROKER_RECEIVE_WAITFOR'
    ,'REQUEST_FOR_DEADLOCK_SEARCH','XE_TIMER_EVENT','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_MANUAL_EVENT','CLR_AUTO_EVENT'
    ,'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT','XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN'
	,'SP_SERVER_DIAGNOSTICS_SLEEP','HADR_FILESTREAM_IOMGR_IOCOMPLETION','SQLTRACE_INCREMENTAL_FLUSH_SLEEP','DIRTY_PAGE_POLL'
    ,'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'
    ,'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE') ;


    
-- Get the delta
UPDATE a SET a.[waiting_tasks_count] = (a.[waiting_tasks_count] - b.[waiting_tasks_count])
                ,a.[wait_time_ms] = (a.[wait_time_ms] - b.[wait_time_ms])
                ,a.[signal_wait_time_ms] = (a.[signal_wait_time_ms] - b.[signal_wait_time_ms])
FROM @Waits AS a INNER JOIN [dbo].[wait_stats] AS b ON a.[wait_type] = b.[wait_type]
            AND b.[capture_time] = @BeginTime ;


--  Get the totals
SELECT @Total_Wait = SUM([wait_time_ms]) + 1, @Total_SignalWait = SUM([signal_wait_time_ms]) + 1 
    FROM @Waits ;

SET @Total_ResourceWait = (1 + @Total_Wait) - @Total_SignalWait ;

SET @Total_Requests = (SELECT SUM([waiting_tasks_count]) FROM @Waits) ;

INSERT INTO @Waits ([wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms], [capture_time])
    SELECT '***Total***',@Total_Requests,@Total_Wait,0,@Total_SignalWait,@EndTime ;


--  Get the time diff from the first to last sample
SET @Days = CONVERT(VARCHAR(5), DATEDIFF(dd,@BeginTime,@EndTime)) ;

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
    --  Make sure we have a valid TOP value
    SET @TopNN = 1000 ;
END
ELSE
BEGIN
    IF @TopNN < 1
        SET @TopNN = 1000 ;
END ;

SELECT TOP(@TopNN) [wait_type] AS [Wait Type]
    ,[waiting_tasks_count] AS [Requests]
	,[wait_time_ms] AS [Total Wait Time (ms)]
    ,[max_wait_time_ms] AS [Max Wait Time (ms)]
	,CAST(100.0 * [wait_time_ms] / @Total_Wait as numeric(28,2)) AS [% Waits]
	,[wait_time_ms] - [signal_wait_time_ms] AS [Resource Waits (ms)]
	,CAST(100.0 * ([wait_time_ms] - [signal_wait_time_ms]) / @Total_ResourceWait as numeric(28,2)) AS [% Res Waits]
	,[signal_wait_time_ms] AS [Signal Waits (ms)]
	,CAST(100.0 * [signal_wait_time_ms] / @Total_SignalWait as numeric(28,2)) AS [% Signal Waits]
	,CAST([wait_time_ms] / CASE [waiting_tasks_count] WHEN 0 THEN 1 ELSE [waiting_tasks_count] END AS NUMERIC(28,1)) AS [Avg Wait ms]
FROM @Waits 
    ORDER BY [Total Wait Time (ms)] DESC, [Wait Type] ;

GO

