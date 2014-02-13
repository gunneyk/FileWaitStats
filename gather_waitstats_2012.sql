USE tempdb ;
GO

IF EXISTS (SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID(N'[dbo].[gather_wait_stats_2012]') and OBJECTPROPERTY([object_id], N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[gather_wait_stats_2012] ;
go
CREATE PROCEDURE [dbo].[gather_wait_stats_2012] (@Clear INT = 0)

AS
/*
 --  @Clear = If 1 then TRUNCATE the wait stats table
 
    20140210   AJK - Added header. Removed the ability to clear the stats
                     counters and just truncate the table.

 */
SET NOCOUNT ON ;

DECLARE @DT DATETIME ;
SET @DT = GETDATE() ;

IF OBJECT_ID(N'[dbo].[wait_stats]',N'U') IS NULL
    CREATE TABLE [dbo].[wait_stats] 
        ([wait_type] nvarchar(60) not null, 
        [waiting_tasks_count] bigint not null,
        [wait_time_ms] bigint not null,
        [max_wait_time_ms] bigint not null,
        [signal_wait_time_ms] bigint not null,
        [capture_time] datetime not null default getdate()) ;

--  If 1 the clear out the wait_stats table
IF @Clear = 1
BEGIN
    TRUNCATE TABLE [dbo].[wait_stats] ;
END


INSERT INTO [dbo].[wait_stats] ([wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms], [capture_time])	
    SELECT [wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms], @DT
        FROM sys.dm_os_wait_stats ;

GO

