USE tempdb ;
GO

IF EXISTS (SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID(N'[dbo].[gather_file_stats_2012]') AND OBJECTPROPERTY([object_id], N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[gather_file_stats_2012] ;
go
CREATE PROCEDURE [dbo].[gather_file_stats_2012] (@Clear INT = 0)

AS
/*
 --  @Clear = If 1 then TRUNCATE the wait stats table
 
    20140210   AJK - Added header. 

 */

SET NOCOUNT ON ;

DECLARE @DT DATETIME ;
SET @DT = GETDATE() ;

IF OBJECT_ID(N'[dbo].[file_stats]',N'U') IS NULL
    CREATE TABLE [dbo].[file_stats](
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
        ) ;


--  If 1 the clear out the table
IF @Clear = 1
BEGIN
    TRUNCATE TABLE [dbo].[file_stats] ;
END


INSERT INTO [dbo].[file_stats]
      ([database_id]
      ,[file_id]
      ,[num_of_reads]
      ,[num_of_bytes_read]
      ,[io_stall_read_ms]
      ,[num_of_writes]
      ,[num_of_bytes_written]
      ,[io_stall_write_ms]
      ,[io_stall]
      ,[size_on_disk_bytes]
      ,[capture_time])
SELECT [database_id]
      ,[file_id]
      ,[num_of_reads]
      ,[num_of_bytes_read]
      ,[io_stall_read_ms]
      ,[num_of_writes]
      ,[num_of_bytes_written]
      ,[io_stall_write_ms]
      ,[io_stall]
      ,[size_on_disk_bytes]
      ,@DT
FROM [sys].dm_io_virtual_file_stats(NULL,NULL) ;

GO
