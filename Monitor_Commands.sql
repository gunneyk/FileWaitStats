

--  Gather Stats
EXEC dbo.gather_wait_stats_2012
EXEC dbo.gather_file_stats_2012

*/

/*
--   Report stats
EXEC dbo.report_wait_stats_2012

EXEC dbo.report_file_stats_2012

EXEC dbo.report_wait_stats_2012 '20140206 15:51:44.127', '20140208', 10

EXEC dbo.report_file_stats_2012 '20140206 15:51:44.127', '20140208', 10, 2


select top 1000 * from dba.wait_stats 
WHERE capture_time > getdate() - 1


SELECT MIN([capture_time]) AS [MIN], MAX([capture_time]) AS [MAX] FROM [DBA].[wait_stats] ;

SELECT MAX([capture_time]) FROM [DBA].[wait_stats] WHERE [capture_time] <= '20130206 15:51:44.127'
SELECT MAX([capture_time]) FROM [DBA].[wait_stats] WHERE [capture_time] <= '20140206 15:50:44.127'


SELECT * FROM [DBA].[wait_stats] WHERE [capture_time] > '20140207 12:51:44.127'
