These SQL Server stored procedures are the latest version of what I have been using for many years to gather and report on the file and wait statistics of SQL Server. The two gather_xx procedures will take a snapshot of the file or wait stat information and log them to a table in the db of your choice. The report_xx procedures will allow you to report on a delta of two snapshots. By default (with no parameters passed in) it will take the very first and very last snapshots in the tables and report off of them. If you use a date (or datetime) for any fo the begin or end date parameters it will find the closest snapshot to the time submitted and use those. There are optional TOP nn  and in the case of the file stats a db_id parameter as well.

Andrew J. Kelly
akelly@solidq.com
