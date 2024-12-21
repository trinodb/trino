/*
this is added to allow more processes on database, otherwise the tests end up giving
ORA-12519, TNS:no appropriate service handler found
ORA-12505, TNS:listener does not currently know of SID given in connect descriptor
to fix this we have to change the number of processes of SPFILE
*/
ALTER SYSTEM SET processes=1000 SCOPE=SPFILE;
ALTER SYSTEM SET disk_asynch_io = FALSE SCOPE = SPFILE;

-- disable audit log for reducing diskspace usage
ALTER SYSTEM SET audit_trail=NONE SCOPE=SPFILE;
-- disable diagnostic logs for reducing diskspace usage
ALTER SYSTEM SET sql_trace=FALSE SCOPE=BOTH;
ALTER SYSTEM SET timed_statistics=FALSE SCOPE=BOTH;

-- Redirect background and user trace logs to /dev/null
ALTER SYSTEM SET background_dump_dest='/dev/null' SCOPE=SPFILE;
ALTER SYSTEM SET user_dump_dest='/dev/null' SCOPE=SPFILE;
ALTER SYSTEM SET diagnostic_dest='/dev/null' SCOPE=SPFILE;
