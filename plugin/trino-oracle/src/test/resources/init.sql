/*
this is added to allow more processes on database, otherwise the tests end up giving
ORA-12519, TNS:no appropriate service handler found
ORA-12505, TNS:listener does not currently know of SID given in connect descriptor
to fix this we have to change the number of processes of SPFILE
*/
ALTER SYSTEM SET processes=1000 SCOPE=SPFILE;
ALTER SYSTEM SET disk_asynch_io = FALSE SCOPE = SPFILE;
