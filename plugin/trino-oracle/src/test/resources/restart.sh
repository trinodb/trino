#!/usr/bin/env bash

echo "Restarting ORACLE"

# Disable listener logging by editing listener.ora for reducing disk space usage
sed -i '/^LOGGING_LISTENER/d' $ORACLE_HOME/network/admin/listener.ora
echo "LOGGING_LISTENER = OFF" >> $ORACLE_HOME/network/admin/listener.ora
echo "TRACE_LEVEL_LISTENER = OFF" >> $ORACLE_HOME/network/admin/listener.ora
echo "DIAG_ADR_ENABLED = OFF" >> $ORACLE_HOME/network/admin/listener.ora

lsnrctl reload && \
sqlplus -s / as sysdba << EOF
   -- Exit on any errors
   WHENEVER SQLERROR EXIT SQL.SQLCODE
   shutdown;
   startup;
   exit;
EOF
