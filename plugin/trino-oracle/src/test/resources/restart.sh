#!/usr/bin/env bash

echo "Restarting ORACLE"

lsnrctl reload && \
sqlplus -s / as sysdba << EOF
   -- Exit on any errors
   WHENEVER SQLERROR EXIT SQL.SQLCODE
   shutdown;
   startup;
   exit;
EOF
