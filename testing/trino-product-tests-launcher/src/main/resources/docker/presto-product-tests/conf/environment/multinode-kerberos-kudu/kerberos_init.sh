#!/usr/bin/env bash

set -euo pipefail

# Set the max ticket lifetime, this is to facilitate testing of renewing kerberos tickets after expiry
MAX_TICKET_LIFETIME="1min"

function create_principal_with_forward_and_reverse_dns_entries() {
    PRIMARY="$1"
    INSTANCE="$2"
    KEYTAB="$3"
    REALM="STARBURSTDATA.COM"

    PRINCIPAL="$PRIMARY/$INSTANCE@$REALM"
    /usr/sbin/kadmin.local -q "addprinc -randkey -maxlife $MAX_TICKET_LIFETIME $PRINCIPAL" && \
    /usr/sbin/kadmin.local -q "xst -norandkey -k $KEYTAB $PRINCIPAL"

    # Create a separate principal in the same keytab
    # This is because the java kerberos client impl does not support rdns=false and the forward/reverse dns entries
    # do not match when running the product tests in docker due to the container naming and docker network
    # See here for details: https://web.mit.edu/kerberos/krb5-1.12/doc/admin/princ_dns.html#provisioning-keytabs
    PRINCIPAL="$PRIMARY/ptl-$INSTANCE.ptl-network@$REALM"
    /usr/sbin/kadmin.local -q "addprinc -randkey -maxlife $MAX_TICKET_LIFETIME $PRINCIPAL" && \
    /usr/sbin/kadmin.local -q "xst -norandkey -k $KEYTAB $PRINCIPAL"
}

# Modify ticket granting ticket principal max lifetime
/usr/sbin/kadmin.local -q "modprinc -maxlife $MAX_TICKET_LIFETIME krbtgt/STARBURSTDATA.COM@STARBURSTDATA.COM"

create_principal_with_forward_and_reverse_dns_entries kuduservice kudu-master /kerberos/kudu-master.keytab

create_principal_with_forward_and_reverse_dns_entries kuduservice kudu-tserver-0 /kerberos/kudu-tserver-0.keytab
create_principal_with_forward_and_reverse_dns_entries kuduservice kudu-tserver-1 /kerberos/kudu-tserver-1.keytab
create_principal_with_forward_and_reverse_dns_entries kuduservice kudu-tserver-2 /kerberos/kudu-tserver-2.keytab

# Create principal and keytab for the trino connector to use
create_principal -p test -k /kerberos/test.keytab
/usr/sbin/kadmin.local -q "modprinc -maxlife $MAX_TICKET_LIFETIME test@STARBURSTDATA.COM"

# The kudu container runs as: uid=1000(kudu) gid=1000(kudu)
chown -R 1000:1000 /kerberos
chmod -R 770 /kerberos
echo "Kerberos init script completed successfully"
