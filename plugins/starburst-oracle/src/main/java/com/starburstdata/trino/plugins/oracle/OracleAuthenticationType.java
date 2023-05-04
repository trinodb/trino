/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

public enum OracleAuthenticationType
{
    PASSWORD,
    PASSWORD_PASS_THROUGH,
    KERBEROS,
    KERBEROS_PASS_THROUGH,
    /**/;
}
