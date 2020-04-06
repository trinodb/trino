/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

public enum SnowflakeImpersonationType
{
    /**
     * Connect as the service user (credentials in connector properties)
     */
    NONE,
    /**
     * Service user assuming the role defined in the connector properties
     */
    ROLE,
    /**
     * Assume the identity of the Presto user (authenticate with Okta using LDAP credentials)
     */
    OKTA_LDAP_PASSTHROUGH,
    /**
     * As above, but additionally map role using auth-to-local
     */
    ROLE_OKTA_LDAP_PASSTHROUGH,
    /**/;
}
