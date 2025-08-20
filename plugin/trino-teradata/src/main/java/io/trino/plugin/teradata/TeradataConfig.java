/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import java.util.Optional;

/**
 * Configuration class for Teradata connector properties.
 * <p>
 * This class holds configuration options related to Teradata JDBC behavior,
 * including transaction mode, character set, logon mechanism, case sensitivity,
 * and default database.
 * </p>
 * <p>
 * Many of these properties correspond to Teradata JDBC connection parameters.
 * </p>
 */
public class TeradataConfig
        extends BaseJdbcConfig
{
    private Optional<String> oidcJWTToken = Optional.empty();
    private String logMech = "TD2";
    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SPECIFIC;
    /**
     * Gets the OIDC JWT token used for authentication.
     *
     * @return an Optional containing the JWT token if set, empty otherwise
     */
    public Optional<String> getOidcJwtToken()
    {
        return oidcJWTToken;
    }

    /**
     * Sets the OIDC JWT token for authentication with Teradata.
     *
     * @param jwtToken the JWT token string, can be null
     * @return this {@link TeradataConfig} instance for method chaining
     */
    @Config("jwt.token")
    public TeradataConfig setOidcJwtToken(String jwtToken)
    {
        this.oidcJWTToken = Optional.ofNullable(jwtToken);
        return this;
    }
    /**
     * Gets the current logon mechanism for Teradata authentication.
     *
     * @return the logon mechanism string (default: "TD2")
     */
    public String getLogMech()
    {
        return logMech;
    }
    /**
     * Sets the logon mechanism for Teradata authentication.
     * Common values include "TD2" for standard authentication,
     * "LDAP" for LDAP authentication, and "JWT" for JWT authentication.
     *
     * @param logMech the logon mechanism identifier
     * @return this {@link TeradataConfig} instance for method chaining
     */
    @Config("logon-mechanism")
    @ConfigDescription("Specifies the logon mechanism for Teradata (default: TD2). Use 'TD2' for TD2 authentication.")
    public TeradataConfig setLogMech(String logMech)
    {
        this.logMech = logMech;
        return this;
    }

    /**
     * Gets the Teradata case sensitivity setting.
     *
     * @return the current TeradataCaseSensitivity mode (default: CASE_SPECIFIC)
     */
    public TeradataCaseSensitivity getTeradataCaseSensitivity()
    {
        return teradataCaseSensitivity;
    }

    /**
     * Sets how char/varchar columns' case sensitivity will be exposed to Trino.
     *
     * @param teradataCaseSensitivity the case sensitivity mode
     * @return this {@link TeradataConfig} instance for method chaining
     */
    @Config("teradata.case-sensitivity")
    @ConfigDescription("How char/varchar columns' case sensitivity will be exposed to Trino (default: CASESPECIFIC).")
    public TeradataConfig setTeradataCaseSensitivity(TeradataCaseSensitivity teradataCaseSensitivity)
    {
        this.teradataCaseSensitivity = teradataCaseSensitivity;
        return this;
    }

    /**
     * Enum representing Teradata case sensitivity modes for char/varchar columns.
     * <ul>
     *   <li>NOT_CASE_SPECIFIC - case insensitive</li>
     *   <li>CASE_SPECIFIC - case sensitive</li>
     *   <li>AS_DEFINED - as defined by Teradata</li>
     * </ul>
     */
    enum TeradataCaseSensitivity
    {
        NOT_CASE_SPECIFIC, CASE_SPECIFIC, AS_DEFINED
    }
}
