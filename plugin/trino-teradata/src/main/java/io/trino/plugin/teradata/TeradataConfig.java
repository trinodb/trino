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
    private Boolean stringPushdownEnabled = false;
    private Optional<String> jwtCredentialsFile = Optional.empty();
    private Optional<String> jwtToken = Optional.empty();
    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SPECIFIC;

    public Optional<String> getJwtCredentialsFile()
    {
        return jwtCredentialsFile;
    }

    @Config("jwt.credentials-file")
    public TeradataConfig setJwtCredentialsFile(String jwtCredentialsFile)
    {
        this.jwtCredentialsFile = Optional.ofNullable(jwtCredentialsFile);
        return this;
    }

    public Optional<String> getJwtToken()
    {
        return jwtToken;
    }

    @Config("jwt.token")
    public TeradataConfig setJwtToken(String jwtToken)
    {
        this.jwtToken = Optional.ofNullable(jwtToken);
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

    public Boolean isStringPushdownEnabled()
    {
        return stringPushdownEnabled;
    }

    @Config("teradata.string-pushdown-enabled")
    @ConfigDescription("Enable pushdown of string predicates (VARCHAR, CHAR) to Teradata")
    public TeradataConfig setStringPushdownEnabled(boolean enabled)
    {
        this.stringPushdownEnabled = enabled;
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
