/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradatajdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class TeradataConfig
{
    enum TeradataMode {
        DEFAULT,
        TERA,
        ANSI
    }

    enum TeradataCaseSensitivity {
        NOT_CASE_SPECIFIC,
        CASE_SPECIFIC,
        AS_DEFINED
    }

    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SPECIFIC;
    // TODO add queryband config
    // TODO below could all be part of the connection url so they should go away
    private TeradataMode teradataMode = TeradataMode.DEFAULT;
    private String sessionCharacterSet = "UTF8";
    private String logonMechanism = "TD2";
    private String defaultDatabase;

    public TeradataCaseSensitivity getTeradataCaseSensitivity()
    {
        return teradataCaseSensitivity;
    }

    @Config("teradatajdbc.case-sensitivity")
    @ConfigDescription("How char/varchar columns' case sensitivity will be exposed to Trino (default: CASESPECIFIC).")
    public TeradataConfig setTeradataCaseSensitivity(TeradataCaseSensitivity teradataCaseSensitivity)
    {
        this.teradataCaseSensitivity = teradataCaseSensitivity;
        return this;
    }

    public TeradataMode getTeradataMode()
    {
        return teradataMode;
    }

    @Config("teradatajdbc.transaction-mode")
    @ConfigDescription("Whether to use TERA or ANSI mode for Teradata operations (default: DEFAULT)")
    public TeradataConfig setTeradataMode(TeradataMode teradataMode)
    {
        this.teradataMode = teradataMode;
        return this;
    }

    public String getSessionCharacterSet()
    {
        return sessionCharacterSet;
    }

    @Config("teradatajdbc.charset")
    @ConfigDescription("Session character set to use (default: UTF8)")
    public TeradataConfig setSessionCharacterSet(String sessionCharacterSet)
    {
        this.sessionCharacterSet = sessionCharacterSet;
        return this;
    }

    public String getLogonMechanism()
    {
        return logonMechanism;
    }

    @Config("teradatajdbc.logmech")
    @ConfigDescription("Logon mechanism to use (default: TD2)")
    public TeradataConfig setLogonMechanism(String logonMechanism)
    {
        this.logonMechanism = logonMechanism;
        return this;
    }

    public String getDefaultDatabase()
    {
        return defaultDatabase;
    }

    @Config("teradatajdbc.default-database")
    @ConfigDescription("Default database to use (optional)")
    public TeradataConfig setDefaultDatabase(String defaulDatabase)
    {
        this.defaultDatabase = defaulDatabase;
        return this;
    }
}
