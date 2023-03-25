package io.trino.plugin.spanner;

import io.airlift.configuration.Config;

public class SpannerConfig
{
    private String credentialsFile;

    public String getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("spanner.credentials.file")
    public void setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = credentialsFile;
    }
}
