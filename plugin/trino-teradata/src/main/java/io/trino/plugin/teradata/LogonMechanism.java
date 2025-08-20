package io.trino.plugin.teradata;

public enum LogonMechanism
{
    TD2("TD2"),
    JWT("JWT"),
    SECRET("SECRET");

    private final String mechanism;

    LogonMechanism(String mechanism)
    {
        this.mechanism = mechanism;
    }

    public static LogonMechanism fromString(String value)
    {
        for (LogonMechanism logonMechanism : values()) {
            if (logonMechanism.getMechanism().equalsIgnoreCase(value)) {
                return logonMechanism;
            }
        }
        throw new IllegalArgumentException("Unknown logon mechanism: " + value);
    }

    public String getMechanism()
    {
        return mechanism;
    }
}
