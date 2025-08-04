package io.trino.plugin.teradata.clearscape;

public class TeradataConstants
{
    public static final String DRIVER_CLASS = "com.teradata.jdbc.TeraDriver";
    public static final String DEFAULT_SCHEMA_NAME = "def_airbyte_db";
    public static final String PARAM_MODE = "mode";
    public static final String PARAM_SSL = "ssl";
    public static final String PARAM_SSL_MODE = "ssl_mode";
    public static final String PARAM_SSLMODE = "sslmode";
    public static final String PARAM_SSLCA = "sslca";
    public static final String ENCRYPTDATA = "ENCRYPTDATA";
    public static final String ENCRYPTDATA_ON = "ON";
    public static final String CA_CERTIFICATE = "ca.pem";
    public static final String ALLOW = "allow";
    public static final String REQUIRE = "require";
    public static final String VERIFY_CA = "verify-ca";
    public static final String VERIFY_FULL = "verify-full";
    public static final String CA_CERT_KEY = "ssl_ca_certificate";
    public static final String QUERY_BAND_KEY = "query_band";
    public static final String DEFAULT_QUERY_BAND = "org=teradata-internal-telem;appname=airbyte;";
    public static final String DEFAULT_QUERY_BAND_ORG = "org=teradata-internal-telem;";
    public static final String DEFAULT_QUERY_BAND_APPNAME = "appname=airbyte;";
    public static final String QUERY_BAND_SET = "SET QUERY_BAND='";
    public static final String QUERY_BAND_SESSION = "' FOR SESSION";
    public static final String LOG_MECH = "logmech";
    public static final String AUTH_TYPE = "auth_type";
    public static final String TD2_LOG_MECH = "BROWSER";
    private TeradataConstants()
    {
        // Utility class - prevent instantiation
    }
}
