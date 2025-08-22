package io.trino.plugin.teradata;

public class AuthenticationConfig
{
    private final String userName;
    private final String password;
    private final String jwtToken;
    private final String jwsPrivateKey;
    private final String jwsCertificate;
    private final String clientId;
    private final String clientSecret;

    public AuthenticationConfig()
    {
        this(null, null, null, null, null, null, null);
    }

    public AuthenticationConfig(String userName, String password)
    {
        this(userName, password, null, null, null, null, null);
    }

    public AuthenticationConfig(String userName, String password, String jwtToken, String jwsPrivateKey, String jwsCertificate,
            String clientId, String clientSecret)
    {
        this.userName = userName;
        this.password = password;
        this.jwtToken = jwtToken;
        this.jwsPrivateKey = jwsPrivateKey;
        this.jwsCertificate = jwsCertificate;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    // Getters only
    public String getUserName()
    {
        return userName;
    }

    public String getPassword()
    {
        return password;
    }

    public String getJwtToken()
    {
        return jwtToken;
    }

    public String getJwsPrivateKey()
    {
        return jwsPrivateKey;
    }

    public String getJwsCertificate()
    {
        return jwsCertificate;
    }

    public String getClientId()
    {
        return clientId;
    }

    public String getClientSecret()
    {
        return clientSecret;
    }
}
