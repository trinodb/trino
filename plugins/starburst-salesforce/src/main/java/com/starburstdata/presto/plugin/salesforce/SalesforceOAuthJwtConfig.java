/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotNull;

public class SalesforceOAuthJwtConfig
{
    private String pkcs12CertificateSubject = "*";
    private String pkcs12Path;
    private String pkcs12Password;
    private String jwtIssuer;
    private String jwtSubject;

    @NotNull
    public String getPkcs12CertificateSubject()
    {
        return pkcs12CertificateSubject;
    }

    @Config("salesforce.oauth.pkcs12-certificate-subject")
    @ConfigDescription("Certificate subject found in the PKCS12. Default is '*' meaning the first subject in the PKCS12")
    public SalesforceOAuthJwtConfig setPkcs12CertificateSubject(String pkcs12CertificateSubject)
    {
        this.pkcs12CertificateSubject = pkcs12CertificateSubject;
        return this;
    }

    @NotNull
    @FileExists
    public String getPkcs12Path()
    {
        return pkcs12Path;
    }

    @Config("salesforce.oauth.pkcs12-path")
    @ConfigDescription("Path on the filesystem to the PKCS12 containing a certificate added to the Salesforce Connected App")
    public SalesforceOAuthJwtConfig setPkcs12Path(String pkcs12Path)
    {
        this.pkcs12Path = pkcs12Path;
        return this;
    }

    @NotNull
    public String getPkcs12Password()
    {
        return pkcs12Password;
    }

    @Config("salesforce.oauth.pkcs12-password")
    @ConfigDescription("Password to unlock the PKCS12")
    @ConfigSecuritySensitive
    public SalesforceOAuthJwtConfig setPkcs12Password(String pkcs12Password)
    {
        this.pkcs12Password = pkcs12Password;
        return this;
    }

    @NotNull
    public String getJwtIssuer()
    {
        return jwtIssuer;
    }

    @Config("salesforce.oauth.jwt-issuer")
    @ConfigDescription("The issuer of the JWT, typically the client ID of the Salesforce OAuth Connected App.")
    public SalesforceOAuthJwtConfig setJwtIssuer(String jwtIssuer)
    {
        this.jwtIssuer = jwtIssuer;
        return this;
    }

    @NotNull
    public String getJwtSubject()
    {
        return jwtSubject;
    }

    @Config("salesforce.oauth.jwt-subject")
    @ConfigDescription("The user subject for which the application is requesting delegated access, typically the user account name or email address.")
    public SalesforceOAuthJwtConfig setJwtSubject(String jwtSubject)
    {
        this.jwtSubject = jwtSubject;
        return this;
    }
}
