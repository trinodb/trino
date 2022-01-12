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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSalesforceOAuthConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SalesforceOAuthJwtConfig.class)
                .setPkcs12CertificateSubject("*")
                .setPkcs12Path(null)
                .setPkcs12Password(null)
                .setJwtIssuer(null)
                .setJwtSubject(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        // PKCS12 file must exist
        File file = Files.createTempFile("cert", ".p12").toFile();
        file.deleteOnExit();

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("salesforce.oauth.pkcs12-certificate-subject", "pkcs12-certificate-subject")
                .put("salesforce.oauth.pkcs12-path", file.getAbsolutePath())
                .put("salesforce.oauth.pkcs12-password", "pkcs12-password")
                .put("salesforce.oauth.jwt-issuer", "jwt-issuer")
                .put("salesforce.oauth.jwt-subject", "jwt-subject")
                .build();

        SalesforceOAuthJwtConfig expected = new SalesforceOAuthJwtConfig()
                .setPkcs12CertificateSubject("pkcs12-certificate-subject")
                .setPkcs12Path(file.getAbsolutePath())
                .setPkcs12Password("pkcs12-password")
                .setJwtIssuer("jwt-issuer")
                .setJwtSubject("jwt-subject");

        assertFullMapping(properties, expected);
    }
}
