/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.inject.Inject;
import io.trino.jdbc.TrinoDriverUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.spi.connector.ConnectorSession;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PasswordCatalogIdentityFactory
        implements StargateCatalogIdentityFactory
{
    private final String catalogIdentity;

    @Inject
    public PasswordCatalogIdentityFactory(BaseJdbcConfig config, StargateCredentialConfig credentialConfig, StargateConfig stargateConfig)
    {
        // Use TrinoDriverUri to retrieve the catalog name
        TrinoDriverUri uri;
        try {
            uri = TrinoDriverUri.create(config.getConnectionUrl(), new Properties());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Mask the catalog name in the connection URL
        String catalog = uri.getCatalog().orElseThrow();
        String baseUri = "jdbc:trino://" + uri.getAddress() + "/";
        String newConnectionUrl = config.getConnectionUrl().replace(baseUri + catalog, baseUri + "***");
        verify(!config.getConnectionUrl().equals(newConnectionUrl), "Cannot replace the catalog name");

        Properties properties = new Properties();
        properties.setProperty("url", newConnectionUrl);
        credentialConfig.getConnectionUser().ifPresent(user -> properties.setProperty("user", user));
        credentialConfig.getConnectionPassword().ifPresent(password -> properties.setProperty("password", password));
        properties.setProperty("authentication", stargateConfig.getAuthenticationType());
        properties.setProperty("SSL", Boolean.toString(stargateConfig.isSslEnabled()));

        // Hash to avoid accidental leaking secrets via log and error messages
        this.catalogIdentity = sha256().hashString(properties.toString(), UTF_8).toString();
    }

    @Override
    public String getCatalogIdentity(ConnectorSession session)
    {
        return catalogIdentity;
    }
}
