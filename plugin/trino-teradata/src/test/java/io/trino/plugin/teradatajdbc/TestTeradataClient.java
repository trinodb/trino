package io.trino.plugin.teradatajdbc;

import io.trino.plugin.teradatajdbc.TeradataQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;


import io.trino.plugin.jdbc.*;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.Optional;

import static org.testng.Assert.*;

public class TestTeradataClient {
    private static final String JDBC_URL = "jdbc:teradata://localhost/DATABASE=dbc";
    private static final String USER = "dbc";
    private static final String PASSWORD = "dbc";

    private TeradataClient createClient() {
        BaseJdbcConfig config = new BaseJdbcConfig() {
            public String getConnectionUrl() {
                return JDBC_URL;
            }
        };
        ConnectionFactory connectionFactory = new DriverConnectionFactory(config, Optional.empty(), () -> {
            try {
                return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return new TeradataClient(config, connectionFactory);
    }

    @Test
    public void testGetSchemaNames() throws Exception {
        TeradataClient client = createClient();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            ConnectorSession session = TestingJdbcClientSession.session();
            var result = client.getSchemaNames(session);
            assertTrue(result.contains("DBC"));
        }
    }

    @Test
    public void testToWriteMapping() {
        TeradataClient client = createClient();
        Optional<WriteMapping> varcharMapping = client.toWriteMapping(TestingJdbcClientSession.session(), io.trino.spi.type.VarcharType.VARCHAR);
        assertTrue(varcharMapping.isPresent());
        assertEquals(varcharMapping.get().getDataType(), "VARCHAR");
    }
}


