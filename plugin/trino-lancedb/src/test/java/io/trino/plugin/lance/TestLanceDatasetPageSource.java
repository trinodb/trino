package io.trino.plugin.lance;

import com.google.common.io.Resources;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceDatasetPageSource
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceDbURL = Resources.getResource(LanceReader.class, "/example_db");
        assertThat(lanceDbURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig().setLanceDbUri(lanceDbURL.toString());
        LanceReader lanceReader = new LanceReader(lanceConfig);
        this.metadata = new LanceMetadata(lanceReader, lanceConfig);
    }

    @Test
    public void testTableScan()
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(null, TEST_TABLE_1);
        LanceDatasetPageSource pageSource = new LanceDatasetPageSource(metadata.getLanceReader(), (LanceTableHandle) tableHandle, metadata.getLanceConfig().getFetchRetryCount());

        Page page = pageSource.getNextPage();
        assertThat(page).isNotNull();
    }
}
