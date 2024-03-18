/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.varada.it.smoke;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MoreCollectors;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import jakarta.ws.rs.HttpMethod;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class VaradaAbstractTestQueryFramework
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(VaradaAbstractTestQueryFramework.class);

    protected final ObjectMapper objectMapper = new ObjectMapperProvider().get();
    protected final Set<String> createdSchemas = new HashSet<>();
    protected final Set<String> createdTables = new HashSet<>();
    protected Path hiveDir;

    @BeforeAll
    @Override
    public void init()
            throws Exception
    {
        hiveDir = Files.createTempDirectory("hive_catalog_");
        super.init();
    }

    @AfterAll
    public void afterClass()
    {
        if (Objects.nonNull(hiveDir)) {
            try {
                Files.deleteIfExists(hiveDir);
            }
            catch (IOException e) {
                logger.warn(e, "failed to delete hive dir [%s]", hiveDir);
            }
        }
    }

    @BeforeEach
    public void beforeMethod(TestInfo testInfo)
    {
        logger.info("Starting test: %s", testInfo.getTestMethod());
    }

    /**
     * the following section is needed to trigger the catalog distribution.
     * catalog are loaded lazily in the workers on the first time it is used.
     * this is needed when running an IT which is not using single env
     */
    protected void initializeWorkers(String catalogName)
    {
        assertUpdate("CREATE SCHEMA fake_schema");
        assertUpdate(String.format("CREATE TABLE %s.fake_schema.fake_table (%s integer, %s varchar(20))", catalogName, "col1", "col2"));

        computeActual(String.format("INSERT INTO %s.fake_schema.fake_table VALUES (1, 'shlomi')", catalogName));
        getQueryRunner().execute(String.format("drop table %s.fake_schema.fake_table", catalogName));
        getQueryRunner().execute(String.format("drop schema %s.fake_schema", catalogName));
    }

    @AfterEach
    public void afterMethod(TestInfo testInfo)
    {
        logger.info("Finished test: %s", testInfo.getTestMethod());
    }

    protected void assertTableExists(String schema, String table)
    {
        MaterializedResult result = computeActual("show tables from " + schema);
        assertThat(result.getRowCount()).isGreaterThanOrEqualTo(1);

        Optional<MaterializedRow> optionalMaterializedRow = result.getMaterializedRows()
                .stream()
                .filter(materializedRowTmp -> materializedRowTmp.getField(0).equals(table))
                .findFirst();

        assertThat(optionalMaterializedRow).isPresent();

        MaterializedRow materializedRow = optionalMaterializedRow.orElseThrow();
        assertThat(materializedRow.getFields().size()).isEqualTo(1);
        assertThat(materializedRow.getFields().stream().collect(MoreCollectors.onlyElement())).isEqualTo(table);

        result = computeActual("SELECT * FROM " + schema + "." + table);
        assertThat(result.getRowCount()).isZero();
    }

    protected void assertSchema(String schema)
    {
        MaterializedResult result = computeActual("show schemas");

        Optional<MaterializedRow> optionalMaterializedRow = result.getMaterializedRows()
                .stream()
                .filter(materializedRowTmp -> materializedRowTmp.getField(0).equals(schema))
                .findFirst();

        assertThat(optionalMaterializedRow.isPresent()).isTrue();
    }

    void assertSchemaIsEmpty(String schema)
    {
        MaterializedResult result = computeActual("show tables from " + schema);
        assertThat(result.getRowCount()).isEqualTo(0);
    }

    protected void createSchemaAndTable(String schemaName, String tableName, String columns)
    {
        assertUpdate("CREATE SCHEMA " + schemaName);

        assertSchema(schemaName);

        assertSchemaIsEmpty(schemaName);

        assertUpdate(format("CREATE TABLE %s.%s %s", schemaName, tableName, columns));

        assertTableExists(schemaName, tableName);
        createdSchemas.add(schemaName);
        createdTables.add(schemaName + "." + tableName);
    }

    @SuppressWarnings("LanguageMismatch")
    void execSilently(String sql)
    {
        try {
            computeActual(sql);
        }
        catch (Throwable t) {
            logger.error(t);
        }
    }

    protected String executeRestCommand(String ext)
            throws IOException
    {
        return executeRestCommand(ext, null);
    }

    protected String executeRestCommand(String ext, Object inObj)
            throws IOException
    {
        return executeRestCommand(ext, inObj, HttpURLConnection.HTTP_OK);
    }

    protected String executeRestCommand(String ext, Object inObj, int responseCode)
            throws IOException
    {
        return executeRestCommand("/v1/ext/varada/", ext, inObj, HttpMethod.POST, responseCode);
    }

    protected String executeRestCommand(String prefix, String ext, Object inObj, String httpMethod, int responseCode)
            throws IOException
    {
        URI baseUrl = ((DistributedQueryRunner) this.getQueryRunner()).getCoordinator().getBaseUrl();
        prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        prefix = prefix.startsWith("/") ? prefix : "/" + prefix;

        URL url = URI.create(baseUrl.getScheme() + "://" + baseUrl.getHost() + ":" + (baseUrl.getPort() + 1) + prefix + ext).toURL();

        Request.Builder request;
        if (HttpMethod.GET.equals(httpMethod)) {
            request = prepareGet();
        }
        else if (HttpMethod.DELETE.equals(httpMethod)) {
            request = prepareDelete();
        }
        else {
            request = preparePost();
        }
        try {
            request.setHeader("Content-Type", "application/json");
            request.setUri(url.toURI());
        }
        catch (URISyntaxException e) {
            logger.error(e);
        }
        HttpClient client = new JettyHttpClient();
        if (Objects.nonNull(inObj)) {
            String input = objectMapper.writeValueAsString(inObj);
            request.setBodyGenerator(createStaticBodyGenerator(input.getBytes(Charset.defaultCharset())));
        }
        StringResponseHandler stringResponseHandler = StringResponseHandler.createStringResponseHandler();
        StringResponseHandler.StringResponse response = client.execute(request.build(), stringResponseHandler);
        assertThat(response.getStatusCode()).describedAs(response.getBody()).isEqualTo(responseCode);
        client.close();
        return response.getBody();
    }
}
