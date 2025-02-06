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

package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.ArrayType;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MAX_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MIN_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.STEP_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class FakerConnector
        implements Connector
{
    private final FakerMetadata metadata;
    private final FakerSplitManager splitManager;
    private final FakerPageSourceProvider pageSourceProvider;
    private final FakerPageSinkProvider pageSinkProvider;
    private final FakerFunctionProvider functionProvider;

    @Inject
    public FakerConnector(
            FakerMetadata metadata,
            FakerSplitManager splitManager,
            FakerPageSourceProvider pageSourceProvider,
            FakerPageSinkProvider pageSinkProvider,
            FakerFunctionProvider functionProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.functionProvider = requireNonNull(functionProvider, "functionPovider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return FakerTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return Set.of(NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        SchemaInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in any column that allows them, in any table of this schema",
                        null,
                        nullProbability -> checkProperty(0 <= nullProbability && nullProbability <= 1, INVALID_SCHEMA_PROPERTY, "null_probability value must be between 0 and 1, inclusive"),
                        false),
                longProperty(
                        SchemaInfo.DEFAULT_LIMIT_PROPERTY,
                        "Default limit of rows returned from any table in this schema, if not specified in the query",
                        null,
                        defaultLimit -> checkProperty(1 <= defaultLimit, INVALID_SCHEMA_PROPERTY, "default_limit value must be equal or greater than 1"),
                        false),
                booleanProperty(
                        SchemaInfo.SEQUENCE_DETECTION_ENABLED,
                        """
                        If true, when creating a table using existing data, columns with the number of distinct values close to
                        the number of rows are treated as sequences""",
                        null,
                        false),
                booleanProperty(
                        SchemaInfo.DICTIONARY_DETECTION_ENABLED,
                        """
                        If true, when creating a table using existing data, columns with a low number of distinct values
                        are treated as dictionaries, and get the allowed_values column property populated with random values""",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        TableInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in any column in this table that allows them",
                        null,
                        nullProbability -> checkProperty(0 <= nullProbability && nullProbability <= 1, INVALID_TABLE_PROPERTY, "null_probability value must be between 0 and 1, inclusive"),
                        false),
                longProperty(
                        TableInfo.DEFAULT_LIMIT_PROPERTY,
                        "Default limit of rows returned from this table if not specified in the query",
                        null,
                        defaultLimit -> checkProperty(1 <= defaultLimit, INVALID_TABLE_PROPERTY, "default_limit value must be equal or greater than 1"),
                        false),
                booleanProperty(
                        TableInfo.SEQUENCE_DETECTION_ENABLED,
                        """
                        If true, when creating a table using existing data, columns with the number of distinct values close to
                        the number of rows are treated as sequences""",
                        null,
                        false),
                booleanProperty(
                        TableInfo.DICTIONARY_DETECTION_ENABLED,
                        """
                        If true, when creating a table using existing data, columns with a low number of distinct values
                        are treated as dictionaries, and get the allowed_values column property populated with random values""",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return ImmutableList.of(
                doubleProperty(
                        ColumnInfo.NULL_PROBABILITY_PROPERTY,
                        "Default probability of null values in this column, if it allows them",
                        null,
                        nullProbability -> checkProperty(0 <= nullProbability && nullProbability <= 1, INVALID_COLUMN_PROPERTY, "null_probability value must be between 0 and 1, inclusive"),
                        false),
                stringProperty(
                        ColumnInfo.GENERATOR_PROPERTY,
                        "Name of the Faker library generator used to generate data for this column",
                        null,
                        generator -> {
                            try {
                                pageSourceProvider.validateGenerator(generator);
                            }
                            catch (RuntimeException e) {
                                throw new TrinoException(INVALID_COLUMN_PROPERTY, "generator must be a valid Faker expression", e);
                            }
                        },
                        false),
                stringProperty(
                        MIN_PROPERTY,
                        "Minimum generated value (inclusive)",
                        null,
                        false),
                stringProperty(
                        MAX_PROPERTY,
                        "Maximum generated value (inclusive)",
                        null,
                        false),
                new PropertyMetadata<>(
                        ALLOWED_VALUES_PROPERTY,
                        "List of allowed values",
                        new ArrayType(VARCHAR),
                        List.class,
                        null,
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .collect(toImmutableList()),
                        value -> value),
                stringProperty(
                        STEP_PROPERTY,
                        "If set, generate sequential values with this step. For date and time columns set this to a duration",
                        null,
                        false));
    }

    private static void checkProperty(boolean expression, ErrorCodeSupplier errorCode, String errorMessage)
    {
        if (!expression) {
            throw new TrinoException(errorCode, errorMessage);
        }
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }
}
