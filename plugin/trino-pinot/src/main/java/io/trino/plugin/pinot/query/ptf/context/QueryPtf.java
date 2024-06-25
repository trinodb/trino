package io.trino.plugin.pinot.query.ptf.context;

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.PinotMetadata;
import io.trino.plugin.pinot.PinotTypeConverter;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;

import java.util.List;
import java.util.Map;

import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

// TODO: rename to Query when done
public class QueryPtf
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final PinotMetadata metadata;
    private final PinotClient client;
    private final PinotTypeConverter pinotTypeConverter;

    @Inject
    public QueryPtf(PinotMetadata metadata, PinotClient client, PinotTypeConverter pinotTypeConverter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.client = requireNonNull(client, "client is null");
        this.pinotTypeConverter = requireNonNull(pinotTypeConverter, "pinotTypeConverter is null");
    }

    public static class QueryPtfFunction
            extends AbstractConnectorTableFunction
    {
        private final PinotMetadata metadata;
        private final PinotClient client;
        private final PinotTypeConverter pinotTypeConverter;

        public QueryPtfFunction(PinotMetadata metadata, PinotClient client, PinotTypeConverter pinotTypeConverter)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY_OPTIONS")
                                    .type(new MapType(VARCHAR, VARCHAR, new TypeOperators()))
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.client = requireNonNull(client, "client is null");
            this.pinotTypeConverter = requireNonNull(pinotTypeConverter, "pinotTypeConverter is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            requireNonNull(arguments, "arguments is null");
            String query = ((Slice) ((ScalarArgument) arguments.get("QUERY")).getValue()).toStringUtf8();
            NullableValue nullableValue = ((ScalarArgument) arguments.get("QUERY_OPTIONS")).getNullableValue();

            return null;
        }
    }
}
