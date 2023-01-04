package io.trino.plugin.deltalake;

import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DeltaLakeCDFTableFunction
        implements Provider<ConnectorTableFunction>
{
    public static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    public static final String START_VERSION_ARGUMENT = "START_VERSION";
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "table_changes";

    private final HiveMetastoreFactory metastoreFactory;

    @Inject
    public DeltaLakeCDFTableFunction(HiveMetastoreFactory metastoreFactory)
    {
        this.metastoreFactory = metastoreFactory;
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new TableChangesFunction(metastoreFactory), getClass().getClassLoader());
    }

    public static class TableChangesFunction
            extends AbstractConnectorTableFunction
    {
        private final HiveMetastoreFactory metastoreFactory;
        public TableChangesFunction(HiveMetastoreFactory metastoreFactory)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name(START_VERSION_ARGUMENT).type(INTEGER).build()
                    ),
                    GENERIC_TABLE);
            this.metastoreFactory = metastoreFactory;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));
            String tablePath = ((Slice) ((ScalarArgument) arguments.get(TABLE_NAME_ARGUMENT)).getValue()).toStringUtf8();
            long version = (Long) ((ScalarArgument) arguments.get(START_VERSION_ARGUMENT)).getValue();
            int pointIndex = tablePath.indexOf(".");
            String schema = tablePath.substring(0, pointIndex);
            String tableName = tablePath.substring(pointIndex + 1);
            Optional<Table> table = metastore.getTable(schema, tableName);
            metastore.getS
            return null;
        }
    }
}
