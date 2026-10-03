package tech.ydb.trino;

import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class YdbTableProperties implements TablePropertiesProvider {
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";

    private final List<PropertyMetadata<?>> tableProperties = List.of(
            new PropertyMetadata<>(
                    PRIMARY_KEY_PROPERTY,
                    "Columns forming the table primary key, in order",
                    new ArrayType(VARCHAR),
                    List.class,
                    List.of(),
                    false,
                    value -> (List<?>) value,
                    value -> value));

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    public static List<String> getPrimaryKey(Map<String, Object> tableProperties) {
        requireNonNull(tableProperties, "tableProperties is null");
        return ((List<?>) tableProperties.get(PRIMARY_KEY_PROPERTY)).stream()
                .map(String.class::cast)
                .toList();
    }
}
