package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class SpannerTableProperties
        implements TablePropertiesProvider
{
    public static final String PRIMARY_KEY = "primary_key";
    public static final String INTERLEAVE_IN_PARENT = "interleave_in_parent";
    public static final String ON_DELETE_CASCADE = "on_delete_cascade";
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SpannerTableProperties()
    {
        System.out.println("CALLED TABLE PROPERTIES ");
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        PRIMARY_KEY,
                        "Primary key for the table being created",
                        null,
                        false))
                .add(stringProperty(INTERLEAVE_IN_PARENT,
                        "Table name which needs to be interleaved with this table", null, false))
                .add(booleanProperty(ON_DELETE_CASCADE,
                        "Boolean property to cascade on delete. ON DELETE CASCADE if set to true or ON DELETE NO ACTION if false",
                        false, false))
                .build();
    }

    public static String getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (String) tableProperties.get(PRIMARY_KEY);
    }

    public static String getInterleaveInParent(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (String) tableProperties.get(INTERLEAVE_IN_PARENT);
    }

    public static boolean getOnDeleteCascade(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (boolean) tableProperties.get(ON_DELETE_CASCADE);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return sessionProperties;
    }
}
