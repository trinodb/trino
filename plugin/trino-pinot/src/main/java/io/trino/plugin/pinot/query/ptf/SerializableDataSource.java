package io.trino.plugin.pinot.query.ptf;

import org.apache.pinot.common.request.DataSource;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SerializableDataSource(Optional<String> tableName, Optional<SerializablePinotQuery> subQuery, Optional<SerializableJoin> join)
{
    public SerializableDataSource
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(subQuery, "subQuery is null");
        requireNonNull(join, "join is null");
    }

    public SerializableDataSource(DataSource dataSource)
    {
        this(Optional.ofNullable(dataSource.getTableName()), Optional.ofNullable(dataSource.getSubquery())
                .map(SerializablePinotQuery::new),
                Optional.ofNullable(dataSource.getJoin())
                        .map(SerializableJoin::new));
    }

    public DataSource toDataSource()
    {
        DataSource dataSource = new DataSource();
        tableName.ifPresent(dataSource::setTableName);
        subQuery.map(SerializablePinotQuery::toPinotQuery).ifPresent(dataSource::setSubquery);
        join.map(SerializableJoin::toJoin).ifPresent(dataSource::setJoin);
        return dataSource;
    }
}
