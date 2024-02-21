/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.models.Operation;

import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public abstract class OperationsHandler<T extends OperationsHandler<T>>
{
    private <O extends Operation> void put(ImmutableMap.Builder<Class<? extends Operation>, BiConsumer<OperationsHandler<T>, ? super Operation>> builder, Class<O> operationType, BiConsumer<OperationsHandler<T>, O> handler)
    {
        //noinspection unchecked
        builder.put(operationType, (BiConsumer<OperationsHandler<T>, Operation>) handler);   // cast is guaranteed type safe
    }

    private final Map<Class<? extends Operation>, BiConsumer<OperationsHandler<T>, ? super Operation>> handlers;

    protected OperationsHandler()
    {
        ImmutableMap.Builder<Class<? extends Operation>, BiConsumer<OperationsHandler<T>, ? super Operation>> builder = ImmutableMap.builder();
        put(builder, Operation.CreateSchema.class, OperationsHandler::createSchema);
        put(builder, Operation.Comment.class, OperationsHandler::generateComment);
        put(builder, Operation.DropTable.class, OperationsHandler::dropTable);
        put(builder, Operation.CreateTable.class, OperationsHandler::createTable);
        put(builder, Operation.AddColumn.class, OperationsHandler::addColumn);
        put(builder, Operation.AddPartitionColumn.class, OperationsHandler::addPartitionColumn);
        put(builder, Operation.DropColumn.class, OperationsHandler::dropColumn);
        put(builder, Operation.DropPartitionColumn.class, OperationsHandler::dropPartitionColumn);
        put(builder, Operation.AddPartitionValue.class, OperationsHandler::addPartitionValue);
        put(builder, Operation.DropPartitionValue.class, OperationsHandler::dropPartitionValue);
        put(builder, Operation.RenameColumn.class, OperationsHandler::renameColumn);
        put(builder, Operation.RenamePartitionColumn.class, OperationsHandler::renamePartitionColumn);
        put(builder, Operation.AddBucket.class, OperationsHandler::addBucket);
        put(builder, Operation.DropBucket.class, OperationsHandler::dropBucket);
        put(builder, Operation.RegisterTable.class, OperationsHandler::registerTable);
        put(builder, Operation.UnregisterTable.class, OperationsHandler::unregisterTable);
        handlers = builder.buildOrThrow();

        for (Class<?> clazz : Operation.class.getPermittedSubclasses()) {
            requireNonNull(handlers.get(clazz), "Missing handler for operation: " + clazz.getSimpleName());
        }
    }

    public final void apply(Operation operation)
    {
        BiConsumer<OperationsHandler<T>, ? super Operation> handler = requireNonNull(handlers.get(operation.getClass()), "no handler found for: " + operation.getClass().getSimpleName());   // null isn't actually possible given that Operation is sealed and the static initializer tests for all permitted subclasses
        handler.accept(this, operation);
    }

    protected abstract void generateComment(Operation.Comment comment);

    protected abstract void createSchema(Operation.CreateSchema schema);

    protected abstract void dropTable(Operation.DropTable dropTable);

    protected abstract void createTable(Operation.CreateTable createTable);

    protected abstract void addColumn(Operation.AddColumn addColumn);

    protected abstract void addPartitionColumn(Operation.AddPartitionColumn addPartitionColumn);

    protected abstract void dropColumn(Operation.DropColumn dropColumn);

    protected abstract void dropPartitionColumn(Operation.DropPartitionColumn dropPartitionColumn);

    protected abstract void addPartitionValue(Operation.AddPartitionValue addPartitionValue);

    protected abstract void dropPartitionValue(Operation.DropPartitionValue dropPartitionValue);

    protected abstract void renameColumn(Operation.RenameColumn renameColumn);

    protected abstract void renamePartitionColumn(Operation.RenamePartitionColumn renamePartitionColumn);

    protected abstract void addBucket(Operation.AddBucket addBucket);

    protected abstract void dropBucket(Operation.DropBucket dropBucket);

    protected abstract void registerTable(Operation.RegisterTable registerTable);

    protected abstract void unregisterTable(Operation.UnregisterTable unregisterTable);
}
