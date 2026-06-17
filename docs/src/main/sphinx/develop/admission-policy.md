# Admission policy

Trino lets you plug in a custom admission policy that decides whether a query
should wait for cluster capacity before dispatch. The engine invokes the bound
policy exactly once per query, at the moment the query enters the
`WAITING_FOR_RESOURCES` state, and acts on the returned decision.

This SPI replaces the single hard-coded gate that previously called
`ClusterSizeMonitor.waitForMinimumWorkers(...)`. Vanilla Trino installations
keep that behavior — the default policy, named `min-workers`, wraps the
existing wait so upgrading is a no-op for operators who do nothing.

## SPI surface

The admission SPI lives in the `io.trino.spi.admission` package of the
`trino-spi` module. It is composed of four types: one decision interface, one
factory interface, one engine-supplied context record, and one sealed return
type with two variants.

### `AdmissionPolicy`

The decision interface declares one method. Implementations must be
thread-safe and must not block.

```java
public interface AdmissionPolicy
{
    WaitDecision shouldQueryWait(QueryAdmissionContext query);
}
```

Returning `null` or throwing any `Throwable` is treated as a policy failure;
the engine fails the query with `GENERIC_INSUFFICIENT_RESOURCES` and preserves
the thrown exception as the failure cause.

### `AdmissionPolicyFactory`

A factory exposes a stable name and builds policy instances from the
operator-supplied properties map.

```java
public interface AdmissionPolicyFactory
{
    String getName();

    AdmissionPolicy create(Map<String, String> config);
}
```

The name is matched case-sensitively against the
`query-manager.admission-policy.name` configuration key. Returning `null` or
throwing from `create()` fails coordinator startup with an error naming the
factory and the underlying cause.

### `QueryAdmissionContext`

The engine hands the policy a narrow record describing the query. It does not
expose the session, the plan, or any coordinator-internal type — vendor
policies that need cluster-state signals maintain their own observers.

```java
public record QueryAdmissionContext(
        QueryId queryId,
        String userName,
        Optional<String> resourceGroupId,
        Map<String, String> sessionProperties) {}
```

### `WaitDecision`

The return type is a sealed interface with exactly two variants.

```java
public sealed interface WaitDecision
        permits WaitDecision.ProceedNow, WaitDecision.Wait
{
    String reason();

    record ProceedNow(String reason) implements WaitDecision { /* ... */ }

    record Wait(Duration maxWait, String reason) implements WaitDecision { /* ... */ }
}
```

`ProceedNow` skips the gate and starts the query immediately. `Wait` gates the
query under the policy's release condition for at most `maxWait`; if the
release condition is not met before the timeout, the engine fails the query
with `GENERIC_INSUFFICIENT_RESOURCES`. Each variant carries a non-null
`reason` string of length 1 to 256 for observability.

The constructors validate their arguments. `Wait.maxWait` must be non-null and
non-negative; `reason` must be non-null with length in `[1, 256]`.

## Plugin discovery

A plugin exposes its admission-policy factories by overriding the default
method on `io.trino.spi.Plugin`:

```java
public interface Plugin
{
    default Iterable<AdmissionPolicyFactory> getAdmissionPolicyFactories()
    {
        return emptyList();
    }
    // ... other factory accessors
}
```

The coordinator iterates this method on every loaded plugin and registers each
factory. Two factories sharing a name (case-sensitive) is a startup error
naming both source plugins.

## Configuration

Operators select the bound policy with a single configuration key in
`etc/config.properties`:

```text
query-manager.admission-policy.name=min-workers
```

The default value is `min-workers`, the OSS implementation that preserves
existing behavior. Setting the key to a value that does not match any
registered factory is a startup error; the engine never silently falls back to
the default.

When the selected factory needs additional properties, place them in
`etc/admission-policy.properties`:

```text
custom-property1=custom-value1
custom-property2=custom-value2
```

The contents of the file are parsed into a `Map<String, String>` and passed to
`AdmissionPolicyFactory.create()`. The file is optional — when absent, the
factory receives an empty map. Vanilla operators do not need to create it.

The bound policy is retained for the lifetime of the coordinator process;
runtime hot-swap is not supported. Restart the coordinator to switch policies.

## Worked example

A minimal policy that admits every query immediately:

```java
package com.example.admission;

import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.QueryAdmissionContext;
import io.trino.spi.admission.WaitDecision;

public class AlwaysProceedPolicy
        implements AdmissionPolicy
{
    @Override
    public WaitDecision shouldQueryWait(QueryAdmissionContext query)
    {
        return new WaitDecision.ProceedNow("no admission gate configured");
    }
}
```

The factory exposes the policy by name:

```java
package com.example.admission;

import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;

import java.util.Map;

public class AlwaysProceedPolicyFactory
        implements AdmissionPolicyFactory
{
    @Override
    public String getName()
    {
        return "always-proceed";
    }

    @Override
    public AdmissionPolicy create(Map<String, String> config)
    {
        return new AlwaysProceedPolicy();
    }
}
```

The plugin returns the factory from `getAdmissionPolicyFactories()`:

```java
package com.example.admission;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.admission.AdmissionPolicyFactory;

public class AlwaysProceedPlugin
        implements Plugin
{
    @Override
    public Iterable<AdmissionPolicyFactory> getAdmissionPolicyFactories()
    {
        return ImmutableList.of(new AlwaysProceedPolicyFactory());
    }
}
```

After installing the plugin, the operator selects it in
`etc/config.properties`:

```text
query-manager.admission-policy.name=always-proceed
```

No `etc/admission-policy.properties` file is required for this example. If the
factory needed properties, the operator would create that file alongside
`config.properties`.

## Default policy

The `min-workers` factory is bundled with the coordinator and is selected by
default. Its policy returns a `WaitDecision.Wait`, and the engine handles that
decision exactly as today's `ClusterSizeMonitor.waitForMinimumWorkers(...)`
call site did — applying the existing
`query-manager.required-workers` and `query-manager.required-workers-max-wait`
configuration keys without modification. Operators upgrading to a release that
ships this SPI see no behavior change unless they explicitly bind a different
policy.
