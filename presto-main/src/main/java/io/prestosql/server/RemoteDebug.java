import com.google.inject.Injector;

import java.util.function.Function;

public class RemoteDebug
        implements Function<Injector, String>
{
    @Override
    public String apply(Injector injector)
    {
        return "hello world";
    }
}
