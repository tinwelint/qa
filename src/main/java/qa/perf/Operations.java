package qa.perf;

public class Operations
{
    private Operations()
    {
        throw new AssertionError();
    }

    public static <T extends Target> OperationSet<T> single( final Operation<T> operation )
    {
        return new OperationSet<T>()
        {
            @Override
            public void perform( T on )
            {
                operation.perform( on );
            }

            @Override
            public Operation<T> at( float value )
            {
                return operation;
            }
        };
    }
}
