package qa.perf;

public interface OperationSet<T extends Target>
{
    /**
     * @param value between 0..1
     * @return an {@link Operation} corresponding to the chance of value.
     */
    Operation<T> at( float value );

    /**
     * Performs a random operation in this set.
     * @param on {@link Target} to execute on.
     */
    void perform( T on );
}
