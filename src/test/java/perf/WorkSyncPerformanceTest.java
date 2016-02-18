package perf;

import org.junit.Test;
import qa.perf.Operation;
import qa.perf.Performance;
import qa.perf.Target;

import java.util.concurrent.TimeUnit;

import org.neo4j.concurrent.Work;
import org.neo4j.concurrent.WorkSync;

public class WorkSyncPerformanceTest
{
    @Test
    public void shouldMeasure() throws Exception
    {
        Performance.measure( new WorkSyncTarget() )
                .withAllCores()
                .withOperations( addWork() )
                .withDuration( 30 )
                .please();
    }

    private Operation<WorkSyncTarget> addWork()
    {
        return (target) -> target.workSync.apply( new IntegerWork( 1 ) );
    }

    private static class WorkSyncTarget implements Target
    {
        protected final Material material = new Material();
        protected WorkSync<Material,IntegerWork> workSync;

        @Override
        public void start() throws Exception
        {
            workSync = new WorkSync<>( material );
        }

        @Override
        public void stop()
        {
            System.out.println( material.getTotal() );
        }
    }

    private static class Material
    {
        private int total;

        public int getTotal()
        {
            return total;
        }
    }

    private static class IntegerWork implements Work<Material,IntegerWork>
    {
        private final int add;

        public IntegerWork( int add )
        {
            this.add = add;
        }

        @Override
        public IntegerWork combine( IntegerWork work )
        {
            return new IntegerWork( add + work.add );
        }

        @Override
        public void apply( Material material )
        {
            material.total += add;
            usleep( 1 );
        }
    }

    private static void usleep( long micros )
    {
        long deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos( micros );
        long now;
        do
        {
            now = System.nanoTime();
        }
        while ( now < deadline );
    }
}
