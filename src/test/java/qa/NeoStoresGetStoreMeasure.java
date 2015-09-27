package qa;

import org.junit.Test;
import qa.perf.Operation;
import qa.perf.Operations;
import qa.perf.Performance;

import static qa.perf.Operations.single;

public class NeoStoresGetStoreMeasure
{
    @Test
    public void shouldMeasure() throws Exception
    {
        NeoStoresTarget target = new NeoStoresTarget( true );
        Performance.measure( target, Operations.<NeoStoresTarget>noop(), single( new Operation<NeoStoresTarget>()
        {
            @Override
            public void perform( NeoStoresTarget on )
            {
                on.stores().getNodeStore();
            }
        } ), 20, 10 );
    }
}
