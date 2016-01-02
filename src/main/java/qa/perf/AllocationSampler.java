package qa.perf;

import com.google.monitoring.runtime.instrumentation.Sampler;

public interface AllocationSampler extends Sampler
{
    void close( long totalOps );
}
