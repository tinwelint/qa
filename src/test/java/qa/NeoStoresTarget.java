package qa;

import qa.perf.Target;

import java.io.File;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class NeoStoresTarget implements Target
{
    private final boolean eager;
    private NeoStores stores;

    public NeoStoresTarget( boolean eager )
    {
        this.eager = eager;
    }

    @Override
    public void start() throws Exception
    {
        File storeDir = new File( DEFAULT_DIR );
        deleteRecursively( storeDir );
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fs, new Config(),
                NULL, NullLog.getInstance() );
        StoreFactory storeFactory = new StoreFactory( fs, storeDir, pageCacheFactory.getOrCreatePageCache(),
                NullLogProvider.getInstance() );
        stores = storeFactory.openNeoStores( true, eager );
    }

    @Override
    public void stop()
    {
        stores.close();
    }

    public NeoStores stores()
    {
        return stores;
    }
}
