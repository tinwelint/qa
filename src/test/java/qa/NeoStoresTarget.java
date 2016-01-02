/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package qa;

import qa.perf.Target;

import java.io.File;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
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
        stores = storeFactory.openNeoStores( true, StoreType.values() );
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
