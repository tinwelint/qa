/**
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package versiondiff;

import java.io.File;
import java.util.Iterator;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.tooling.GlobalGraphOperations;

public class VersionDifferences
{
    /*
     * neoStores( GraphDatabaseService db )
     * scanAllRecords( RecordStore<RECORD> store, Visitor<RECORD,RuntimeException> visitor )
     * getAllRelationships( GraphDatabaseService db )
     * label( String name )
     * nonUniqueIndexConfiguration()
     * count( Iterator<?> iterator )
     * newDb( File storeDir )
     * newDbBuilder( File storeDir )
     * newDb( String storeDir )
     * newDbBuilder( String storeDir )
     */


//    // 3.0
//    public static NeoStores neoStores( GraphDatabaseService db )
//    {
//        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
//                .testAccessNeoStores();
//    }
//
//    public static <RECORD extends AbstractBaseRecord> void scanAllRecords( RecordStore<RECORD> store,
//            Visitor<RECORD,RuntimeException> visitor )
//    {
//        store.scanAllRecords( visitor );
//    }
//
//    public static Iterable<Relationship> getAllRelationships( GraphDatabaseService db )
//    {
//        return db.getAllRelationships();
//    }
//
//    public static Label label( String name )
//    {
//        return Label.label( name );
//    }
//
//    public static IndexConfiguration nonUniqueIndexConfiguration()
//    {
//        return IndexConfiguration.NON_UNIQUE;
//    }
//
//    public static long count( Iterator<?> iterator )
//    {
//        return Iterators.count( iterator );
//    }
//
//    public static GraphDatabaseService newDb( File storeDir )
//    {
//        return new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
//    }
//
//    public static GraphDatabaseBuilder newDbBuilder( File storeDir )
//    {
//        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
//    }
//
//    public static GraphDatabaseService newDb( String storeDir )
//    {
//        return newDb( new File( storeDir ) );
//    }
//
//    public static GraphDatabaseBuilder newDbBuilder( String storeDir )
//    {
//        return newDbBuilder( new File( storeDir ) );
//    }

//    // 2.3
//    public static NeoStores neoStores( GraphDatabaseService db )
//    {
//        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( NeoStores.class );
//    }
//
//    public static <RECORD extends AbstractBaseRecord> void scanAllRecords( RecordStore<RECORD> store,
//            Visitor<RECORD,RuntimeException> visitor )
//    {
//        long lowId = store.getNumberOfReservedLowIds();
//        long highId = store.getHighId();
//        for ( long id = lowId; id < highId; id++ )
//        {
//            try
//            {
//                visitor.visit( store.getRecord( id ) );
//            }
//            catch ( InvalidRecordException e )
//            {   // OK
//            }
//        }
//    }
//
//    public static Iterable<Relationship> getAllRelationships( GraphDatabaseService db )
//    {
//        return GlobalGraphOperations.at( db ).getAllRelationships();
//    }
//
//    public static Label label( String name )
//    {
//        return DynamicLabel.label( name );
//    }
//
//    public static IndexConfiguration nonUniqueIndexConfiguration()
//    {
//        return new IndexConfiguration( false );
//    }
//
//    public static long count( Iterator<?> iterator )
//    {
//        return IteratorUtil.count( iterator );
//    }
//
//    public static GraphDatabaseService newDb( File storeDir )
//    {
//        return new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
//    }
//
//    public static GraphDatabaseBuilder newDbBuilder( File storeDir )
//    {
//        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
//    }
//
//    public static GraphDatabaseService newDb( String storeDir )
//    {
//        return newDb( new File( storeDir ) );
//    }
//
//    public static GraphDatabaseBuilder newDbBuilder( String storeDir )
//    {
//        return newDbBuilder( new File( storeDir ) );
//    }

    // 2.2
    public static NeoStore neoStores( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( NeoStore.class );
    }

    public static <RECORD extends AbstractBaseRecord> void scanAllRecords( RecordStore<RECORD> store,
            Visitor<RECORD,RuntimeException> visitor )
    {
        long lowId = store.getNumberOfReservedLowIds();
        long highId = store.getHighId();
        for ( long id = lowId; id < highId; id++ )
        {
            try
            {
                visitor.visit( store.getRecord( id ) );
            }
            catch ( InvalidRecordException e )
            {   // OK
            }
        }
    }

    public static Iterable<Relationship> getAllRelationships( GraphDatabaseService db )
    {
        return GlobalGraphOperations.at( db ).getAllRelationships();
    }

    public static Label label( String name )
    {
        return DynamicLabel.label( name );
    }

    public static IndexConfiguration nonUniqueIndexConfiguration()
    {
        return new IndexConfiguration( false );
    }

    public static long count( Iterator<?> iterator )
    {
        return IteratorUtil.count( iterator );
    }

    public static GraphDatabaseAPI newDb( String storeDir )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
    }

    public static GraphDatabaseAPI newDb( File storeDir )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
    }

    public static GraphDatabaseBuilder newDbBuilder( String storeDir )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
    }

    public static GraphDatabaseBuilder newDbBuilder( File storeDir )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir.getAbsolutePath() );
    }
}
