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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class VersionDifferences
{
    /* META / APPLICABLE TO ALL */
    public static GraphDatabaseService newDb( File storeDir )
    {
        return newDb( new GraphDatabaseFactory(), storeDir );
    }

    public static GraphDatabaseBuilder newDbBuilder( File storeDir )
    {
        return newDbBuilder( new GraphDatabaseFactory(), storeDir );
    }

    public static GraphDatabaseService newDb( String storeDir )
    {
        return newDb( new File( storeDir ) );
    }

    public static GraphDatabaseBuilder newDbBuilder( String storeDir )
    {
        return newDbBuilder( new File( storeDir ) );
    }

    public static GraphDatabaseService newDb( GraphDatabaseFactory factory, String storeDir )
    {
        return newDb( factory, new File( storeDir ) );
    }

    public static GraphDatabaseBuilder newDbBuilder( GraphDatabaseFactory factory, String storeDir )
    {
        return newDbBuilder( factory, new File( storeDir ) );
    }


    /*
     * neoStores( GraphDatabaseService db )
     * scanAllRecords( RecordStore<RECORD> store, Visitor<RECORD,RuntimeException> visitor )
     * getAllNodes( GraphDatabaseService db )
     * getAllRelationships( GraphDatabaseService db )
     * label( String name )
     * nonUniqueIndexConfiguration()
     * count( Iterator<?> iterator )
     * newDb( File storeDir )
     * newDbBuilder( File storeDir )
     */


    // 3.1
    public static NeoStores neoStores( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores();
    }

    public static <RECORD extends AbstractBaseRecord> void scanAllRecords( RecordStore<RECORD> store,
            Visitor<RECORD,RuntimeException> visitor )
    {
        store.scanAllRecords( visitor );
    }

    public static Iterable<Node> getAllNodes( GraphDatabaseService db )
    {
        return db.getAllNodes();
    }

    public static Iterable<Relationship> getAllRelationships( GraphDatabaseService db )
    {
        return db.getAllRelationships();
    }

    public static Label label( String name )
    {
        return Label.label( name );
    }

    public static IndexDescriptor.Type nonUniqueIndexConfiguration()
    {
        return IndexDescriptor.Type.GENERAL;
    }

    public static long count( Iterator<?> iterator )
    {
        return Iterators.count( iterator );
    }

    public static GraphDatabaseService newDb( GraphDatabaseFactory factory, File storeDir )
    {
        return factory.newEmbeddedDatabase( storeDir );
    }

    public static GraphDatabaseBuilder newDbBuilder( GraphDatabaseFactory factory, File storeDir )
    {
        return factory.newEmbeddedDatabaseBuilder( storeDir );
    }

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
//    public static Iterable<Node> getAllNodes( GraphDatabaseService db )
//    {
//        return db.getAllNodes();
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
//    public static GraphDatabaseService newDb( GraphDatabaseFactory factory, File storeDir )
//    {
//        return factory.newEmbeddedDatabase( storeDir );
//    }
//
//    public static GraphDatabaseBuilder newDbBuilder( GraphDatabaseFactory factory, File storeDir )
//    {
//        return factory.newEmbeddedDatabaseBuilder( storeDir );
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
//    // 2.2
//    public static NeoStore neoStores( GraphDatabaseService db )
//    {
//        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( NeoStore.class );
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
//    public static GraphDatabaseAPI newDb( String storeDir )
//    {
//        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
//    }
//
//    public static GraphDatabaseAPI newDb( File storeDir )
//    {
//        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
//    }
}
