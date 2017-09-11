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
package tooling;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.schema.IndexReader;

public class CheckSchemaIndex
{
    public static void main( String[] args ) throws Exception
    {
        File storeDir = new File( args[0] );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            IndexingService indexing = db.getDependencyResolver().resolveDependency( IndexingService.class );
            Map<String,AtomicInteger> totalIncorrectLabelCounts = new HashMap<>();
            for ( long indexId = 1; indexId < 4400; indexId++ )
            {
                IndexProxy proxy;
                try
                {
                    proxy = indexing.getIndexProxy( indexId );
                }
                catch ( IndexNotFoundKernelException e )
                {
                    System.out.println( "No index " + indexId );
                    continue;
                }

                IndexDescriptor descriptor = proxy.getDescriptor();
                Label label;
                String key;
                try ( Statement statement =
                        db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                                .getKernelTransactionBoundToThisThread( true ).acquireStatement() )
                {
                    label = Label.label( statement.readOperations().labelGetName( descriptor.schema().getLabelId() ) );
                    key = statement.readOperations().propertyKeyGetName( descriptor.schema().getPropertyId() );
                }

                int count = 0;
                int incorrectNode = 0;
                int incorrectLabel = 0;
                int incorrectKey = 0;
                Map<String,AtomicInteger> incorrectLabelCounts = new HashMap<>();
                Map<String,AtomicInteger> incorrectKeyCounts = new HashMap<>();
                try ( IndexReader reader = proxy.newReader() )
                {
                    PrimitiveLongIterator nodeIds = reader.query( IndexQuery.exists( descriptor.schema().getPropertyId() ) );
                    while ( nodeIds.hasNext() )
                    {
                        long nodeId = nodeIds.next();
                        count++;
                        Node node;
                        try
                        {
                            node = db.getNodeById( nodeId );
                        }
                        catch ( NotFoundException e )
                        {
                            incorrectNode++;
                            continue;
                        }

                        if ( !node.hasLabel( label ) )
                        {
                            incorrectLabel++;
                            for ( Label existingLabel : node.getLabels() )
                            {
                                incorrectLabelCounts.computeIfAbsent( existingLabel.name(), l -> new AtomicInteger() )
                                        .incrementAndGet();
                                totalIncorrectLabelCounts.computeIfAbsent( existingLabel.name(), l -> new AtomicInteger() )
                                        .incrementAndGet();
                            }
                        }
                        if ( !node.hasProperty( key ) )
                        {
                            incorrectKey++;
                            for ( String existingKey : node.getPropertyKeys() )
                            {
                                incorrectKeyCounts.computeIfAbsent( existingKey, k -> new AtomicInteger() ).incrementAndGet();
                            }
                        }
                    }
                }

                System.out.print( "[" + indexId + "] " );
                if ( count == 0 )
                {
                    System.out.println( "EMPTY" );
                }
                else if ( incorrectKey != 0 || incorrectLabel != 0 || incorrectNode != 0 )
                {
                    System.out.println( "count:" + count + ", incorrectLabel:" + incorrectLabel
                            + ", incorrectKey:" + incorrectKey + ", incorrectNode:" + incorrectNode );
                    if ( !incorrectLabelCounts.isEmpty() )
                    {
                        System.out.println( "Existing labels" );
                        System.out.println( incorrectLabelCounts );
                    }
                    if ( !incorrectKeyCounts.isEmpty() )
                    {
                        System.out.println( "Existing keys" );
                        System.out.println( incorrectKeyCounts );
                    }
                }
                else
                {
                    System.out.println( "CLEAN" );
                }
            }

            if ( !totalIncorrectLabelCounts.isEmpty() )
            {
                System.out.println( "TOTAL Existing labels" );
                System.out.println( totalIncorrectLabelCounts );
            }
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            throw t;
        }
        finally
        {
            db.shutdown();
        }
    }
}
