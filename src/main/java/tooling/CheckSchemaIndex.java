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
                    label = Label.label( statement.readOperations().labelGetName( descriptor.getLabelId() ) );
                    key = statement.readOperations().propertyKeyGetName( descriptor.getPropertyKeyId() );
                }

                int count = 0;
                int incorrectNode = 0;
                int incorrectLabel = 0;
                int incorrectKey = 0;
                Map<String,AtomicInteger> incorrectLabelCounts = new HashMap<>();
                Map<String,AtomicInteger> incorrectKeyCounts = new HashMap<>();
                try ( IndexReader reader = proxy.newReader() )
                {
                    PrimitiveLongIterator nodeIds = reader.scan();
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
