package perf;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Performance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import static qa.perf.Operations.inTx;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class CoreApiBenchmark
{
    enum Labels implements Label
    {
        LABEL1, LABEL2, LABEL3, Root, A, B;
    }

    enum Types implements RelationshipType
    {
        TYPE1, HAS_A, HAS_B;
    }

    private static final Label[] LABELS = Labels.values();

    @Test
    public void nodeExists() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( inTx( ( on ) -> {
            Node[] nodes = new Node[1_000];
            for ( int i = 0; i < 1_000; i++ )
            {
                nodes[i] = on.db.createNode( LABELS );
            }
        } ) ).withOperations( inTx( ( on ) -> {
            for ( int i = 0; i < 1_000; i++ )
            {
                on.db.getNodeById( i );
            }
        } ) ).please();
    }

    @Test
    public void nodeHasLabel() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( inTx( ( on ) -> {
            Node[] nodes = new Node[1_000];
            for ( int i = 0; i < 1_000; i++ )
            {
                nodes[i] = on.db.createNode( LABELS );
            }
            on.state = nodes;
        } ) ).withOperations( inTx( ( on ) -> {
            Node[] nodes = (Node[]) on.state;
            for ( int i = 0; i < 1_000; i++ )
            {
                nodes[i].hasLabel( LABELS[i % LABELS.length] );
            }
        } ) ).please();
    }

    @Test
    public void nodeGetRelationships() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( inTx( ( on ) -> {
            Node node = on.db.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                node.createRelationshipTo( node, Types.TYPE1 );
            }
            on.state = node;
        } ) ).withOperations( inTx( ( on ) -> {
            Node node = (Node) on.state;
            count( node.getRelationships() );
        } ) ).please();
    }

    @Test
    public void nodeGetProperty() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( inTx( ( on ) -> {
            Node node = on.db.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "key-" + i, "value" );
            }
            on.state = node;
        } ) ).withOperations( inTx( ( on ) -> {
            Node node = (Node) on.state;
            for ( int i = 0; i < 10; i++ )
            {
                node.getProperty( "key-" + i );
            }
        } ) ).please();
    }

    @Test
    public void nodeHasProperty() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( inTx( ( on ) -> {
            Node node = on.db.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "key-" + i, "value" );
            }
            on.state = node;
        } ) ).withOperations( inTx( ( on ) -> {
            Node node = (Node) on.state;
            for ( int i = 0; i < 10; i++ )
            {
                node.hasProperty( "key-" + i );
            }
        } ) ).please();
    }

    @Test
    public void traverseSmallSubGraph() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() ).withInitialOperation( ( on ) -> {
            populate( on.db, 8, 2 );
        } ).withOperations( inTx( ( on ) -> {
            new TrueBNodesCounter( on.db ).count( true );
        } ) ).withBatchSize( 1 )
//        .withDuration( 240 )
        .please();
    }

    private static final int DEFAULT_DEPTH = 5;
    private static final int DEFAULT_FANOUT = 4;
    private static final int CHUNK_SIZE = 10_000;

    private Node populate( GraphDatabaseService db, int depth, int fanout )
    {
        // Don't create a new tree if one already exists
        try ( Transaction ignored = db.beginTx(); ResourceIterator<Node> roots = db.findNodes( Labels.Root ) )
        {
            if ( roots.hasNext() )
            {
                return roots.next();
            }
        }
        Random random = new Random();
        Node root = createRoot( db, random );
        Collection<Node> startNodes = Collections.singleton( root );
        for ( int i = 0; i < depth; i++ )
        {
            startNodes = createLevel( db, startNodes, fanout, random );
        }
        return root;
    }

    private Node createRoot( GraphDatabaseService db, Random random )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node root = createNodeWithValue( db, random, Labels.Root, Labels.A );
            tx.success();
            return root;
        }
    }

    private Collection<Node> createLevel( GraphDatabaseService db, Collection<Node> startNodes, int fanout,
            Random random )
    {
        Collection<Node> newStartNodes = new ArrayList<>();
        Iterator<Node> startNodeIterator = startNodes.iterator();
        while ( startNodeIterator.hasNext() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                newStartNodes.addAll( createLevelChunk( db, startNodeIterator, fanout, random ) );
                tx.success();
            }
        }
        return newStartNodes;
    }

    private Collection<Node> createLevelChunk( GraphDatabaseService db, Iterator<Node> startNodeIterator, int fanout,
            Random random )
    {
        Collection<Node> newStartNodes = new ArrayList<>();
        int createdNodes = 0;
        while ( createdNodes < CHUNK_SIZE && startNodeIterator.hasNext() )
        {
            Node startNode = startNodeIterator.next();
            for ( int i = 0; i < fanout; i++ )
            {
                Node bNode = createChildNode( db, startNode, Types.HAS_B, Labels.B, random );
                createdNodes++;
                for ( int j = 0; j < fanout; j++ )
                {
                    newStartNodes.add( createChildNode( db, bNode, Types.HAS_A, Labels.A, random ) );
                    createdNodes++;
                }
            }
        }
        return newStartNodes;
    }

    private Node createChildNode( GraphDatabaseService db, Node parentNode, RelationshipType relationshipType,
            Label label, Random random )
    {
        Node childNode = createNodeWithValue( db, random, label );
        parentNode.createRelationshipTo( childNode, relationshipType );
        return childNode;
    }

    private Node createNodeWithValue( GraphDatabaseService db, Random random, Label... labels )
    {
        Node node = db.createNode( labels );
        node.setProperty( "value", random.nextBoolean() );
        return node;
    }

    class TrueBNodesCounter
    {
        private final GraphDatabaseService graphDb;

        public TrueBNodesCounter( GraphDatabaseService graphDb )
        {
            this.graphDb = graphDb;
        }

        public int count( boolean depthFirst )
        {
            try ( ResourceIterator<Node> roots = graphDb.findNodes( Labels.Root ) )
            {
                if ( roots.hasNext() )
                {
                    return count( roots.next(), depthFirst );
                }
            }
            return -1;
        }

        private int count( Node root, boolean depthFirst )
        {
            TraversalDescription td = graphDb.traversalDescription().uniqueness( Uniqueness.NONE )
                    .evaluator( TrueBEvaluator.INSTANCE ).expand( CustomPathExpander.INSTANCE );
            if ( depthFirst )
            {
                td = td.depthFirst();
            }
            else
            {
                td = td.breadthFirst();
            }
            int count = 0;
            for ( Path ignored : td.traverse( root ) )
            {
                count++;
            }
            return count;
        }
    }

    private enum TrueBEvaluator implements Evaluator
    {
        INSTANCE;
        @Override
        public Evaluation evaluate( Path path )
        {
            Node endNode = path.endNode();
            return Evaluation.ofIncludes( includes( endNode ) );
        }

        private static boolean includes( Node endNode )
        {
            return endNode.hasLabel( Labels.B ) && (Boolean) endNode.getProperty( "value" );
        }

        @Override
        public String toString()
        {
            return "TrueBEvaluator()";
        }
    }

    private enum CustomPathExpander implements PathExpander<Object>
    {
        INSTANCE;
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Object> state )
        {
            Node endNode = path.endNode();
            if ( endNode.hasLabel( Labels.A ) )
            {
                return endNode.getRelationships( Direction.OUTGOING, Types.HAS_B );
            }
            else if ( endNode.hasLabel( Labels.B ) )
            {
                return endNode.getRelationships( Direction.OUTGOING, Types.HAS_A );
            }
            return Collections.emptyList();
        }

        @Override
        public PathExpander<Object> reverse()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "CustomPathExpander()";
        }
    }
}
