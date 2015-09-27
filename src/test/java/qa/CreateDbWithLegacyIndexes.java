package qa;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Factory;
import org.neo4j.function.IntFunction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.io.fs.FileUtils;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class CreateDbWithLegacyIndexes
{
    private static final RelationshipType type = DynamicRelationshipType.withName( "TYPE" );

    @Test
    public void shouldCreateStuff() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                cleared( new File( "db-with-legacy-indexes" ) ) );

        // TODO we can't have mixed values... i.e. f.ex a key in an index have some string values and some numeric values

        // WHEN
        IntFunction<String> keyFactory = basicKeyFactory();
        Factory<Node> nodes = createNodes( db );
        populateIndex( db, nodeIndex( db, "node-1", EXACT_CONFIG ), nodes, keyFactory, stringValues() );
        populateIndex( db, nodeIndex( db, "node-2", EXACT_CONFIG ), nodes, keyFactory, intValues() );
        populateIndex( db, nodeIndex( db, "node-3", FULLTEXT_CONFIG ), nodes, keyFactory, stringValues() );
        populateIndex( db, nodeIndex( db, "node-4", FULLTEXT_CONFIG ), nodes, keyFactory, longValues() );
        Factory<Relationship> relationships = createRelationships( db, type );
        populateIndex( db, relationshipIndex( db, "rel-1", EXACT_CONFIG ), relationships, keyFactory, stringValues() );
        populateIndex( db, relationshipIndex( db, "rel-2", EXACT_CONFIG ), relationships, keyFactory, floatValues() );
        populateIndex( db, relationshipIndex( db, "rel-3", FULLTEXT_CONFIG ), relationships, keyFactory, stringValues() );
        populateIndex( db, relationshipIndex( db, "rel-4", FULLTEXT_CONFIG ), relationships, keyFactory, doubleValues() );

        // THEN
        db.shutdown();
    }

    @Test
    public void shouldReadThatStuffAfterBeingMigrated() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( "db-with-legacy-indexes" ) );

        try
        {
            // THEN
            IntFunction<String> keyFactory = basicKeyFactory();
            Factory<Node> readNodes = readNodes( db );
            readIndex( db, nodeIndex( db, "node-1", EXACT_CONFIG ), readNodes, keyFactory, stringValues() );
            readIndex( db, nodeIndex( db, "node-2", EXACT_CONFIG ), readNodes, keyFactory, intValues() );
            readIndex( db, nodeIndex( db, "node-3", FULLTEXT_CONFIG ), readNodes, keyFactory, stringValues() );
            readIndex( db, nodeIndex( db, "node-4", FULLTEXT_CONFIG ), readNodes, keyFactory, longValues() );
            Factory<Relationship> relationships = readRelationships( db, type );
            readIndex( db, relationshipIndex( db, "rel-1", EXACT_CONFIG ), relationships, keyFactory, stringValues() );
            readIndex( db, relationshipIndex( db, "rel-2", EXACT_CONFIG ), relationships, keyFactory, floatValues() );
            readIndex( db, relationshipIndex( db, "rel-3", FULLTEXT_CONFIG ), relationships, keyFactory, stringValues() );
            readIndex( db, relationshipIndex( db, "rel-4", FULLTEXT_CONFIG ), relationships, keyFactory, doubleValues() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private File cleared( File file ) throws IOException
    {
        FileUtils.deleteRecursively( file );
        return file;
    }

    private IntFunction<Object> intValues()
    {
        return new IntFunction<Object>()
        {
            @Override
            public Object apply( int value )
            {
                return ValueContext.numeric( value );
            }
        };
    }

    private IntFunction<Object> longValues()
    {
        return new IntFunction<Object>()
        {
            @Override
            public Object apply( int value )
            {
                return ValueContext.numeric( (long) value );
            }
        };
    }

    private IntFunction<Object> floatValues()
    {
        return new IntFunction<Object>()
        {
            @Override
            public Object apply( int value )
            {
                return ValueContext.numeric( (float) value );
            }
        };
    }

    private IntFunction<Object> doubleValues()
    {
        return new IntFunction<Object>()
        {
            @Override
            public Object apply( int value )
            {
                return ValueContext.numeric( (double) value );
            }
        };
    }

    private IntFunction<Object> stringValues()
    {
        return new IntFunction<Object>()
        {
            @Override
            public Object apply( int value )
            {
                return "value balue " + value;
            }
        };
    }

    private Factory<Node> createNodes( final GraphDatabaseService db )
    {
        return new Factory<Node>()
        {
            @Override
            public Node newInstance()
            {
                return db.createNode();
            }
        };
    }

    private Factory<Node> readNodes( final GraphDatabaseService db )
    {
        return new Factory<Node>()
        {
            private long id;

            @Override
            public Node newInstance()
            {
                return db.getNodeById( id++ );
            }
        };
    }

    private Factory<Relationship> createRelationships( final GraphDatabaseService db, final RelationshipType type )
    {
        return new Factory<Relationship>()
        {
            @Override
            public Relationship newInstance()
            {
                Node node = db.createNode();
                return node.createRelationshipTo( node, type );
            }
        };
    }

    private Factory<Relationship> readRelationships( final GraphDatabaseService db, final RelationshipType type )
    {
        return new Factory<Relationship>()
        {
            private long id;

            @Override
            public Relationship newInstance()
            {
                return db.getRelationshipById( id++ );
            }
        };
    }

    private Index<Node> nodeIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( name, config );
            tx.success();
            return index;
        }
    }

    private RelationshipIndex relationshipIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex index = db.index().forRelationships( name, config );
            tx.success();
            return index;
        }
    }

    private <ENTITY extends PropertyContainer> void populateIndex( GraphDatabaseService db, Index<ENTITY> index,
            Factory<ENTITY> entityFactory, IntFunction<String> keyFactory, IntFunction<Object> valueFactory )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                ENTITY entity = entityFactory.newInstance();
                index.add( entity, keyFactory.apply( i ), valueFactory.apply( i ) );
            }
            tx.success();
        }
    }

    private <ENTITY extends PropertyContainer> void readIndex( GraphDatabaseService db, Index<ENTITY> index,
            Factory<ENTITY> entityFactory, IntFunction<String> keyFactory, IntFunction<Object> valueFactory )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                ENTITY entity = entityFactory.newInstance();
                String key = keyFactory.apply( i );
                Object value = valueFactory.apply( i );
//                System.out.println( key + ":" + value + " -> " + asList( (Iterator<ENTITY>)index.get( key, value ) ) );
                assertEquals( entity, single( (Iterator<ENTITY>) index.get( key, value ) ) );
            }
            tx.success();
        }
    }

    private IntFunction<String> basicKeyFactory()
    {
        return new IntFunction<String>()
        {
            @Override
            public String apply( int value )
            {
                return "key-" + (value%3);
            }
        };
    }
}
