package qa;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.neo_workbench.command_bench.model.results.NullResultHandler;
import org.neo4j.neo_workbench.command_bench.workloads.WorkloadRunner;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.CsvWriter;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.CsvWriterEventHandler;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.NodeCsvFile;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.OutputFormat;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.RelationshipCsvFile;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.ResettableNodeBuilder;
import org.neo4j.neo_workbench.data_bench.store_generation.model.AttributeDefinition;
import org.neo4j.neo_workbench.data_bench.store_generation.model.Attributes;
import org.neo4j.neo_workbench.data_bench.store_generation.model.GraphElementDefinitionsBuilder;
import org.neo4j.neo_workbench.data_bench.store_generation.model.NodeBuilder;
import org.neo4j.neo_workbench.data_bench.store_generation.model.NodeDefinitions;
import org.neo4j.neo_workbench.data_bench.store_generation.model.NodeFactory;
import org.neo4j.neo_workbench.data_bench.store_generation.model.RelationshipBuilder;
import org.neo4j.neo_workbench.data_bench.store_generation.model.RelationshipDefinitions;
import org.neo4j.neo_workbench.data_bench.store_generation.model.RelationshipFactory;
import org.neo4j.neo_workbench.data_bench.store_generation.model.StoreGenerator;
import org.neo4j.neo_workbench.data_bench.store_generation.model.StoreWriter;
import org.neo4j.neo_workbench.data_bench.workloads.social.extended.SocialNetworkGenerator;

import static java.lang.Math.min;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Configuration.DEFAULT;
import static org.neo4j.csv.reader.Readables.wrap;

public class RelationshipStageTest
{
//    @Rule
//    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
//
//    @Test
//    public void shouldEncodeAllTheProperties() throws Exception
//    {
//        // GIVEN
//        Configuration config = DEFAULT;
//        BatchingNeoStores store = new BatchingNeoStores( new DefaultFileSystemAbstraction(), directory.directory(),
//                config, NullLogService.getInstance(), EMPTY );
//        IdMapper idMapper = new EncodingIdMapper( AUTO, new LongEncoder(), Radix.LONG, NO_MONITOR );
//        populateIdMapper( idMapper );
//        InputIterable<InputRelationship> input = relationshipInput();
//        NodeRelationshipCache nodeRelationshipCache = new NodeRelationshipCache( AUTO, 50 );
//        RelationshipStage stage = new RelationshipStage( config, new IoMonitor( store.getIoTracer() ),
//                input, idMapper, store, nodeRelationshipCache, false, EntityStoreUpdaterStep.NO_MONITOR );
//
//        // WHEN
//        superviseDynamicExecution( defaultVisible(), config, stage );
//    }
//
//    private InputIterable<InputRelationship> relationshipInput()
//    {
//        return null;
//    }
//
//    private void populateIdMapper( IdMapper idMapper )
//    {
//    }

    @Test
    public void shouldParseTheSocialNetworkDataCorrectly() throws Exception
    {
        // GIVEN
        OutputFormat outputFormat = OutputFormat.ImportTool;
        StoreGenerator generator = new SocialNetworkGenerator();
        GraphElementDefinitionsBuilder<NodeDefinitions> nodeDefinitionsBuilder =
                GraphElementDefinitionsBuilder.forNodes( outputFormat.defaultNodeAttributes() );
        GraphElementDefinitionsBuilder<RelationshipDefinitions> relationshipDefinitionsBuilder =
                GraphElementDefinitionsBuilder.forRelationships( outputFormat.defaultRelationshipAttributes() );

        generator.addNodeDefinitions( nodeDefinitionsBuilder );
        generator.addRelationshipDefinitions( relationshipDefinitionsBuilder );

        ParseDirectlyStoreWriter writer = new ParseDirectlyStoreWriter(
                nodeDefinitionsBuilder.build(),
                relationshipDefinitionsBuilder.build(),
                outputFormat );
        Properties properties = new Properties();
        properties.setProperty( "PARTICIPANTS", String.valueOf( 100_000 ) );
        WorkloadRunner runner = generator.newGenerator( writer, properties, NullResultHandler.Instance );

        // WHEN
        runner.start().await();
    }

    public static class ParseDirectlyStoreWriter implements StoreWriter
    {
        private final Mark mark = new Mark();
        private final CharSeeker seeker;
        private final Extractors extractors;
        private final AppendableReader intermediary = new AppendableReader();
        private final Map<String,RelationshipCsvFile> relationshipCsvFiles = new HashMap<>();
        private final Map<String,NodeCsvFile> nodeCsvFiles = new HashMap<>();
        private final Map<String,Attributes> nodeStuff;
        private final Map<String,Attributes> relStuff;

        private int lookForPropertyIndex = -1;
        private boolean lookForIt;

        public ParseDirectlyStoreWriter( Map<String,Attributes> nodeStuff, Map<String,Attributes> relStuff,
                OutputFormat outputFormat )
        {
            this.nodeStuff = nodeStuff;
            this.relStuff = relStuff;
            final int delimiter = outputFormat.delimiter().charAt( 0 );
            CsvWriter csvWriter = new CsvWriter( intermediary.asWriter(), outputFormat,
                    new CsvWriterEventHandler()
                    {
                        @Override
                        public void onWriteLine( int counter ) throws IOException
                        {
                            if ( counter % 1_000_000 == 0 )
                            {
                                System.out.println( counter );
                            }

                            if ( lookForIt )
                            {
                                boolean foundIt = false;
                                for ( int i = 0; seeker.seek( mark, delimiter ); i++ )
                                {
                                    if ( i == lookForPropertyIndex )
                                    {
                                        seeker.extract( mark, extractors.long_() );
                                        foundIt = true;
                                    }
                                }
                                if ( !foundIt )
                                {
                                    System.out.println( "Line didn't have 'since', at col " + lookForPropertyIndex );
                                }
                            }
                            else
                            {
                                intermediary.clear();
                            }
                        }
                    } );
            for ( Map.Entry<String, Attributes> entry : nodeStuff.entrySet() )
            {
                nodeCsvFiles.put( entry.getKey(), new NodeCsvFile( entry.getValue(), csvWriter ) );
            }
            for ( Map.Entry<String, Attributes> entry : relStuff.entrySet() )
            {
                relationshipCsvFiles.put( entry.getKey(), new RelationshipCsvFile( entry.getValue(), csvWriter ) );
                if ( entry.getKey().equals( "friendships" ) )
                {
                    lookForPropertyIndex = findAttributeIndex( entry.getValue(), "since" );
                }
            }
            this.extractors = new Extractors( outputFormat.arrayDelimiter().charAt( 0 ) );
            this.seeker = charSeeker( wrap( intermediary ), DEFAULT, false );
        }

        @Override
        public NodeFactory forNodes( String name )
        {
//            System.out.println( "forNodes:" + name );
            lookForIt = false;
            final NodeCsvFile csvFile = nodeCsvFiles.get( name );
            return new NodeFactory()
            {
                @Override
                public NodeBuilder.Labels newNode()
                {
                    return newNode( null );
                }

                @Override
                public NodeBuilder.Labels newNode( Object id )
                {
                    ResettableNodeBuilder line = csvFile.newLine();
                    if ( id != null )
                    {
                        line.addProperty( ResettableNodeBuilder.Id.name(), id );
                    }
                    return line;
                }

                @Override
                public void close() throws Exception
                {
                }
            };
        }

        @Override
        public RelationshipFactory forRelationships( String name )
        {
//            System.out.println( "forRelationships:" + name );
            lookForIt = name.equals( "friendships" );
            final RelationshipCsvFile csvFile = relationshipCsvFiles.get( name );
            return new RelationshipFactory()
            {
                @Override
                public RelationshipBuilder.Type newRelationship()
                {
                    return csvFile.newLine();
                }

                @Override
                public void close() throws Exception
                {
                }
            };
        }

        private int findAttributeIndex( Attributes attributes, String name )
        {
            int i = 0;
            for ( AttributeDefinition definition : attributes.definitions() )
            {
                if ( name.equals( definition.name() ) )
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        @Override
        public void close() throws Exception
        {
            seeker.close();
        }
    }

    public static class AppendableReader extends Reader
    {
        private final char[] ahead = new char[1024*1024];
        private int readCursor, writeCursor;

        @Override
        public int read( char[] cbuf, int off, int len ) throws IOException
        {
            int actual = min( available(), len );
            for ( int i = 0; i < actual; i++ )
            {
                cbuf[off + i] = ahead[readCursor++];
            }
            return actual;
        }

        private int available()
        {
            return writeCursor - readCursor;
        }

        @Override
        public void close() throws IOException
        {
        }

        public void clear()
        {
            readCursor = writeCursor = 0;
        }

        public Writer asWriter()
        {
            return new Writer()
            {
                @Override
                public void write( char[] cbuf, int off, int len ) throws IOException
                {
                    // Wasteful as heck
                    theAppend( cbuf, off, len );
                }

                @Override
                public void flush() throws IOException
                {
                }

                @Override
                public void close() throws IOException
                {
                }
            };
        }

        public void theAppend( char[] cbuf, int off, int len )
        {
            pack();

            try
            {
                System.arraycopy( cbuf, 0, ahead, writeCursor, len );
                writeCursor += len;
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {
                throw new RuntimeException( e );
            }
        }

        private void pack()
        {
            if ( readCursor > 0 )
            {
                int available = available();
                for ( int i = 0; i < available; i++ )
                {
                    ahead[i] = ahead[readCursor+i];
                }
                writeCursor -= readCursor;
                readCursor = 0;
            }
        }
    }
}
