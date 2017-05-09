package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class StartStopTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );

    @Test
    public void shouldTest() throws Exception
    {
        GraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        while ( true )
        {
            factory.newEmbeddedDatabase( directory.directory() ).shutdown();
        }
    }
}
