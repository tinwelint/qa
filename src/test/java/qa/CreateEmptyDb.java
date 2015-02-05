package qa;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class CreateEmptyDb
{
    @Test
    public void should() throws Exception
    {
        new GraphDatabaseFactory().newEmbeddedDatabase( clean( "empty-db" ) ).shutdown();
    }

    private String clean( String file ) throws IOException
    {
        FileUtils.deleteRecursively( new File( file ) );
        return file;
    }
}
