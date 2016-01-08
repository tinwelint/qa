package qa;

import org.junit.Test;

import java.io.File;
import java.util.Properties;

import org.neo4j.neo_workbench.data_bench.store_generation.Store;
import org.neo4j.neo_workbench.data_bench.store_generation.csv.OutputFormat;

public class GenerateSocialNetworkData
{
    @Test
    public void shouldGenerate() throws Exception
    {
        Store store = new Store( org.neo4j.neo_workbench.data_bench.workloads.social.extended.SocialNetworkGenerator.class,
                new File( "C:\\Users\\Matilas\\Desktop\\social-data" ), OutputFormat.ImportTool );
        Properties properties = new Properties();
        properties.setProperty( "PARTICIPANTS", String.valueOf( 20_000_000 ) );
        store.create( properties );
    }
}