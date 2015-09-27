package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Race;

public class ZD2489
{
    public final @Rule DatabaseRule dbr = new EmbeddedDatabaseRule();

    @Test
    public void shouldSeeWhereTheExceptionComesFrom() throws Throwable
    {
        while ( true )
        {
            // Set up
            dbr.execute( "CREATE (n:YO)" );

            // Try to trigger
            Race race = new Race();
            race.addContestant( execute( "MATCH (n:YO) SET n.property = 10" ) );
            race.addContestant( execute( "MATCH (n:YO) DELETE n" ) );
            race.go();
        }
    }

    private Runnable execute( final String query )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                dbr.execute( query );
            }
        };
    }
}
