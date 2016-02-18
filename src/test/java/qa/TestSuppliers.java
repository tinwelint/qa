package qa;

import org.junit.Test;

import java.util.function.Supplier;

public class TestSuppliers
{
    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        Supplier<Bla> supplier1 = () -> { return new Bla( "1" ); };
        Supplier<Bla> supplier2 = () -> new Bla( "2" );

        // WHEN
        supplier1.get();
        supplier2.get();
    }

    private static class Bla
    {
        public Bla( String name )
        {
            System.out.println( "construct Bla " + name );
        }
    }
}
