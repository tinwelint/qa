/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Args;

public class CommandReactor
{
    private final Map<String, Action> actions = new HashMap<>();
    private final Reactor reactor;
    private final PrintStream log;
    private final Action helpAction = new Action()
    {
        @Override
        public void run( Args action ) throws Exception
        {
            log.println( "Listing available commands:" );
            for ( Map.Entry<String, Action> entry : actions.entrySet() )
            {
                log.println( "  " + entry.getKey() + ": " + entry.getValue() );
            }
        }
    };

    public CommandReactor( String name, PrintStream log )
    {
        this.reactor = new Reactor( name );
        this.log = log;
    }

    public void add( String command, Action action )
    {
        this.actions.put( command, action );
    }

    public void shutdown()
    {
        reactor.halted = true;
    }

    private class Reactor extends Thread
    {
        private volatile boolean halted;
        private final File commandFile = new File( "command" );

        public Reactor( String name )
        {
            super( name + " - terminal command reactor" );
            start();
        }

        @Override
        public void run()
        {
            while ( !halted )
            {
                try
                {
                    String commandLine = getAvailableCommand();
                    if ( commandLine != null )
                    {
                        String command = extractCommand( commandLine );
                        Action action = getAction( command );
                        if ( action != null )
                        {
                            action.run( Args.parse( commandLine.split( " " ) ) );
                        }
                    }
                    else
                    {
                        Thread.sleep( 200 );
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }

        private Action getAction( String command )
        {
            if ( command.equals( "?" ) )
            {
                return helpAction;
            }
            return actions.get( command );
        }

        private String extractCommand( String commandLine )
        {
            int index = commandLine.indexOf( ' ' );
            return index == -1 ? commandLine : commandLine.substring( 0, index );
        }

        private String getAvailableCommand() throws IOException
        {
            if ( System.in.available() > 0 )
            {
                String line = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
                if ( line != null )
                {
                    return line;
                }
            }
            if ( commandFile.exists() )
            {
                BufferedReader reader = new BufferedReader( new FileReader( commandFile ) );
                String line = reader.readLine();
                reader.close();
                commandFile.delete();
                if ( line != null )
                {
                    return line;
                }
            }
            return null;
        }
    }

    public boolean isShutdown()
    {
        return !reactor.isAlive();
    }

    public void waitFor() throws InterruptedException
    {
        reactor.join();
    }

    public interface Action
    {
        void run( Args action ) throws Exception;
    }

    public Action shutdownAction()
    {
        return new Action()
        {
            @Override
            public void run( Args action ) throws Exception
            {
                shutdown();
            }
        };
    }
}
