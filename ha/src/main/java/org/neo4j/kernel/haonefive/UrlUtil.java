package org.neo4j.kernel.haonefive;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlUtil
{
    private UrlUtil()
    {
    }
    
    public static URL toUrl( String host, int port )
    {
        try
        {
            return new URL( "http", host, port, "" );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( e );
        }
    }
}
