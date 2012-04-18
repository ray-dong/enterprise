package org.neo4j.kernel.haonefive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TargetDirectory;

public class TestCreateNode
{
    private TargetDirectory PATH;
    
    @Before
    public void before() throws Exception
    {
        PATH = TargetDirectory.forTest( getClass() );
    }

    @After
    public void after() throws Exception
    {

    }
    
    @Test
    public void createHighlyAvailableNode() throws Exception
    {
        HaOneFiveGraphDb db1 = new HaOneFiveGraphDb( PATH.directory( "0", true ).getAbsolutePath(),
                MapUtil.stringMap( "ha.server_id", "0" ) );
        HaOneFiveGraphDb db2 = new HaOneFiveGraphDb( PATH.directory( "1", true ).getAbsolutePath(),
                MapUtil.stringMap( "ha.server_id", "1" ) );
        
        Transaction tx = db1.beginTx();
        db1.createNode();
        tx.success();
        tx.finish();
    }
}
