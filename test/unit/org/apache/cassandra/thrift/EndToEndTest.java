package org.apache.cassandra.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

public class EndToEndTest extends SchemaLoader
{
  
  private static CassandraServer server;
    @BeforeClass
    public static void setup() throws IOException, InvalidRequestException, TException{
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));        
        server = new CassandraServer();
        server.set_keyspace("Keyspace1");
    }
    
    @Test
    public void test_delete_with_predicate() throws Exception
    {
      byte [] superc = "imsuper".getBytes();
        ColumnParent cp = new ColumnParent();
        cp.setSuper_column(superc);
        cp.setColumn_family("Super1");
        ByteBuffer key = ByteBuffer.wrap("rangedeletekey".getBytes());
        for (long a = 0; a < 7; a++){
          Column c1 = new Column();
          c1.setName(ByteBufferUtil.bytes(a));
          c1.setValue(new byte [0]);
          c1.setTimestamp(System.nanoTime());
          server.insert(key, cp, c1, ConsistencyLevel.ONE);
        }
        Deletion d1 = new Deletion();
        SlicePredicate delPred = new SlicePredicate();
        d1.setSuper_column(superc);
        SliceRange  delSr = new SliceRange();
        delSr.setStart(ByteBufferUtil.bytes(3l));
        delSr.setFinish(ByteBufferUtil.bytes(5l));
        delPred.setSlice_range(delSr);
        d1.setPredicate(delPred);
        d1.setTimestamp(System.nanoTime());
        Mutation m1 = new Mutation();
        m1.setDeletion(d1);
        
        Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map = new HashMap<ByteBuffer, Map<String, List<Mutation>>> ();
        Map<String, List<Mutation>> mutationsForCf = new HashMap<>();
        mutationsForCf.put("Super1", (List<Mutation>) Arrays.asList(m1));
        mutation_map.put(key, mutationsForCf);
        server.batch_mutate(mutation_map, ConsistencyLevel.ONE); 
        
        SlicePredicate finder = new SlicePredicate();
        SliceRange rangeFinder = new SliceRange();
        rangeFinder.setCount(100);
        rangeFinder.setStart(ByteBufferUtil.bytes(0l));
        rangeFinder.setFinish(ByteBufferUtil.bytes(8l));
        finder.setSlice_range(rangeFinder);
        List<ColumnOrSuperColumn> results = server.get_slice(key, cp, finder, ConsistencyLevel.ONE);
        Assert.assertEquals(4, results.size());
        Assert.assertEquals(0L, ByteBufferUtil.toLong( results.get(0).column.name));
        Assert.assertEquals(1L, ByteBufferUtil.toLong( results.get(1).column.name));
        Assert.assertEquals(2L, ByteBufferUtil.toLong( results.get(2).column.name));
        Assert.assertEquals(6L, ByteBufferUtil.toLong( results.get(3).column.name));
        
    }

    @Test
    public void test_delete() throws Exception
    {
      ColumnParent cp = new ColumnParent();
      cp.setColumn_family("Standard1");
      ByteBuffer key = ByteBuffer.wrap("rangedeletekey".getBytes());
      for (char a='a'; a < 'g'; a++){
        Column c1 = new Column();
        c1.setName((a+"").getBytes());
        c1.setValue(new byte [0]);
        c1.setTimestamp(System.nanoTime());
        server.insert(key, cp, c1, ConsistencyLevel.ONE);
      }
      Deletion d1 = new Deletion();
      SlicePredicate delPred = new SlicePredicate();
      SliceRange  delSr = new SliceRange();
      delSr.setStart( "d".getBytes());
      delSr.setFinish( "e".getBytes());
      delPred.setSlice_range(delSr);
      d1.setPredicate(delPred);
      d1.setTimestamp(System.nanoTime());
      Mutation m1 = new Mutation();
      m1.setDeletion(d1);
      
      Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map = new HashMap<ByteBuffer, Map<String, List<Mutation>>> ();
      Map<String, List<Mutation>> mutationsForCf = new HashMap<>();
      mutationsForCf.put("Standard1", (List<Mutation>) Arrays.asList(m1));
      mutation_map.put(key, mutationsForCf);
      server.batch_mutate(mutation_map, ConsistencyLevel.ONE); 
      
      SlicePredicate finder = new SlicePredicate();
      SliceRange rangeFinder = new SliceRange();
      rangeFinder.setCount(100);
      rangeFinder.setStart("a".getBytes());
      rangeFinder.setFinish("h".getBytes());
      finder.setSlice_range(rangeFinder);
      List<ColumnOrSuperColumn> results = server.get_slice(key, cp, finder, ConsistencyLevel.ONE);
      Assert.assertEquals( 4, results.size() );
      Assert.assertEquals("a", ByteBufferUtil.string( results.get(0).column.name ) );
      Assert.assertEquals("b", ByteBufferUtil.string( results.get(1).column.name ) );
      Assert.assertEquals("c", ByteBufferUtil.string( results.get(2).column.name ) );
      Assert.assertEquals("f", ByteBufferUtil.string( results.get(3).column.name ) );
      
    }

    
}
