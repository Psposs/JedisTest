package redis.clients.jedis.tests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.util.SafeEncoder;


public class JedisTest extends JedisCommandTestBase {
	private String localhost = "10.0.50.11";
  @Test
  public void useWithoutConnecting() {
    Jedis jedis = new Jedis(localhost,6379);
    jedis.auth("foobared");
    jedis.set("test", "ddd");// set 类似于 redis的set命令，以key value的形式向 redis里面存值
    System.out.println(jedis.get("test"));// 得到 key为test 的value
    System.out.println(jedis.dbSize());//jedis.dbSize()  dbsize 命令,查看当前库数据量
    jedis.flushAll();
    jedis.append("test", "dddd");
  }

@Test
  public void checkBinaryData() {
    byte[] bigdata = new byte[1777];
    for (int b = 0; b < bigdata.length; b++) {
      bigdata[b] = (byte) ((byte) b % 255);
    }
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("data", SafeEncoder.encode(bigdata));

    String status = jedis.hmset("foo", hash);//同时将多个field-value (域-值) 对设置到哈希表key 中。
    assertEquals("OK", status);
    assertEquals(hash, jedis.hgetAll("foo"));// jedis.hgetAll("foo") 返回哈希表key 中，所有的域和值。
  }


/**
 * @author Psposs
 * @date 2015-12-15 下午10:17:44
 * @return void
 */
@Test
  public void connectWithShardInfo() {
    JedisShardInfo shardInfo = new JedisShardInfo(localhost, Protocol.DEFAULT_PORT);
    shardInfo.setPassword("foobared");
    Jedis jedis = new Jedis(shardInfo);
    jedis.set("foo", "value");
    System.out.println(jedis.get("foo"));
  }

  @Test(expected = JedisConnectionException.class) // expected = JedisConnectionException.class 表示抛出JedisConnectionException异常时测试成功
  public void timeoutConnection() throws Exception {
    jedis = new Jedis(localhost, 6379, 15000);
    jedis.auth("foobared");
    jedis.configSet("timeout", "1");
    Thread.sleep(2000);
    jedis.hmget("foobar", "foo");
  }

  @Test(expected = JedisConnectionException.class)
  public void timeoutConnectionWithURI() throws Exception {
    jedis = new Jedis(new URI("redis://:foobared@"+localhost+":6380/2"), 15000);
    jedis.configSet("timeout", "1");
    Thread.sleep(2000);
    jedis.hmget("foobar", "foo");
  }

  @Test(expected = JedisDataException.class)
  public void failWhenSendingNullValues() {
    jedis.set("foo", null);
  }

  @Test(expected = InvalidURIException.class)
  public void shouldThrowInvalidURIExceptionForInvalidURI() throws URISyntaxException {
    Jedis j = new Jedis(new URI(localhost+":6380"));
    j.ping();
  }

  @Test
  public void shouldReconnectToSameDB() throws IOException {
    jedis.select(1);
    jedis.set("foo", "bar");
    jedis.getClient().getSocket().shutdownInput();
    jedis.getClient().getSocket().shutdownOutput();
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void startWithUrlString() {
    Jedis j = new Jedis(localhost, 6379);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    Jedis jedis = new Jedis("redis://:foobared@localhost:6380/2");
    assertEquals("PONG", jedis.ping()); 
    
    //  jedis.ping() 使用客户端向Redis 服务器发送一个PING ，如果服务器运作正常的话，会返回一个PONG 。 通常用于测试与服务器的连接是否仍然生效，或者用于测量延迟值
    
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void startWithUrl() throws URISyntaxException {
    Jedis j = new Jedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    Jedis jedis = new Jedis(new URI("redis://:foobared@localhost:6380/2"));
    assertEquals("PONG", jedis.ping());
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void shouldNotUpdateDbIndexIfSelectFails() throws URISyntaxException {
    int currentDb = jedis.getDB();
    try {
      int invalidDb = -1;
      jedis.select(invalidDb);

      fail("Should throw an exception if tried to select invalid db");
    } catch (JedisException e) {
      assertEquals(currentDb, jedis.getDB());
    }
  }

  @Test
  public void allowUrlWithNoDBAndNoPassword() {
    Jedis jedis = new Jedis("redis://localhost:6380");
    jedis.auth("foobared");
    assertEquals(jedis.getClient().getHost(), "localhost");
    assertEquals(jedis.getClient().getPort(), 6380);
    assertEquals(jedis.getDB(), 0);

    jedis = new Jedis("redis://localhost:6380/");
    jedis.auth("foobared");
    assertEquals(jedis.getClient().getHost(), "localhost");
    assertEquals(jedis.getClient().getPort(), 6380);
    assertEquals(jedis.getDB(), 0);
  }

  @Test
  public void checkCloseable() {
    jedis.close();
    BinaryJedis bj = new BinaryJedis("localhost");
    bj.connect();
    bj.close();
  }

  @Test
  public void checkDisconnectOnQuit() {
    jedis.quit();
    assertFalse(jedis.getClient().isConnected());
  }

}