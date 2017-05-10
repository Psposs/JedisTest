package redis.clients.jedis;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.commands.*;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.set.SetParams;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Slowlog;

public class Jedis extends BinaryJedis implements JedisCommands, MultiKeyCommands,
    AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands {

  protected JedisPoolAbstract dataSource = null;

  /**
 * @date 2015-12-16 上午10:54:39
 */
public Jedis() {
    super();
  }

  /**
 * @date 2015-12-16 上午10:54:41
 * @param host
 */
public Jedis(final String host) {
    super(host);
  }

  /**
 * @date 2015-12-16 上午10:54:36
 * @param host
 * @param port
 */
public Jedis(final String host, final int port) {
    super(host, port);
  }


/**
 * 设置redis host port timeout
 * @date 2015-12-15 下午10:18:21
 * @param host
 * @param port
 * @param timeout
 */
public Jedis(final String host, final int port, final int timeout) {
    super(host, port, timeout);
  }

  /**
 * @date 2015-12-16 上午10:54:27
 * @param host
 * @param port
 * @param connectionTimeout
 * @param soTimeout
 */
public Jedis(final String host, final int port, final int connectionTimeout, final int soTimeout) {
    super(host, port, connectionTimeout, soTimeout);
  }

  /**
 * @date 2015-12-16 上午10:54:23
 * @param shardInfo
 */
public Jedis(JedisShardInfo shardInfo) {
    super(shardInfo);
  }

  /**
 * @date 2015-12-16 上午10:54:20
 * @param uri
 */
public Jedis(URI uri) {
    super(uri);
  }

  /**
 * @date 2015-12-16 上午10:54:18
 * @param uri
 * @param timeout
 */
public Jedis(final URI uri, final int timeout) {
    super(uri, timeout);
  }

  /**
 * @date 2015-12-16 上午10:54:14
 * @param uri
 * @param connectionTimeout
 * @param soTimeout
 */
public Jedis(final URI uri, final int connectionTimeout, final int soTimeout) {
    super(uri, connectionTimeout, soTimeout);
  }

  /**
   * Set the string value as value of the key. The string can't be longer than 1073741824 bytes (1
   * GB).
   * 设置一个字符串作为 key 的值，但是字符串的大小不能超过1073741824 bytes（1GB）
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param value
   * @return Status code reply
   */
  @Override
  public String set(final String key, String value) {
    checkIsInMultiOrPipeline();
    client.set(key, value);
    return client.getStatusCodeReply();
  }

  /**
   * SET key value [EX seconds] [PX milliseconds] [NX|XX]将字符串值value 关联到key 。如果key 已经持有其他值，
   * SET 就覆写旧值，无视类型。对于某个原本带有生存时间（TTL）的键来说，当SET 命令成功在这个键上执行时，这个键原有的TTL 将被清除。
   * 从Redis 2.6.12 版本开始，SET 命令的行为可以通过一系列参数来修改： EX second ：设置键的过期时间为second 秒。
   * SET key value EX second 效果等同于SETEX key second value 。• PX millisecond ：设置键的过期时间为millisecond 毫秒。
   * SET key value PX millisecond 效果等同于PSETEX key millisecond value 。
   * NX ：只在键不存在时，才对键进行设置操作。SET key value NX 效果等同于SETNX key value 。 XX ：只在键已经存在时，才对键进行设置操作。
   * Set the string value as value of the key. The string can't be longer than 1073741824 bytes (1GB).
   * 设置一个字符串作为 key 的值，但是字符串的大小不能超过1073741824 bytes（1GB）
   * Note: 因为SET 命令可以通过参数来实现和SETNX 、SETEX 和PSETEX 三个命令的效果，所以将来的Redis 版本可能会废弃并最终移除SETNX 、SETEX 和PSETEX 这三个命令
   * @param key
   * @param value
   * @param params NX|XX, NX -- Only set the key if it does not already exist. XX -- Only set the
   *          key if it already exist. EX|PX, expire time units: EX = seconds; PX = milliseconds
   * NX|XX, NX --有此参数时只能 set 不存在的key,如果给已经存在的key set 值则不生效， XX -- 此参数只能设置已经存在的key 的值，不存在的不生效
   * EX|PX, key 的存在时间: EX = seconds; PX = milliseconds
   * @return Status code reply
   */
  //设置过期时间,手机验证码等功能常用
  public String set(final String key, final String value, final SetParams params) {
    checkIsInMultiOrPipeline();// 判断 客户端连接是否正常
    client.set(key, value, params);
    return client.getStatusCodeReply();
  }

  /**
   * Get the value of the specified key. If the key does not exist null is returned. If the value
   * stored at key is not a string an error is returned because GET can only handle string values.
   * 获取指定key的值。如果键不存在返回null,如果该key 对应的value 不是一个字符串，则返回一个错误，因为只能处理字符串值
   * <p>
   * Time complexity: O(1)
   * @param key
   * @return Bulk reply
   */
  @Override
  public String get(final String key) {
    checkIsInMultiOrPipeline();
    client.sendCommand(Protocol.Command.GET, key);
    return client.getBulkReply();
  }

  /**
   * Test if the specified key exists. The command returns the number of keys existed Time
   * complexity: O(N) 时间复杂度： O(N)
   * 检查给定key 是否存在。 返回值： 若key 存在，返回1 ，否则返回0 。
   * @param keys
   * @return Integer reply, specifically: an integer greater than 0 if one or more keys were removed
   *         0 if none of the specified key existed
   */
  public Long exists(final String... keys) {
    checkIsInMultiOrPipeline();
    client.exists(keys);
    return client.getIntegerReply();
  }

  /**
   * Test if the specified key exists. The command returns "1" if the key exists, otherwise "0" is
   * returned. Note that even keys set with an empty string as value will return "1". Time
   * complexity: O(1)
   *  检查 单个 key 值是否存在   返回值： 如果key 值存在 返回 true ,如果不存在 在返回false
   * @param key
   * @return Boolean reply, true if the key exists, otherwise false
   */
  @Override
  public Boolean exists(final String key) {
    checkIsInMultiOrPipeline();
    client.exists(key);
    return client.getIntegerReply() == 1;
  }

  /**
   * Remove the specified keys. If a given key does not exist no operation is performed for this
   * key. The command returns the number of keys removed. Time complexity: O(1)
   * 删除给定的一个或多个key 。不存在的key 会被忽略
   * @param keys
   * @return Integer reply, specifically: an integer greater than 0 if one or more keys were removed
   *         0 if none of the specified key existed
   *         返回删除key 的数量
   */
  @Override
  public Long del(final String... keys) {
    checkIsInMultiOrPipeline();
    client.del(keys);
    return client.getIntegerReply();
  }

  @Override
  public Long del(String key) {
    client.del(key);
    return client.getIntegerReply();
  }

  /**
   * Return the type of the value stored at key in form of a string. The type can be one of "none",
   * "string", "list", "set". "none" is returned if the key does not exist. Time complexity: O(1)
   * 返回 存储的类型以字符串的形式，返回类型可以是  "none","string", "list", "set"等， "none" 表示key不存在
   * @param key
   * @return Status code reply, specifically: "none" if the key does not exist "string" if the key
   *         contains a String value "list" if the key contains a List value "set" if the key
   *         contains a Set value "zset" if the key contains a Sorted Set value "hash" if the key
   *         contains a Hash value
   */
  @Override
  public String type(final String key) {
    checkIsInMultiOrPipeline();
    client.type(key);
    return client.getStatusCodeReply();
  }

  /**
   * 根据 正则表达式返回所有的key 值
   * 如： foo foobar 则用命令 keys foo* 则可以返回 foo foobar
   * Returns all the keys matching the glob-style pattern as space separated strings. For example if
   * you have in the database the keys "foo" and "foobar" the command "KEYS foo*" will return
   * "foo foobar".
   * <p>
   * Note that while the time complexity for this operation is O(n) the constant times are pretty
   * low. For example Redis running on an entry level laptop can scan a 1 million keys database in
   * 40 milliseconds. <b>Still it's better to consider this one of the slow commands that may ruin
   * the DB performance if not used with care.</b>
   * <p>
   * In other words this command is intended only for debugging and special operations like creating
   * a script to change the DB schema. Don't use it in your normal code. Use Redis Sets in order to
   * group together a subset of objects.
   * <p>
   * Glob style patterns examples:
   * <ul>
   * <li>h?llo will match hello hallo hhllo
   * <li>h*llo will match hllo heeeello
   * <li>h[ae]llo will match hello and hallo, but not hillo
   * </ul>
   * <p>
   * Use \ to escape special chars if you want to match them verbatim.
   * <p>
   * Time complexity: O(n) (with n being the number of keys in the DB, and assuming keys and pattern
   * of limited length)
   * @param pattern
   * @return Multi bulk reply
   */
  @Override
  public Set<String> keys(final String pattern) {
    checkIsInMultiOrPipeline();
    client.keys(pattern);
    return BuilderFactory.STRING_SET.build(client.getBinaryMultiBulkReply());
  }

  /**
   * Return a randomly selected key from the currently selected DB.
   * 从当前数据库中随机返回(不删除) 一个key 。
   * <p>
   * Time complexity: O(1)
   * @return Singe line reply, specifically the randomly selected key or an empty string is the
   *         database is empty
   */
  @Override
  public String randomKey() {
    checkIsInMultiOrPipeline();
    client.randomKey();
    return client.getBulkReply();
  }

  /**
   *将key 改名为newkey 。
   *当key 和newkey 相同，或者key 不存在时，返回一个错误。
   *当newkey 已经存在时，RENAME 命令将覆盖旧值。
   * Atomically renames the key oldkey to newkey. If the source and destination name are the same an
   * error is returned. If newkey already exists it is overwritten.
   * <p>
   * Time complexity: O(1)
   * @param oldkey
   * @param newkey
   * @return Status code repy
   */
  @Override
  public String rename(final String oldkey, final String newkey) {
    checkIsInMultiOrPipeline();
    client.rename(oldkey, newkey);
    return client.getStatusCodeReply();
  }

  /**
   * 当且仅当newkey 不存在时，将key 改名为newkey 。当key 不存在时，返回一个错误。  
   * Rename oldkey into newkey but fails if the destination key newkey already exists.
   * <p>
   * Time complexity: O(1)
   * @param oldkey
   * @param newkey
   * @return Integer reply, specifically: 1 if the key was renamed 0 if the target key already exist
   */
  @Override
  public Long renamenx(final String oldkey, final String newkey) {
    checkIsInMultiOrPipeline();
    client.renamenx(oldkey, newkey);
    return client.getIntegerReply();
  }

  /**
   * 为给定key 设置生存时间，当key 过期时(生存时间为0 )，它会被自动删除。在Redis 中，带有生存时间的key 被称为『易失的』(volatile)。
   *生存时间可以通过使用DEL 命令来删除整个key 来移除，或者被SET 和GETSET 命令覆写(overwrite)，这意味着，如果一个命令只是修改(alter) 一个带生存时间的key 的值而不是用一个新的key 值来代替
   *(replace) 它的话，那么生存时间不会被改变。 比如说，对一个key 执行INCR 命令，对一个列表进行LPUSH 命令，或者对一个哈希表执行HSET 命令，
   *这类操作都不会修改key 本身的生存时间。 另一方面，如果使用RENAME 对一个key 进行改名，那么改名后的key 的生存时间和改名前一样。
   *RENAME 命令的另一种可能是，尝试将一个带生存时间的key 改名成另一个带生存时间的another_key
   *，这时旧的another_key (以及它的生存时间) 会被删除，然后旧的key 会改名为another_key ，因此，新
   * 的another_key 的生存时间也和原本的key 一样。 使用PERSIST 命令可以在不删除key 的情况下，移除key 的生存时间，让key 重新成为一个『持久的』
   * (persistent) key 。 更新生存时间 可以对一个已经带有生存时间的key 执行EXPIRE 命令，新指定的生存时间会取代旧的生存时间。
   * 过期时间的精确度 在Redis 2.4 版本中，过期时间的延迟在1 秒钟之内——也即是，就算key 已经过期，但它还是可能在过期
   * 之后一秒钟之内被访问到，而在新的Redis 2.6 版本中，延迟被降低到1 毫秒之内。 Redis 2.1.3 之前的不同之处
   * 在Redis 2.1.3 之前的版本中，修改一个带有生存时间的key 会导致整个key 被删除，这一行为是受当时复
   * 制(replication) 层的限制而作出的，现在这一限制已经被修复。
   * Set a timeout on the specified key. After the timeout the key will be automatically deleted by
   * the server. A key with an associated timeout is said to be volatile in Redis terminology.
   * <p>
   * Voltile keys are stored on disk like the other keys, the timeout is persistent too like all the
   * other aspects of the dataset. Saving a dataset containing expires and stopping the server does
   * not stop the flow of time as Redis stores on disk the time when the key will no longer be
   * available as Unix time, and not the remaining seconds.
   * <p>
   * Since Redis 2.1.3 you can update the value of the timeout of a key already having an expire
   * set. It is also possible to undo the expire at all turning the key into a normal key using the
   * {@link #persist(String) PERSIST} command.
   * <p>
   * Time complexity: O(1)
   * @see <a href="http://redis.io/commands/expire">Expire Command</a>
   * @param key
   * @param seconds
   * @return Integer reply, specifically: 1: the timeout was set. 0: the timeout was not set since
   *         the key already has an associated timeout (this may happen only in Redis versions &lt;
   *         2.1.3, Redis &gt;= 2.1.3 will happily update the timeout), or the key does not exist.
   */
  @Override
  public Long expire(final String key, final int seconds) {
    checkIsInMultiOrPipeline();
    client.expire(key, seconds);
    return client.getIntegerReply();
  }

  /**
   * EXPIREAT 的作用和EXPIRE 类似，都用于为key 设置生存时间。不同在于EXPIREAT 命令接受的时间参数是UNIX 时间戳(unix timestamp)。
   * EXPIREAT works exctly like {@link #expire(String, int) EXPIRE} but instead to get the number of
   * seconds representing the Time To Live of the key as a second argument (that is a relative way
   * of specifing the TTL), it takes an absolute one in the form of a UNIX timestamp (Number of
   * seconds elapsed since 1 Gen 1970).
   * <p>
   * EXPIREAT was introduced in order to implement the Append Only File persistence mode so that
   * EXPIRE commands are automatically translated into EXPIREAT commands for the append only file.
   * Of course EXPIREAT can also used by programmers that need a way to simply specify that a given
   * key should expire at a given time in the future.
   * <p>
   * Since Redis 2.1.3 you can update the value of the timeout of a key already having an expire
   * set. It is also possible to undo the expire at all turning the key into a normal key using the
   * {@link #persist(String) PERSIST} command.
   * <p>
   * Time complexity: O(1)
   * @see <a href="http://redis.io/commands/expire">Expire Command</a>
   * @param key
   * @param unixTime
   * @return Integer reply, specifically: 1: the timeout was set. 0: the timeout was not set since
   *         the key already has an associated timeout (this may happen only in Redis versions &lt;
   *         2.1.3, Redis &gt;= 2.1.3 will happily update the timeout), or the key does not exist.
   */
  @Override
  public Long expireAt(final String key, final long unixTime) {
    checkIsInMultiOrPipeline();
    client.expireAt(key, unixTime);
    return client.getIntegerReply();
  }

  /**
   * 查看剩余生存时间，如果没有 返回-1 如果key 值不存在返回 -2
   * The TTL command returns the remaining time to live in seconds of a key that has an
   * {@link #expire(String, int) EXPIRE} set. This introspection capability allows a Redis client to
   * check how many seconds a given key will continue to be part of the dataset.
   * @param key
   * @return Integer reply, returns the remaining time to live in seconds of a key that has an
   *         EXPIRE. In Redis 2.6 or older, if the Key does not exists or does not have an
   *         associated expire, -1 is returned. In Redis 2.8 or newer, if the Key does not have an
   *         associated expire, -1 is returned or if the Key does not exists, -2 is returned.
   */
  @Override
  public Long ttl(final String key) {
    checkIsInMultiOrPipeline();
    client.ttl(key);
    return client.getIntegerReply();
  }

  /**
   * 从当前 DB 中移动 key 到指定的 DB 中。
   * 如果当前数据库(源数据库) 和给定数据库(目标数据库) 有相同名字的给定key ，或者key 不存在于当前数据库，那么MOVE 没有任何效果。
   * 因此，也可以利用这一特性，将MOVE 当作锁(locking) 原语(primitive)。
   * Move the specified key from the currently selected DB to the specified destination DB. Note
   * that this command returns 1 only if the key was successfully moved, and 0 if the target key was
   * already there or if the source key was not found at all, so it is possible to use MOVE as a
   * locking primitive.
   * @param key
   * @param dbIndex
   * @return Integer reply, specifically: 1 if the key was moved 0 if the key was not moved because
   *         already present on the target DB or was not found in the current DB.
   */
  @Override
  public Long move(final String key, final int dbIndex) {
    checkIsInMultiOrPipeline();
    client.move(key, dbIndex);
    return client.getIntegerReply();
  }

  /**
   * 返回旧值  set 新值
   * GETSET is an atomic set this value and return the old value command. Set key to the string
   * value and return the old value stored at key. The string can't be longer than 1073741824 bytes
   * (1 GB).
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param value
   * @return Bulk reply
   */
  @Override
  public String getSet(final String key, final String value) {
    checkIsInMultiOrPipeline();
    client.getSet(key, value);
    return client.getBulkReply();
  }

  /**
   * 返回所有(一个或多个) 给定key 的值。 如果给定的key 里面，有某个key 不存在或者 value 非String，那么这个key 返回特殊值null 。因此，该命令永不失败
   * Get the values of all the specified keys. If one or more keys dont exist or is not of type
   * String, a 'nil' value is returned instead of the value of the specified key, but the operation
   * never fails.
   * <p>
   * Time complexity: O(1) for every key
   * @param keys
   * @return Multi bulk reply
   */
  @Override
  public List<String> mget(final String... keys) {
    checkIsInMultiOrPipeline();
    client.mget(keys);
    return client.getMultiBulkReply();
  }

  /**
   * Note: 因为SET 命令可以通过参数来实现和SETNX 、SETEX 和PSETEX 三个命令的效果，所以将来的Redis 版本可能会废弃并最终移除SETNX 、SETEX 和PSETEX 这三个命令
   * 将key 的值设为value ，当且仅当key 不存在。若给定的key 已经存在，则SETNX 不做任何动作。 SETNX 是『SET if Not eXists』(如果不存在，则SET) 的简写。
   * SETNX works exactly like {@link #set(String, String) SET} with the only difference that if the
   * key already exists no operation is performed. SETNX actually means "SET if Not eXists".
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param value
   * @return Integer reply, specifically: 1 if the key was set 0 if the key was not set
   * 返回值：设置成功，返回1 。 设置失败，返回0 。
   */
  @Override
  public Long setnx(final String key, final String value) {
    checkIsInMultiOrPipeline();
    client.setnx(key, value);
    return client.getIntegerReply();
  }

  /**
   * Note: 因为SET 命令可以通过参数来实现和SETNX 、SETEX 和PSETEX 三个命令的效果，所以将来的Redis 版本可能会废弃并最终移除SETNX 、SETEX 和PSETEX 这三个命令
   * set 值 并设置 生存时间 类似于  set 命令 和 EXPIRE 命令一块执行
   * The command is exactly equivalent to the following group of commands:
   * {@link #set(String, String) SET} + {@link #expire(String, int) EXPIRE}. The operation is
   * atomic.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param seconds
   * @param value
   * @return Status code reply
   */
  @Override
  public String setex(final String key, final int seconds, final String value) {
    checkIsInMultiOrPipeline();
    client.setex(key, seconds, value);
    return client.getStatusCodeReply();
  }

  /**
   *同时设置一个或多个key-value 对。
   *如果某个给定key 已经存在，那么MSET 会用新值覆盖原来的旧值，如果这不是你所希望的效果，请考虑
   *使用 {@link #msetnx(String...) MSETNX} 命令：它只会在所有给定key 都不存在的情况下进行设置操作。
   *MSET 是一个原子性(atomic) 操作，所有给定key 都会在同一时间内被设置，某些给定key 被更新而另一
   *些给定key 没有改变的情况，不可能发生。
   *可用版本： >= 1.0.1
   *时间复杂度： O(N)，N 为要设置的key 数量。
   *返回值： 总是返回OK (因为MSET 不可能失败)
   *redis> MSET date "2012.3.30" time "11:00 a.m."
   * Set the the respective keys to the respective values. MSET will replace old values with new
   * values, while {@link #msetnx(String...) MSETNX} will not perform any operation at all even if
   * just a single key already exists.
   * <p>
   * Because of this semantic MSETNX can be used in order to set different keys representing
   * different fields of an unique logic object in a way that ensures that either all the fields or
   * none at all are set.
   * <p>
   * Both MSET and MSETNX are atomic operations. This means that for instance if the keys A and B
   * are modified, another client talking to Redis can either see the changes to both A and B at
   * once, or no modification at all.
   * @see #msetnx(String...)
   * @param keysvalues
   * @return Status code reply Basically +OK as MSET can't fail
   */
  @Override
  public String mset(final String... keysvalues) {
    checkIsInMultiOrPipeline();
    client.mset(keysvalues);
    return client.getStatusCodeReply();
  }

  /**
   * 同时设置一个或多个key-value 对，当且仅当所有给定key 都不存在。
   *即使只有一个给定key 已存在，MSETNX 也会拒绝执行所有给定key 的设置操作。
   *MSETNX 是原子性的，因此它可以用作设置多个不同key 表示不同字段(field) 的唯一性逻辑对象(unique
   *logic object)，所有字段要么全被设置，要么全不被设置。
   *可用版本： >= 1.0.1
   *时间复杂度： O(N)，N 为要设置的key 的数量。
   *返回值：当所有key 都成功设置，返回1 。如果所有给定key 都设置失败(至少有一个key 已经存在)，那么返回0 。
   * Set the the respective keys to the respective values. {@link #mset(String...) MSET} will
   * replace old values with new values, while MSETNX will not perform any operation at all even if
   * just a single key already exists.
   * <p>
   * Because of this semantic MSETNX can be used in order to set different keys representing
   * different fields of an unique logic object in a way that ensures that either all the fields or
   * none at all are set.
   * <p>
   * Both MSET and MSETNX are atomic operations. This means that for instance if the keys A and B
   * are modified, another client talking to Redis can either see the changes to both A and B at
   * once, or no modification at all.
   * @see #mset(String...)
   * @param keysvalues
   * @return Integer reply, specifically: 1 if the all the keys were set 0 if no key was set (at
   *         least one key already existed)
   */
  @Override
  public Long msetnx(final String... keysvalues) {
    checkIsInMultiOrPipeline();
    client.msetnx(keysvalues);
    return client.getIntegerReply();
  }

  /**
   * 将key 所储存的值减去减量decrement 。如果key 不存在，那么key 的值会先被初始化为0 ，然后再执行DECRBY 操作。
   *如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。本操作的值限制在64 位(bit) 有符号数字表示之内。
   *关于更多递增(increment) / 递减(decrement) 操作的更多信息，请参见INCR 命令。
   *可用版本： >= 1.0.0
   *时间复杂度： O(1)
   * IDECRBY work just like {@link #decr(String) INCR} but instead to decrement by 1 the decrement
   * is integer.
   * <p>
   * INCR commands are limited to 64 bit signed integers.
   * <p>
   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
   * and then converted back as a string.
   * <p>
   * Time complexity: O(1)
   * @see #incr(String)
   * @see #decr(String)
   * @see #incrBy(String, long)
   * @param key
   * @param integer
   * @return Integer reply, this commands will reply with the new value of key after the increment.
   * 返回值： 减去decrement 之后，key 的值。
   */
  @Override
  public Long decrBy(final String key, final long integer) {
    checkIsInMultiOrPipeline();
    client.decrBy(key, integer);
    return client.getIntegerReply();
  }

  /**
   * 将key 中储存的数字值减一。如果key 不存在，那么key 的值会先被初始化为0 ，然后再执行DECR 操作。
   * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
   * 本操作的值限制在64 位(bit) 有符号数字表示之内。
   * key 不存在的情况  先将 key 初始化为 0 在减去1
   * Decrement the number stored at key by one. If the key does not exist or contains a value of a
   * wrong type, set the key to the value of "0" before to perform the decrement operation.
   * <p>
   * INCR commands are limited to 64 bit signed integers.
   * <p>
   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
   * and then converted back as a string.
   * <p>
   * Time complexity: O(1)
   * @see #incr(String)
   * @see #incrBy(String, long)
   * @see #decrBy(String, long)
   * @param key
   * @return Integer reply, this commands will reply with the new value of key after the increment.
   * 返回值： 执行DECR 命令之后key 的值。
   */
  @Override
  public Long decr(final String key) {
    checkIsInMultiOrPipeline();
    client.decr(key);
    return client.getIntegerReply();
  }

  /**
   * 将key 所储存的值加上增量increment 。如果key 不存在，那么key 的值会先被初始化为0 ，然后再执行INCRBY 命令。如果值包含错误的类型，
   * 或字符串类型的值不能表示为数字，那么返回一个错误。本操作的值限制在64 位(bit) 有符号数字表示之内。关于递增(increment) / 递减(decrement) 操作的更多信息，参见INCR 命令。
   * INCRBY work just like {@link #incr(String) INCR} but instead to increment by 1 the increment is
   * integer.
   * <p>
   * INCR commands are limited to 64 bit signed integers.
   * <p>
   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
   * and then converted back as a string.
   * <p>
   * Time complexity: O(1)
   * @see #incr(String)
   * @see #decr(String)
   * @see #decrBy(String, long)
   * @param key
   * @param integer
   * @return Integer reply, this commands will reply with the new value of key after the increment.
   * 可用版本： >= 1.0.0
   * 时间复杂度： O(1)
   * 返回值： 加上increment 之后，key 的值。
   */
  @Override
  public Long incrBy(final String key, final long integer) {
    checkIsInMultiOrPipeline();
    client.incrBy(key, integer);
    return client.getIntegerReply();
  }

  /**
   * 精度是double 
   * INCRBYFLOAT
   * <p>
   * INCRBYFLOAT commands are limited to double precision floating point values.
   * <p>
   * Note: this is actually a string operation, that is, in Redis there are not "double" types.
   * Simply the string stored at the key is parsed as a base double precision floating point value,
   * incremented, and then converted back as a string. There is no DECRYBYFLOAT but providing a
   * negative value will work as expected.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param value
   * @return Double reply, this commands will reply with the new value of key after the increment.
   * 返回值精度 也是double
   */
  @Override
  public Double incrByFloat(final String key, final double value) {
    checkIsInMultiOrPipeline();
    client.incrByFloat(key, value);
    String dval = client.getBulkReply();
    return (dval != null ? new Double(dval) : null);
  }

  /**
   * 将key 中储存的数字值增一。如果key 不存在，那么key 的值会先被初始化为0 ，然后再执行INCR 操作。如果值包含错误的类型，
   * 或字符串类型的值不能表示为数字，那么返回一个错误。本操作的值限制在64 位(bit) 有符号数字表示之内。
   * Increment the number stored at key by one. If the key does not exist or contains a value of a
   * wrong type, set the key to the value of "0" before to perform the increment operation.
   * <p>
   * INCR commands are limited to 64 bit signed integers.
   * <p>
   * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
   * Note: 这是一个针对字符串的操作，因为Redis 没有专用的整数类型，所以key 内储存的字符串被解释为十进制64 位有符号整数来执行INCR 操作。
   * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
   * and then converted back as a string.
   * <p>
   * Time complexity: O(1)
   * @see #incrBy(String, long)
   * @see #decr(String)
   * @see #decrBy(String, long)
   * @param key
   * @return Integer reply, this commands will reply with the new value of key after the increment.
   * 返回值： 执行INCR 命令之后key 的值。
   */
  @Override
  public Long incr(final String key) {
    checkIsInMultiOrPipeline();
    client.incr(key);
    return client.getIntegerReply();
  }

  /**
   * 如果key 已经存在并且是一个字符串，APPEND 命令将value 追加到key 原来的值的末尾。如果key 不存在，
   * APPEND 就简单地将给定key 设为value ，就像执行SET key value 一样。
   * If the key already exists and is a string, this command appends the provided value at the end
   * of the string. If the key does not exist it is created and set as an empty string, so APPEND
   * will be very similar to SET in this special case.
   * <p>
   * Time complexity: O(1). The amortized time complexity is O(1) assuming the appended value is
   * small and the already present value is of any size, since the dynamic string library used by
   * Redis will double the free space available on every reallocation.
   * @param key
   * @param value
   * @return Integer reply, specifically the total length of the string after the append operation.
   * 可用版本： >= 2.0.0
   * 时间复杂度： 平摊O(1)
   * 返回值： 追加value 之后，key 中字符串的长度。
   */
  @Override
  public Long append(final String key, final String value) {
    checkIsInMultiOrPipeline();
    client.append(key, value);
    return client.getIntegerReply();
  }

  /**
   * 返回key 中字符串值的子字符串，字符串的截取范围由start 和end 两个偏移量决定(包括start 和end在内)。
   * 负数偏移量表示从字符串最后开始计数，-1 表示最后一个字符，-2 表示倒数第二个，以此类推。
   * substr(在>= 2.4 的版本里，GETRANGE作用类似于SUBSTR。) 通过保证子字符串的值域(range) 不超过实际字符串的值域来处理超出范围的值域请求。
   * Return a subset of the string from offset start to offset end (both offsets are inclusive).
   * Negative offsets can be used in order to provide an offset starting from the end of the string.
   * So -1 means the last char, -2 the penultimate and so forth.
   * <p>
   * The function handles out of range requests without raising an error, but just limiting the
   * resulting range to the actual length of the string.
   * <p>
   * Time complexity: O(start+n) (with start being the start index and n the total length of the
   * requested range). Note that the lookup part of this command is O(1) so for small strings this
   * is actually an O(1) command.
   * @param key
   * @param start
   * @param end
   * @return Bulk reply
   * 返回值： 截取得出的子字符串。
   */
  @Override
  public String substr(final String key, final int start, final int end) {
    checkIsInMultiOrPipeline();
    client.substr(key, start, end);
    return client.getBulkReply();
  }

  /**
   * 将哈希表key 中的域field 的值设为value 。如果key 不存在，一个新的哈希表被创建并进行HSET 操作。如果域field 已经存在于哈希表中，旧值将被覆盖。
   * Set the specified hash field to the specified value.
   * <p>
   * If key does not exist, a new key holding a hash is created.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param field
   * @param value
   * @return If the field already exists, and the HSET just produced an update of the value, 0 is
   *         returned, otherwise if a new field is created 1 is returned.
   * 返回值：如果field 是哈希表中的一个新建域，并且值设置成功，返回1 。如果哈希表中域field 已经存在且旧值已被新值覆盖，返回0 。
   */
  @Override
  public Long hset(final String key, final String field, final String value) {
    checkIsInMultiOrPipeline();
    client.hset(key, field, value);
    return client.getIntegerReply();
  }

  /**
   * 返回哈希表key 中给定域field 的值。
   * If key holds a hash, retrieve the value associated to the specified field.
   * <p>
   * If the field is not found or the key does not exist, a special 'nil' value is returned.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param field
   * @return Bulk reply
   * 返回值：给定域的值。当给定域不存在或是给定key 不存在时，返回nil 。
   */
  @Override
  public String hget(final String key, final String field) {
    checkIsInMultiOrPipeline();
    client.hget(key, field);
    return client.getBulkReply();
  }

  /**
   * 将哈希表key 中的域field 的值设置为value ，当且仅当域field 不存在。若域field 已经存在，该操作无效。如果key 不存在，一个新哈希表被创建并执行HSETNX 命令。
   * Set the specified hash field to the specified value if the field not exists. <b>Time
   * complexity:</b> O(1)
   * @param key
   * @param field
   * @param value
   * @return If the field already exists, 0 is returned, otherwise if a new field is created 1 is
   *         returned.
   *         返回值：设置成功，返回1 。如果给定域已经存在且没有操作被执行，返回0 。
   */
  @Override
  public Long hsetnx(final String key, final String field, final String value) {
    checkIsInMultiOrPipeline();
    client.hsetnx(key, field, value);
    return client.getIntegerReply();
  }

  /**
   * 同时将多个field-value (域-值) 对设置到哈希表key 中。 此命令会覆盖哈希表中已存在的域。如果key 不存在，一个空哈希表被创建并执行HMSET 操作。
   * Set the respective fields to the respective values. HMSET replaces old values with new values.
   * <p>
   * If key does not exist, a new key holding a hash is created.
   * <p>
   * <b>Time complexity:</b> O(N) (with N being the number of fields)
   * @param key
   * @param hash
   * @return Return OK or Exception if hash is empty
   * 如果命令执行成功，返回OK 。当key 不是哈希表(hash) 类型时，返回一个错误。
   */
  @Override
  public String hmset(final String key, final Map<String, String> hash) {
    checkIsInMultiOrPipeline();
    client.hmset(key, hash);
    return client.getStatusCodeReply();
  }

  /**
   * 返回哈希表key 中，一个或多个给定域的值。如果给定的域不存在于哈希表，那么返回一个nil 值。
   * 因为不存在的key 被当作一个空哈希表来处理，所以对一个不存在的key 进行HMGET 操作将返回一个只带有null 值的表。
   * Retrieve the values associated to the specified fields.
   * <p>
   * If some of the specified fields do not exist, nil values are returned. Non existing keys are
   * considered like empty hashes.
   * <p>
   * <b>Time complexity:</b> O(N) (with N being the number of fields)
   * @param key
   * @param fields
   * @return Multi Bulk Reply specifically a list of all the values associated with the specified
   *         fields, in the same order of the request.
   * 返回值： 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样。
   */
  @Override
  public List<String> hmget(final String key, final String... fields) {
    checkIsInMultiOrPipeline();
    client.hmget(key, fields);
    return client.getMultiBulkReply();
  }

  /**
   * 为哈希表key 中的域field 的值加上增量increment 。增量也可以为负数，相当于对给定域进行减法操作。如果key 不存在，
   * 一个新的哈希表被创建并执行HINCRBY 命令。如果域field 不存在，那么在执行命令前，域的值被初始化为0 。
   * 对一个储存字符串值的域field 执行HINCRBY 命令将造成一个错误。本操作的值被限制在64 位(bit) 有符号数字表示之内。
   * 可用版本： >= 2.0.0
   * 时间复杂度： O(1)
   * 返回值： 执行HINCRBY 命令之后，哈希表key 中域field 的值
   * Increment the number stored at field in the hash at key by value. If key does not exist, a new
   * key holding a hash is created. If field does not exist or holds a string, the value is set to 0
   * before applying the operation. Since the value argument is signed you can use this command to
   * perform both increments and decrements.
   * <p>
   * The range of values supported by HINCRBY is limited to 64 bit signed integers.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param field
   * @param value
   * @return Integer reply The new value at field after the increment operation.
   */
  @Override
  public Long hincrBy(final String key, final String field, final long value) {
    checkIsInMultiOrPipeline();
    client.hincrBy(key, field, value);
    return client.getIntegerReply();
  }

  /**
   * 为哈希表key 中的域field 加上浮点数增量increment 。
   * 如果哈希表中没有域field ，那么HINCRBYFLOAT 会先将域field 的值设为0 ，然后再执行加法操作。
   * 如果键key 不存在，那么HINCRBYFLOAT 会先创建一个哈希表，再创建域field ，最后再执行加法操作。当以下任意一个条件发生时，返回一个错误：
   * 域field 的值不是字符串类型(因为redis 中的数字和浮点数都以字符串的形式保存，所以它们都属于字符串类型）
   * 域field 当前的值或给定的增量increment 不能解释(parse) 为双精度浮点数(double precisionfloating point number)
   * HINCRBYFLOAT 命令的详细功能和INCRBYFLOAT 命令类似，请查看INCRBYFLOAT 命令获取更多相关信息。
   * 可用版本： >= 2.6.0
   * 时间复杂度： O(1)
   * 返回值： 执行加法操作之后field 域的值。
   * Increment the number stored at field in the hash at key by a double precision floating point
   * value. If key does not exist, a new key holding a hash is created. If field does not exist or
   * holds a string, the value is set to 0 before applying the operation. Since the value argument
   * is signed you can use this command to perform both increments and decrements.
   * <p>
   * The range of values supported by HINCRBYFLOAT is limited to double precision floating point
   * values.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param field
   * @param value
   * @return Double precision floating point reply The new value at field after the increment
   *         operation.
   */
  @Override
  public Double hincrByFloat(final String key, final String field, final double value) {
    checkIsInMultiOrPipeline();
    client.hincrByFloat(key, field, value);
    final String dval = client.getBulkReply();
    return (dval != null ? new Double(dval) : null);
  }

  /**
   * 查看哈希表key 中，给定域field 是否存在。可用版本： >= 2.0.0
   * 时间复杂度： O(1)
   * 返回值：如果哈希表含有给定域，返回1 。如果哈希表不含有给定域，或key 不存在，返回0 。
   * Test for existence of a specified field in a hash. <b>Time complexity:</b> O(1)
   * @param key
   * @param field
   * @return Return 1 if the hash stored at key contains the specified field. Return 0 if the key is
   *         not found or the field is not present.
   */
  @Override
  public Boolean hexists(final String key, final String field) {
    checkIsInMultiOrPipeline();
    client.hexists(key, field);
    return client.getIntegerReply() == 1;
  }

  /**
   * 删除哈希表key 中的一个或多个指定域，不存在的域将被忽略。Note: 在Redis2.4 以下的版本里，HDEL 每次只能删除单个域，
   * 如果你需要在一个原子时间内删除多个域，请将命令包含在MULTI / EXEC 块内。可用版本： >= 2.0.0时间复杂度: O(N)，N 为要删除的域的数量。
   * 返回值: 被成功移除的域的数量，不包括被忽略的域。
   * Remove the specified field from an hash stored at key.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param fields
   * @return If the field was present in the hash it is deleted and 1 is returned, otherwise 0 is
   *         returned and no operation is performed.
   */
  @Override
  public Long hdel(final String key, final String... fields) {
    checkIsInMultiOrPipeline();
    client.hdel(key, fields);
    return client.getIntegerReply();
  }

  /**
   * 返回哈希表key 中域的数量。
   * 时间复杂度： O(1)
   * 返回值：哈希表中域的数量。当key 不存在时，返回0 。
   * Return the number of items in a hash.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @return The number of entries (fields) contained in the hash stored at key. If the specified
   *         key does not exist, 0 is returned assuming an empty hash.
   */
  @Override
  public Long hlen(final String key) {
    checkIsInMultiOrPipeline();
    client.hlen(key);
    return client.getIntegerReply();
  }

  /**
   * 返回哈希表key 中的所有域。
   * 可用版本： >= 2.0.0
   * 时间复杂度： O(N)，N 为哈希表的大小。
   * 返回值：一个包含哈希表中所有域的表。当key 不存在时，返回一个空表。
   * Return all the fields in a hash.
   * <p>
   * <b>Time complexity:</b> O(N), where N is the total number of entries
   * @param key
   * @return All the fields names contained into a hash.
   */
  @Override
  public Set<String> hkeys(final String key) {
    checkIsInMultiOrPipeline();
    client.hkeys(key);
    return BuilderFactory.STRING_SET.build(client.getBinaryMultiBulkReply());
  }

  /**
   * 返回哈希表key 中所有域的值。可用版本： >= 2.0.0时间复杂度： O(N)，N 为哈希表的大小。返回值：一个包含哈希表中所有值的表。当key 不存在时，返回一个空表。
   * Return all the values in a hash.
   * <p>
   * <b>Time complexity:</b> O(N), where N is the total number of entries
   * @param key
   * @return All the fields values contained into a hash.
   */
  @Override
  public List<String> hvals(final String key) {
    checkIsInMultiOrPipeline();
    client.hvals(key);
    final List<String> lresult = client.getMultiBulkReply();
    return lresult;
  }

  /**
   * 返回哈希表key 中，所有的域和值。在返回值里，紧跟每个域名(field name) 之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
   * 可用版本： >= 2.0.0时间复杂度： O(N)，N 为哈希表的大小。返回值：以列表形式返回哈希表的域和域的值。若key 不存在，返回空列表。
   * Return all the fields and associated values in a hash.
   * <p>
   * <b>Time complexity:</b> O(N), where N is the total number of entries
   * @param key
   * @return All the fields and values contained into a hash.
   */
  @Override
  public Map<String, String> hgetAll(final String key) {
    checkIsInMultiOrPipeline();
    client.hgetAll(key);
    return BuilderFactory.STRING_MAP.build(client.getBinaryMultiBulkReply());
  }

  /**
   * 将一个或多个值value 插入到列表key 的表尾(最右边)。如果有多个value 值，那么各个value 值按从左到右的顺序依次插入到表尾：比如对一个空列表mylist
   * 执行RPUSH mylist a b c ，得出的结果列表为a b c ，等同于执行命令RPUSH mylist a 、RPUSH mylistb 、RPUSH mylist c 。
   * 如果key 不存在，一个空列表会被创建并执行RPUSH 操作。当key 存在但不是列表类型时，返回一个错误。
   * 返回值： 执行RPUSH 操作后，表的长度。
   * Add the string value to the head (LPUSH) or tail (RPUSH) of the list stored at key. If the key
   * does not exist an empty list is created just before the append operation. If the key exists but
   * is not a List an error is returned.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param strings
   * @return Integer reply, specifically, the number of elements inside the list after the push
   *         operation.
   */
  @Override
  public Long rpush(final String key, final String... strings) {
    checkIsInMultiOrPipeline();
    client.rpush(key, strings);
    return client.getIntegerReply();
  }

  /**
   * 将一个或多个值value 插入到列表key 的表头如果有多个value 值，那么各个value 值按从左到右的顺序依次插入到表头：比如说，对空列表mylist
   * 执行命令LPUSH mylist a b c ，列表的值将是c b a ，这等同于原子性地执行LPUSH mylist a 、LPUSH mylist b 和LPUSH mylist c 三个命令。
   * 如果key 不存在，一个空列表会被创建并执行LPUSH 操作。
   * 当key 存在但不是列表类型时，返回一个错误。
   * Note: 在Redis 2.4 版本以前的LPUSH 命令，都只接受单个value 值。
   * Add the string value to the head (LPUSH) or tail (RPUSH) of the list stored at key. If the key
   * does not exist an empty list is created just before the append operation. If the key exists but
   * is not a List an error is returned.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @param strings
   * @return Integer reply, specifically, the number of elements inside the list after the push
   *         operation.
   * 返回值： 执行LPUSH 命令后，列表的长度。
   */
  @Override
  public Long lpush(final String key, final String... strings) {
    checkIsInMultiOrPipeline();
    client.lpush(key, strings);
    return client.getIntegerReply();
  }

  /**
   * 
   * 返回列表key 的长度。如果key 不存在，则key 被解释为一个空列表，返回0 .如果key 不是列表类型，返回一个错误。
   * Return the length of the list stored at the specified key. If the key does not exist zero is
   * returned (the same behaviour as for empty lists). If the value stored at key is not a list an
   * error is returned.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @return The length of the list.
   */
  @Override
  public Long llen(final String key) {
    checkIsInMultiOrPipeline();
    client.llen(key);
    return client.getIntegerReply();
  }

  /**
   * 返回列表key 中指定区间内的元素，区间以偏移量start 和stop 指定。
   * 下标(index) 参数start 和stop 都以0 为底，也就是说，以0 表示列表的第一个元素，以1 表示列表的第二个元素，以此类推。
   * 你也可以使用负数下标，以-1 表示列表的最后一个元素，-2 表示列表的倒数第二个元素，以此类推。注意LRANGE 命令和编程语言区间函数的区别
   * 假如你有一个包含一百个元素的列表，对该列表执行LRANGE list 0 10 ，结果是一个包含11 个元素的列表，
   * 这表明stop 下标也在LRANGE 命令的取值范围之内(闭区间)，这和某些语言的区间函数可能不一致，比如Ruby 的Range.new 、Array#slice 和Python 的range() 函数。超出范围的下标
   * 超出范围的下标值不会引起错误。如果start 下标比列表的最大下标end ( LLEN list 减去1 ) 还要大，那么LRANGE 返回一个空列表。
   * 如果stop 下标比end 下标还要大，Redis 将stop 的值设置为end 。
   * 可用版本： >= 1.0.0
   * 时间复杂度: O(S+N)，S 为偏移量start ，N 为指定区间内元素的数量。
   * 返回值: 一个列表，包含指定区间内的元素。
   * Return the specified elements of the list stored at the specified key. Start and end are
   * zero-based indexes. 0 is the first element of the list (the list head), 1 the next element and
   * so on.
   * <p>
   * For example LRANGE foobar 0 2 will return the first three elements of the list.
   * <p>
   * start and end can also be negative numbers indicating offsets from the end of the list. For
   * example -1 is the last element of the list, -2 the penultimate element and so on.
   * <p>
   * <b>Consistency with range functions in various programming languages</b>
   * <p>
   * Note that if you have a list of numbers from 0 to 100, LRANGE 0 10 will return 11 elements,
   * that is, rightmost item is included. This may or may not be consistent with behavior of
   * range-related functions in your programming language of choice (think Ruby's Range.new,
   * Array#slice or Python's range() function).
   * <p>
   * LRANGE behavior is consistent with one of Tcl.
   * <p>
   * <b>Out-of-range indexes</b>
   * <p>
   * Indexes out of range will not produce an error: if start is over the end of the list, or start
   * &gt; end, an empty list is returned. If end is over the end of the list Redis will threat it
   * just like the last element of the list.
   * <p>
   * Time complexity: O(start+n) (with n being the length of the range and start being the start
   * offset)
   * @param key
   * @param start
   * @param end
   * @return Multi bulk reply, specifically a list of elements in the specified range.
   */
  @Override
  public List<String> lrange(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.lrange(key, start, end);
    return client.getMultiBulkReply();
  }

  /**
   * 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。举个例子，执行命令LTRIM list 0 2 ，
   * 表示只保留列表list 的前三个元素，其余元素全部删除。下标(index) 参数start 和stop 都以0 为底，也就是说，以0 表示列表的第一个元素，以1 表示列表的第二个元素，以此类推。
   * 你也可以使用负数下标，以-1 表示列表的最后一个元素，-2 表示列表的倒数第二个元素，以此类推。当key 不是列表类型时，返回一个错误。LTRIM 命令通常和LPUSH 命令或RPUSH 命令配合使用，举个例子：
   * LPUSH log newest_log LTRIM log 0 99 这个例子模拟了一个日志程序，每次将最新日志newest_log 放到log 列表中，并且只保留最新的100 项。
   * 注意当这样使用LTRIM 命令时，时间复杂度是O(1)，因为平均情况下，每次只有一个元素被移除。注意LTRIM 命令和编程语言区间函数的区别假如你有一个包含一百个元素的列表list ，
   * 对该列表执行LTRIM list 0 10 ，结果是一个包含11 个元素的列表，这表明stop 下标也在LTRIM 命令的取值范围之内(闭区间)，这和某些语言的区间函数可能不一致，
   * 比如Ruby 的Range.new 、Array#slice 和Python 的range() 函数。
   * 超出范围的下标 超出范围的下标值不会引起错误。如果start 下标比列表的最大下标end ( LLEN list 减去1 ) 还要大，或者start > stop ，LTRIM 返回
   * 一个空列表(因为LTRIM 已经将整个列表清空)。如果stop 下标比end 下标还要大，Redis 将stop 的值设置为end 。
   * 可用版本： >= 1.0.0
   * 时间复杂度: O(N)，N 为被移除的元素的数量。
   * 返回值:命令执行成功时，返回ok 。
   * Trim an existing list so that it will contain only the specified range of elements specified.
   * Start and end are zero-based indexes. 0 is the first element of the list (the list head), 1 the
   * next element and so on.
   * <p>
   * For example LTRIM foobar 0 2 will modify the list stored at foobar key so that only the first
   * three elements of the list will remain.
   * <p>
   * start and end can also be negative numbers indicating offsets from the end of the list. For
   * example -1 is the last element of the list, -2 the penultimate element and so on.
   * <p>
   * Indexes out of range will not produce an error: if start is over the end of the list, or start
   * &gt; end, an empty list is left as value. If end over the end of the list Redis will threat it
   * just like the last element of the list.
   * <p>
   * Hint: the obvious use of LTRIM is together with LPUSH/RPUSH. For example:
   * <p>
   * {@code lpush("mylist", "someelement"); ltrim("mylist", 0, 99); * }
   * <p>
   * The above two commands will push elements in the list taking care that the list will not grow
   * without limits. This is very useful when using Redis to store logs for example. It is important
   * to note that when used in this way LTRIM is an O(1) operation because in the average case just
   * one element is removed from the tail of the list.
   * <p>
   * Time complexity: O(n) (with n being len of list - len of range)
   * @param key
   * @param start
   * @param end
   * @return Status code reply
   */
  @Override
  public String ltrim(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.ltrim(key, start, end);
    return client.getStatusCodeReply();
  }

  /**
   * 
   * 返回列表key 中，下标为index 的元素。下标(index) 参数start 和stop 都以0 为底，也就是说，
   * 以0 表示列表的第一个元素，以1 表示列表的第二个元素，以此类推。你也可以使用负数下标，以-1 表示列表的最后一个元素，-2 表示列表的倒数第二个元素，
   * 以此类推。如果key 不是列表类型，返回一个错误。可用版本： >= 1.0.0 时间复杂度：O(N)，N 为到达下标index 过程中经过的元素数量。
   * 因此，对列表的头元素和尾元素执行LINDEX 命令，复杂度为O(1)。返回值:列表中下标为index 的元素。
   * 如果index 参数的值不在列表的区间范围内(out of range)，返回nil 。
   * 
   * Return the specified element of the list stored at the specified key. 0 is the first element, 1
   * the second and so on. Negative indexes are supported, for example -1 is the last element, -2
   * the penultimate and so on.
   * <p>
   * If the value stored at key is not of list type an error is returned. If the index is out of
   * range a 'nil' reply is returned.
   * <p>
   * Note that even if the average time complexity is O(n) asking for the first or the last element
   * of the list is O(1).
   * <p>
   * Time complexity: O(n) (with n being the length of the list)
   * @param key
   * @param index
   * @return Bulk reply, specifically the requested element
   */
  @Override
  public String lindex(final String key, final long index) {
    checkIsInMultiOrPipeline();
    client.lindex(key, index);
    return client.getBulkReply();
  }

  /**
   * 将列表key 下标为index 的元素的值设置为value 。当index 参数超出范围，或对一个空列表( key 不存在) 进行LSET 时，返回一个错误。
   * 关于列表下标的更多信息，请参考LINDEX 命令。
   * 可用版本： >= 1.0.0
   * 时间复杂度：对头元素或尾元素进行LSET 操作，复杂度为O(1)。其他情况下，为O(N)，N 为列表的长度。
   * 返回值： 操作成功返回ok ，否则返回错误信息。
   * Set a new value as the element at index position of the List at key.
   * <p>
   * Out of range indexes will generate an error.
   * <p>
   * Similarly to other list commands accepting indexes, the index can be negative to access
   * elements starting from the end of the list. So -1 is the last element, -2 is the penultimate,
   * and so forth.
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(N) (with N being the length of the list), setting the first or last elements of the list is
   * O(1).
   * @see #lindex(String, long)
   * @param key
   * @param index
   * @param value
   * @return Status code reply
   */
  @Override
  public String lset(final String key, final long index, final String value) {
    checkIsInMultiOrPipeline();
    client.lset(key, index, value);
    return client.getStatusCodeReply();
  }

  /**
   * 根据参数count 的值，移除列表中与参数value 相等的元素。count 的值可以是以下几种：
   * count > 0 : 从表头开始向表尾搜索，移除与value 相等的元素，数量为count 。
   * count < 0 : 从表尾开始向表头搜索，移除与value 相等的元素，数量为count 的绝对值。
   * count = 0 : 移除表中所有与value 相等的值。
   * 可用版本： >= 1.0.0
   * 时间复杂度： O(N)，N 为列表的长度。
   * 返回值：被移除元素的数量。因为不存在的key 被视作空表(empty list)，所以当key 不存在时，LREM 命令总是返回0 。
   * Remove the first count occurrences of the value element from the list. If count is zero all the
   * elements are removed. If count is negative elements are removed from tail to head, instead to
   * go from head to tail that is the normal behaviour. So for example LREM with count -2 and hello
   * as value to remove against the list (a,b,c,hello,x,hello,hello) will lave the list
   * (a,b,c,hello,x). The number of removed elements is returned as an integer, see below for more
   * information about the returned value. Note that non existing keys are considered like empty
   * lists by LREM, so LREM against non existing keys will always return 0.
   * <p>
   * Time complexity: O(N) (with N being the length of the list)
   * @param key
   * @param count
   * @param value
   * @return Integer Reply, specifically: The number of removed elements if the operation succeeded
   */
  @Override
  public Long lrem(final String key, final long count, final String value) {
    checkIsInMultiOrPipeline();
    client.lrem(key, count, value);
    return client.getIntegerReply();
  }

  /**
   * 移除并返回列表key 的头元素。
   * Atomically return and remove the first (LPOP) or last (RPOP) element of the list. For example
   * if the list contains the elements "a","b","c" LPOP will return "a" and the list will become
   * "b","c".
   * <p>
   * If the key does not exist or the list is already empty the special value 'nil' is returned.
   * @see #rpop(String)
   * @param key
   * @return Bulk reply
   * 列表的头元素。 当key 不存在时，返回nil 。
   */
  @Override
  public String lpop(final String key) {
    checkIsInMultiOrPipeline();
    client.lpop(key);
    return client.getBulkReply();
  }

  /**
   * Atomically return and remove the first (LPOP) or last (RPOP) element of the list. For example
   * if the list contains the elements "a","b","c" RPOP will return "c" and the list will become
   * "a","b".
   * <p>
   * If the key does not exist or the list is already empty the special value 'nil' is returned.
   * @see #lpop(String)
   * @param key
   * @return Bulk reply
   */
  @Override
  public String rpop(final String key) {
    checkIsInMultiOrPipeline();
    client.rpop(key);
    return client.getBulkReply();
  }

  /**
   * 命令RPOPLPUSH 在一个原子时间内，执行以下两个动作：将列表source 中的最后一个元素(尾元素) 弹出，并返回给客户端。
   * 将source 弹出的元素插入到列表destination ，作为destination 列表的的头元素。
   * 举个例子，你有两个列表source 和destination ，source 列表有元素a, b, c ，destination 列表有元素x, y, z ，
   * 执行RPOPLPUSH source destination 之后，source 列表包含元素a, b ，destination 列表包含元素c, x, y, z ，并且元素c 会被返回给客户端。
   * 如果source 不存在，值nil 被返回，并且不执行其他动作。
   * 如果source 和destination 相同，则列表中的表尾元素被移动到表头，并返回该元素，可以把这种特殊情况视作列表的旋转(rotation) 操作。
   * 可用版本： >= 1.2.0
   * 时间复杂度： O(1)
   * 返回值： 被弹出的元素。
   * Atomically return and remove the last (tail) element of the srckey list, and push the element
   * as the first (head) element of the dstkey list. For example if the source list contains the
   * elements "a","b","c" and the destination list contains the elements "foo","bar" after an
   * RPOPLPUSH command the content of the two lists will be "a","b" and "c","foo","bar".
   * <p>
   * If the key does not exist or the list is already empty the special value 'nil' is returned. If
   * the srckey and dstkey are the same the operation is equivalent to removing the last element
   * from the list and pusing it as first element of the list, so it's a "list rotation" command.
   * <p>
   * Time complexity: O(1)
   * @param srckey
   * @param dstkey
   * @return Bulk reply
   */
  @Override
  public String rpoplpush(final String srckey, final String dstkey) {
    checkIsInMultiOrPipeline();
    client.rpoplpush(srckey, dstkey);
    return client.getBulkReply();
  }

  /**
   * Add the specified member to the set value stored at key. If member is already a member of the
   * set no operation is performed. If key does not exist a new set with the specified member as
   * sole member is created. If the key exists but does not hold a set value an error is returned.
   * <p>
   * Time complexity O(1)
   * @param key
   * @param members
   * @return Integer reply, specifically: 1 if the new element was added 0 if the element was
   *         already a member of the set
   */
  @Override
  public Long sadd(final String key, final String... members) {
    checkIsInMultiOrPipeline();
    client.sadd(key, members);
    return client.getIntegerReply();
  }

  /**
   * Return all the members (elements) of the set value stored at key. This is just syntax glue for
   * {@link #sinter(String...) SINTER}.
   * <p>
   * Time complexity O(N)
   * @param key
   * @return Multi bulk reply
   */
  @Override
  public Set<String> smembers(final String key) {
    checkIsInMultiOrPipeline();
    client.smembers(key);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * Remove the specified member from the set value stored at key. If member was not a member of the
   * set no operation is performed. If key does not hold a set value an error is returned.
   * <p>
   * Time complexity O(1)
   * @param key
   * @param members
   * @return Integer reply, specifically: 1 if the new element was removed 0 if the new element was
   *         not a member of the set
   */
  @Override
  public Long srem(final String key, final String... members) {
    checkIsInMultiOrPipeline();
    client.srem(key, members);
    return client.getIntegerReply();
  }

  /**
   * Remove a random element from a Set returning it as return value. If the Set is empty or the key
   * does not exist, a nil object is returned.
   * <p>
   * The {@link #srandmember(String)} command does a similar work but the returned element is not
   * removed from the Set.
   * <p>
   * Time complexity O(1)
   * @param key
   * @return Bulk reply
   */
  @Override
  public String spop(final String key) {
    checkIsInMultiOrPipeline();
    client.spop(key);
    return client.getBulkReply();
  }

  @Override
  public Set<String> spop(final String key, final long count) {
    checkIsInMultiOrPipeline();
    client.spop(key, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * Move the specifided member from the set at srckey to the set at dstkey. This operation is
   * atomic, in every given moment the element will appear to be in the source or destination set
   * for accessing clients.
   * <p>
   * If the source set does not exist or does not contain the specified element no operation is
   * performed and zero is returned, otherwise the element is removed from the source set and added
   * to the destination set. On success one is returned, even if the element was already present in
   * the destination set.
   * <p>
   * An error is raised if the source or destination keys contain a non Set value.
   * <p>
   * Time complexity O(1)
   * @param srckey
   * @param dstkey
   * @param member
   * @return Integer reply, specifically: 1 if the element was moved 0 if the element was not found
   *         on the first set and no operation was performed
   */
  @Override
  public Long smove(final String srckey, final String dstkey, final String member) {
    checkIsInMultiOrPipeline();
    client.smove(srckey, dstkey, member);
    return client.getIntegerReply();
  }

  /**
   * Return the set cardinality (number of elements). If the key does not exist 0 is returned, like
   * for empty sets.
   * @param key
   * @return Integer reply, specifically: the cardinality (number of elements) of the set as an
   *         integer.
   */
  @Override
  public Long scard(final String key) {
    checkIsInMultiOrPipeline();
    client.scard(key);
    return client.getIntegerReply();
  }

  /**
   * Return 1 if member is a member of the set stored at key, otherwise 0 is returned.
   * <p>
   * Time complexity O(1)
   * @param key
   * @param member
   * @return Integer reply, specifically: 1 if the element is a member of the set 0 if the element
   *         is not a member of the set OR if the key does not exist
   */
  @Override
  public Boolean sismember(final String key, final String member) {
    checkIsInMultiOrPipeline();
    client.sismember(key, member);
    return client.getIntegerReply() == 1;
  }

  /**
   * Return the members of a set resulting from the intersection of all the sets hold at the
   * specified keys. Like in {@link #lrange(String, long, long) LRANGE} the result is sent to the
   * client as a multi-bulk reply (see the protocol specification for more information). If just a
   * single key is specified, then this command produces the same result as
   * {@link #smembers(String) SMEMBERS}. Actually SMEMBERS is just syntax sugar for SINTER.
   * <p>
   * Non existing keys are considered like empty sets, so if one of the keys is missing an empty set
   * is returned (since the intersection with an empty set always is an empty set).
   * <p>
   * Time complexity O(N*M) worst case where N is the cardinality of the smallest set and M the
   * number of sets
   * @param keys
   * @return Multi bulk reply, specifically the list of common elements.
   */
  @Override
  public Set<String> sinter(final String... keys) {
    checkIsInMultiOrPipeline();
    client.sinter(keys);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * This commnad works exactly like {@link #sinter(String...) SINTER} but instead of being returned
   * the resulting set is sotred as dstkey.
   * <p>
   * Time complexity O(N*M) worst case where N is the cardinality of the smallest set and M the
   * number of sets
   * @param dstkey
   * @param keys
   * @return Status code reply
   */
  @Override
  public Long sinterstore(final String dstkey, final String... keys) {
    checkIsInMultiOrPipeline();
    client.sinterstore(dstkey, keys);
    return client.getIntegerReply();
  }

  /**
   * Return the members of a set resulting from the union of all the sets hold at the specified
   * keys. Like in {@link #lrange(String, long, long) LRANGE} the result is sent to the client as a
   * multi-bulk reply (see the protocol specification for more information). If just a single key is
   * specified, then this command produces the same result as {@link #smembers(String) SMEMBERS}.
   * <p>
   * Non existing keys are considered like empty sets.
   * <p>
   * Time complexity O(N) where N is the total number of elements in all the provided sets
   * @param keys
   * @return Multi bulk reply, specifically the list of common elements.
   */
  @Override
  public Set<String> sunion(final String... keys) {
    checkIsInMultiOrPipeline();
    client.sunion(keys);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * This command works exactly like {@link #sunion(String...) SUNION} but instead of being returned
   * the resulting set is stored as dstkey. Any existing value in dstkey will be over-written.
   * <p>
   * Time complexity O(N) where N is the total number of elements in all the provided sets
   * @param dstkey
   * @param keys
   * @return Status code reply
   */
  @Override
  public Long sunionstore(final String dstkey, final String... keys) {
    checkIsInMultiOrPipeline();
    client.sunionstore(dstkey, keys);
    return client.getIntegerReply();
  }

  /**
   * Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN
   * <p>
   * <b>Example:</b>
   * 
   * <pre>
   * key1 = [x, a, b, c]
   * key2 = [c]
   * key3 = [a, d]
   * SDIFF key1,key2,key3 =&gt; [x, b]
   * </pre>
   * 
   * Non existing keys are considered like empty sets.
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(N) with N being the total number of elements of all the sets
   * @param keys
   * @return Return the members of a set resulting from the difference between the first set
   *         provided and all the successive sets.
   */
  @Override
  public Set<String> sdiff(final String... keys) {
    checkIsInMultiOrPipeline();
    client.sdiff(keys);
    return BuilderFactory.STRING_SET.build(client.getBinaryMultiBulkReply());
  }

  /**
   * This command works exactly like {@link #sdiff(String...) SDIFF} but instead of being returned
   * the resulting set is stored in dstkey.
   * @param dstkey
   * @param keys
   * @return Status code reply
   */
  @Override
  public Long sdiffstore(final String dstkey, final String... keys) {
    checkIsInMultiOrPipeline();
    client.sdiffstore(dstkey, keys);
    return client.getIntegerReply();
  }

  /**
   * Return a random element from a Set, without removing the element. If the Set is empty or the
   * key does not exist, a nil object is returned.
   * <p>
   * The SPOP command does a similar work but the returned element is popped (removed) from the Set.
   * <p>
   * Time complexity O(1)
   * @param key
   * @return Bulk reply
   */
  @Override
  public String srandmember(final String key) {
    checkIsInMultiOrPipeline();
    client.srandmember(key);
    return client.getBulkReply();
  }

  @Override
  public List<String> srandmember(final String key, final int count) {
    checkIsInMultiOrPipeline();
    client.srandmember(key, count);
    return client.getMultiBulkReply();
  }

  /**
   * Add the specified member having the specifeid score to the sorted set stored at key. If member
   * is already a member of the sorted set the score is updated, and the element reinserted in the
   * right position to ensure sorting. If key does not exist a new sorted set with the specified
   * member as sole member is crated. If the key exists but does not hold a sorted set value an
   * error is returned.
   * <p>
   * The score value can be the string representation of a double precision floating point number.
   * <p>
   * Time complexity O(log(N)) with N being the number of elements in the sorted set
   * @param key
   * @param score
   * @param member
   * @return Integer reply, specifically: 1 if the new element was added 0 if the element was
   *         already a member of the sorted set and the score was updated
   */
  @Override
  public Long zadd(final String key, final double score, final String member) {
    checkIsInMultiOrPipeline();
    client.zadd(key, score, member);
    return client.getIntegerReply();
  }

  @Override
  public Long zadd(final String key, final double score, final String member,
      final ZAddParams params) {
    checkIsInMultiOrPipeline();
    client.zadd(key, score, member, params);
    return client.getIntegerReply();
  }

  @Override
  public Long zadd(final String key, final Map<String, Double> scoreMembers) {
    checkIsInMultiOrPipeline();
    client.zadd(key, scoreMembers);
    return client.getIntegerReply();
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
    checkIsInMultiOrPipeline();
    client.zadd(key, scoreMembers, params);
    return client.getIntegerReply();
  }

  @Override
  public Set<String> zrange(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.zrange(key, start, end);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * Remove the specified member from the sorted set value stored at key. If member was not a member
   * of the set no operation is performed. If key does not not hold a set value an error is
   * returned.
   * <p>
   * Time complexity O(log(N)) with N being the number of elements in the sorted set
   * @param key
   * @param members
   * @return Integer reply, specifically: 1 if the new element was removed 0 if the new element was
   *         not a member of the set
   */
  @Override
  public Long zrem(final String key, final String... members) {
    checkIsInMultiOrPipeline();
    client.zrem(key, members);
    return client.getIntegerReply();
  }

  /**
   * If member already exists in the sorted set adds the increment to its score and updates the
   * position of the element in the sorted set accordingly. If member does not already exist in the
   * sorted set it is added with increment as score (that is, like if the previous score was
   * virtually zero). If key does not exist a new sorted set with the specified member as sole
   * member is crated. If the key exists but does not hold a sorted set value an error is returned.
   * <p>
   * The score value can be the string representation of a double precision floating point number.
   * It's possible to provide a negative value to perform a decrement.
   * <p>
   * For an introduction to sorted sets check the Introduction to Redis data types page.
   * <p>
   * Time complexity O(log(N)) with N being the number of elements in the sorted set
   * @param key
   * @param score
   * @param member
   * @return The new score
   */
  @Override
  public Double zincrby(final String key, final double score, final String member) {
    checkIsInMultiOrPipeline();
    client.zincrby(key, score, member);
    String newscore = client.getBulkReply();
    return Double.valueOf(newscore);
  }

  @Override
  public Double zincrby(String key, double score, String member, ZIncrByParams params) {
    checkIsInMultiOrPipeline();
    client.zincrby(key, score, member, params);
    String newscore = client.getBulkReply();

    // with nx / xx options it could return null now
    if (newscore == null) return null;

    return Double.valueOf(newscore);
  }

  /**
   * Return the rank (or index) or member in the sorted set at key, with scores being ordered from
   * low to high.
   * <p>
   * When the given member does not exist in the sorted set, the special value 'nil' is returned.
   * The returned rank (or index) of the member is 0-based for both commands.
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))
   * @see #zrevrank(String, String)
   * @param key
   * @param member
   * @return Integer reply or a nil bulk reply, specifically: the rank of the element as an integer
   *         reply if the element exists. A nil bulk reply if there is no such element.
   */
  @Override
  public Long zrank(final String key, final String member) {
    checkIsInMultiOrPipeline();
    client.zrank(key, member);
    return client.getIntegerReply();
  }

  /**
   * Return the rank (or index) or member in the sorted set at key, with scores being ordered from
   * high to low.
   * <p>
   * When the given member does not exist in the sorted set, the special value 'nil' is returned.
   * The returned rank (or index) of the member is 0-based for both commands.
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))
   * @see #zrank(String, String)
   * @param key
   * @param member
   * @return Integer reply or a nil bulk reply, specifically: the rank of the element as an integer
   *         reply if the element exists. A nil bulk reply if there is no such element.
   */
  @Override
  public Long zrevrank(final String key, final String member) {
    checkIsInMultiOrPipeline();
    client.zrevrank(key, member);
    return client.getIntegerReply();
  }

  @Override
  public Set<String> zrevrange(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.zrevrange(key, start, end);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.zrangeWithScores(key, start, end);
    return getTupledSet();
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.zrevrangeWithScores(key, start, end);
    return getTupledSet();
  }

  /**
   * Return the sorted set cardinality (number of elements). If the key does not exist 0 is
   * returned, like for empty sorted sets.
   * <p>
   * Time complexity O(1)
   * @param key
   * @return the cardinality (number of elements) of the set as an integer.
   */
  @Override
  public Long zcard(final String key) {
    checkIsInMultiOrPipeline();
    client.zcard(key);
    return client.getIntegerReply();
  }

  /**
   * Return the score of the specified element of the sorted set at key. If the specified element
   * does not exist in the sorted set, or the key does not exist at all, a special 'nil' value is
   * returned.
   * <p>
   * <b>Time complexity:</b> O(1)
   * @param key
   * @param member
   * @return the score
   */
  @Override
  public Double zscore(final String key, final String member) {
    checkIsInMultiOrPipeline();
    client.zscore(key, member);
    final String score = client.getBulkReply();
    return (score != null ? new Double(score) : null);
  }

  @Override
  public String watch(final String... keys) {
    client.watch(keys);
    return client.getStatusCodeReply();
  }

  /**
   * Sort a Set or a List.
   * <p>
   * Sort the elements contained in the List, Set, or Sorted Set value at key. By default sorting is
   * numeric with elements being compared as double precision floating point numbers. This is the
   * simplest form of SORT.
   * @see #sort(String, String)
   * @see #sort(String, SortingParams)
   * @see #sort(String, SortingParams, String)
   * @param key
   * @return Assuming the Set/List at key contains a list of numbers, the return value will be the
   *         list of numbers ordered from the smallest to the biggest number.
   */
  @Override
  public List<String> sort(final String key) {
    checkIsInMultiOrPipeline();
    client.sort(key);
    return client.getMultiBulkReply();
  }

  /**
   * Sort a Set or a List accordingly to the specified parameters.
   * <p>
   * <b>examples:</b>
   * <p>
   * Given are the following sets and key/values:
   * 
   * <pre>
   * x = [1, 2, 3]
   * y = [a, b, c]
   * 
   * k1 = z
   * k2 = y
   * k3 = x
   * 
   * w1 = 9
   * w2 = 8
   * w3 = 7
   * </pre>
   * 
   * Sort Order:
   * 
   * <pre>
   * sort(x) or sort(x, sp.asc())
   * -&gt; [1, 2, 3]
   * 
   * sort(x, sp.desc())
   * -&gt; [3, 2, 1]
   * 
   * sort(y)
   * -&gt; [c, a, b]
   * 
   * sort(y, sp.alpha())
   * -&gt; [a, b, c]
   * 
   * sort(y, sp.alpha().desc())
   * -&gt; [c, a, b]
   * </pre>
   * 
   * Limit (e.g. for Pagination):
   * 
   * <pre>
   * sort(x, sp.limit(0, 2))
   * -&gt; [1, 2]
   * 
   * sort(y, sp.alpha().desc().limit(1, 2))
   * -&gt; [b, a]
   * </pre>
   * 
   * Sorting by external keys:
   * 
   * <pre>
   * sort(x, sb.by(w*))
   * -&gt; [3, 2, 1]
   * 
   * sort(x, sb.by(w*).desc())
   * -&gt; [1, 2, 3]
   * </pre>
   * 
   * Getting external keys:
   * 
   * <pre>
   * sort(x, sp.by(w*).get(k*))
   * -&gt; [x, y, z]
   * 
   * sort(x, sp.by(w*).get(#).get(k*))
   * -&gt; [3, x, 2, y, 1, z]
   * </pre>
   * @see #sort(String)
   * @see #sort(String, SortingParams, String)
   * @param key
   * @param sortingParameters
   * @return a list of sorted elements.
   */
  @Override
  public List<String> sort(final String key, final SortingParams sortingParameters) {
    checkIsInMultiOrPipeline();
    client.sort(key, sortingParameters);
    return client.getMultiBulkReply();
  }

  /**
   * BLPOP (and BRPOP) is a blocking list pop primitive. You can see this commands as blocking
   * versions of LPOP and RPOP able to block if the specified keys don't exist or contain empty
   * lists.
   * <p>
   * The following is a description of the exact semantic. We describe BLPOP but the two commands
   * are identical, the only difference is that BLPOP pops the element from the left (head) of the
   * list, and BRPOP pops from the right (tail).
   * <p>
   * <b>Non blocking behavior</b>
   * <p>
   * When BLPOP is called, if at least one of the specified keys contain a non empty list, an
   * element is popped from the head of the list and returned to the caller together with the name
   * of the key (BLPOP returns a two elements array, the first element is the key, the second the
   * popped value).
   * <p>
   * Keys are scanned from left to right, so for instance if you issue BLPOP list1 list2 list3 0
   * against a dataset where list1 does not exist but list2 and list3 contain non empty lists, BLPOP
   * guarantees to return an element from the list stored at list2 (since it is the first non empty
   * list starting from the left).
   * <p>
   * <b>Blocking behavior</b>
   * <p>
   * If none of the specified keys exist or contain non empty lists, BLPOP blocks until some other
   * client performs a LPUSH or an RPUSH operation against one of the lists.
   * <p>
   * Once new data is present on one of the lists, the client finally returns with the name of the
   * key unblocking it and the popped value.
   * <p>
   * When blocking, if a non-zero timeout is specified, the client will unblock returning a nil
   * special value if the specified amount of seconds passed without a push operation against at
   * least one of the specified keys.
   * <p>
   * The timeout argument is interpreted as an integer value. A timeout of zero means instead to
   * block forever.
   * <p>
   * <b>Multiple clients blocking for the same keys</b>
   * <p>
   * Multiple clients can block for the same key. They are put into a queue, so the first to be
   * served will be the one that started to wait earlier, in a first-blpopping first-served fashion.
   * <p>
   * <b>blocking POP inside a MULTI/EXEC transaction</b>
   * <p>
   * BLPOP and BRPOP can be used with pipelining (sending multiple commands and reading the replies
   * in batch), but it does not make sense to use BLPOP or BRPOP inside a MULTI/EXEC block (a Redis
   * transaction).
   * <p>
   * The behavior of BLPOP inside MULTI/EXEC when the list is empty is to return a multi-bulk nil
   * reply, exactly what happens when the timeout is reached. If you like science fiction, think at
   * it like if inside MULTI/EXEC the time will flow at infinite speed :)
   * <p>
   * Time complexity: O(1)
   * @see #brpop(int, String...)
   * @param timeout
   * @param keys
   * @return BLPOP returns a two-elements array via a multi bulk reply in order to return both the
   *         unblocking key and the popped value.
   *         <p>
   *         When a non-zero timeout is specified, and the BLPOP operation timed out, the return
   *         value is a nil multi bulk reply. Most client values will return false or nil
   *         accordingly to the programming language used.
   */
  @Override
  public List<String> blpop(final int timeout, final String... keys) {
    return blpop(getArgsAddTimeout(timeout, keys));
  }

  private String[] getArgsAddTimeout(int timeout, String[] keys) {
    final int keyCount = keys.length;
    final String[] args = new String[keyCount + 1];
    for (int at = 0; at != keyCount; ++at) {
      args[at] = keys[at];
    }

    args[keyCount] = String.valueOf(timeout);
    return args;
  }

  @Override
  public List<String> blpop(String... args) {
    checkIsInMultiOrPipeline();
    client.blpop(args);
    client.setTimeoutInfinite();
    try {
      return client.getMultiBulkReply();
    } finally {
      client.rollbackTimeout();
    }
  }

  @Override
  public List<String> brpop(String... args) {
    checkIsInMultiOrPipeline();
    client.brpop(args);
    client.setTimeoutInfinite();
    try {
      return client.getMultiBulkReply();
    } finally {
      client.rollbackTimeout();
    }
  }

  /**
   * Sort a Set or a List accordingly to the specified parameters and store the result at dstkey.
   * @see #sort(String, SortingParams)
   * @see #sort(String)
   * @see #sort(String, String)
   * @param key
   * @param sortingParameters
   * @param dstkey
   * @return The number of elements of the list at dstkey.
   */
  @Override
  public Long sort(final String key, final SortingParams sortingParameters, final String dstkey) {
    checkIsInMultiOrPipeline();
    client.sort(key, sortingParameters, dstkey);
    return client.getIntegerReply();
  }

  /**
   * Sort a Set or a List and Store the Result at dstkey.
   * <p>
   * Sort the elements contained in the List, Set, or Sorted Set value at key and store the result
   * at dstkey. By default sorting is numeric with elements being compared as double precision
   * floating point numbers. This is the simplest form of SORT.
   * @see #sort(String)
   * @see #sort(String, SortingParams)
   * @see #sort(String, SortingParams, String)
   * @param key
   * @param dstkey
   * @return The number of elements of the list at dstkey.
   */
  @Override
  public Long sort(final String key, final String dstkey) {
    checkIsInMultiOrPipeline();
    client.sort(key, dstkey);
    return client.getIntegerReply();
  }

  /**
   * BLPOP (and BRPOP) is a blocking list pop primitive. You can see this commands as blocking
   * versions of LPOP and RPOP able to block if the specified keys don't exist or contain empty
   * lists.
   * <p>
   * The following is a description of the exact semantic. We describe BLPOP but the two commands
   * are identical, the only difference is that BLPOP pops the element from the left (head) of the
   * list, and BRPOP pops from the right (tail).
   * <p>
   * <b>Non blocking behavior</b>
   * <p>
   * When BLPOP is called, if at least one of the specified keys contain a non empty list, an
   * element is popped from the head of the list and returned to the caller together with the name
   * of the key (BLPOP returns a two elements array, the first element is the key, the second the
   * popped value).
   * <p>
   * Keys are scanned from left to right, so for instance if you issue BLPOP list1 list2 list3 0
   * against a dataset where list1 does not exist but list2 and list3 contain non empty lists, BLPOP
   * guarantees to return an element from the list stored at list2 (since it is the first non empty
   * list starting from the left).
   * <p>
   * <b>Blocking behavior</b>
   * <p>
   * If none of the specified keys exist or contain non empty lists, BLPOP blocks until some other
   * client performs a LPUSH or an RPUSH operation against one of the lists.
   * <p>
   * Once new data is present on one of the lists, the client finally returns with the name of the
   * key unblocking it and the popped value.
   * <p>
   * When blocking, if a non-zero timeout is specified, the client will unblock returning a nil
   * special value if the specified amount of seconds passed without a push operation against at
   * least one of the specified keys.
   * <p>
   * The timeout argument is interpreted as an integer value. A timeout of zero means instead to
   * block forever.
   * <p>
   * <b>Multiple clients blocking for the same keys</b>
   * <p>
   * Multiple clients can block for the same key. They are put into a queue, so the first to be
   * served will be the one that started to wait earlier, in a first-blpopping first-served fashion.
   * <p>
   * <b>blocking POP inside a MULTI/EXEC transaction</b>
   * <p>
   * BLPOP and BRPOP can be used with pipelining (sending multiple commands and reading the replies
   * in batch), but it does not make sense to use BLPOP or BRPOP inside a MULTI/EXEC block (a Redis
   * transaction).
   * <p>
   * The behavior of BLPOP inside MULTI/EXEC when the list is empty is to return a multi-bulk nil
   * reply, exactly what happens when the timeout is reached. If you like science fiction, think at
   * it like if inside MULTI/EXEC the time will flow at infinite speed :)
   * <p>
   * Time complexity: O(1)
   * @see #blpop(int, String...)
   * @param timeout
   * @param keys
   * @return BLPOP returns a two-elements array via a multi bulk reply in order to return both the
   *         unblocking key and the popped value.
   *         <p>
   *         When a non-zero timeout is specified, and the BLPOP operation timed out, the return
   *         value is a nil multi bulk reply. Most client values will return false or nil
   *         accordingly to the programming language used.
   */
  @Override
  public List<String> brpop(final int timeout, final String... keys) {
    return brpop(getArgsAddTimeout(timeout, keys));
  }

  @Override
  public Long zcount(final String key, final double min, final double max) {
    checkIsInMultiOrPipeline();
    client.zcount(key, min, max);
    return client.getIntegerReply();
  }

  @Override
  public Long zcount(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zcount(key, min, max);
    return client.getIntegerReply();
  }

  /**
   * Return the all the elements in the sorted set at key with a score between min and max
   * (including elements with score equal to min or max).
   * <p>
   * The elements having the same score are returned sorted lexicographically as ASCII strings (this
   * follows from a property of Redis sorted sets and does not involve further computation).
   * <p>
   * Using the optional {@link #zrangeByScore(String, double, double, int, int) LIMIT} it's possible
   * to get only a range of the matching elements in an SQL-alike way. Note that if offset is large
   * the commands needs to traverse the list for offset elements and this adds up to the O(M)
   * figure.
   * <p>
   * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
   * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of returning the
   * actual elements in the specified interval, it just returns the number of matching elements.
   * <p>
   * <b>Exclusive intervals and infinity</b>
   * <p>
   * min and max can be -inf and +inf, so that you are not required to know what's the greatest or
   * smallest element in order to take, for instance, elements "up to a given value".
   * <p>
   * Also while the interval is for default closed (inclusive) it's possible to specify open
   * intervals prefixing the score with a "(" character, so for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (1.3 5}
   * <p>
   * Will return all the values with score &gt; 1.3 and &lt;= 5, while for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (5 (10}
   * <p>
   * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of
   * elements returned by the command, so if M is constant (for instance you always ask for the
   * first ten elements with LIMIT) you can consider it O(log(N))
   * @see #zrangeByScore(String, double, double)
   * @see #zrangeByScore(String, double, double, int, int)
   * @see #zrangeByScoreWithScores(String, double, double)
   * @see #zrangeByScoreWithScores(String, String, String)
   * @see #zrangeByScoreWithScores(String, double, double, int, int)
   * @see #zcount(String, double, double)
   * @param key
   * @param min a double or Double.MIN_VALUE for "-inf"
   * @param max a double or Double.MAX_VALUE for "+inf"
   * @return Multi bulk reply specifically a list of elements in the specified score range.
   */
  @Override
  public Set<String> zrangeByScore(final String key, final double min, final double max) {
    checkIsInMultiOrPipeline();
    client.zrangeByScore(key, min, max);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrangeByScore(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zrangeByScore(key, min, max);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * Return the all the elements in the sorted set at key with a score between min and max
   * (including elements with score equal to min or max).
   * <p>
   * The elements having the same score are returned sorted lexicographically as ASCII strings (this
   * follows from a property of Redis sorted sets and does not involve further computation).
   * <p>
   * Using the optional {@link #zrangeByScore(String, double, double, int, int) LIMIT} it's possible
   * to get only a range of the matching elements in an SQL-alike way. Note that if offset is large
   * the commands needs to traverse the list for offset elements and this adds up to the O(M)
   * figure.
   * <p>
   * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
   * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of returning the
   * actual elements in the specified interval, it just returns the number of matching elements.
   * <p>
   * <b>Exclusive intervals and infinity</b>
   * <p>
   * min and max can be -inf and +inf, so that you are not required to know what's the greatest or
   * smallest element in order to take, for instance, elements "up to a given value".
   * <p>
   * Also while the interval is for default closed (inclusive) it's possible to specify open
   * intervals prefixing the score with a "(" character, so for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (1.3 5}
   * <p>
   * Will return all the values with score &gt; 1.3 and &lt;= 5, while for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (5 (10}
   * <p>
   * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of
   * elements returned by the command, so if M is constant (for instance you always ask for the
   * first ten elements with LIMIT) you can consider it O(log(N))
   * @see #zrangeByScore(String, double, double)
   * @see #zrangeByScore(String, double, double, int, int)
   * @see #zrangeByScoreWithScores(String, double, double)
   * @see #zrangeByScoreWithScores(String, double, double, int, int)
   * @see #zcount(String, double, double)
   * @param key
   * @param min
   * @param max
   * @return Multi bulk reply specifically a list of elements in the specified score range.
   */
  @Override
  public Set<String> zrangeByScore(final String key, final double min, final double max,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrangeByScore(key, min, max, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrangeByScore(final String key, final String min, final String max,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrangeByScore(key, min, max, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  /**
   * Return the all the elements in the sorted set at key with a score between min and max
   * (including elements with score equal to min or max).
   * <p>
   * The elements having the same score are returned sorted lexicographically as ASCII strings (this
   * follows from a property of Redis sorted sets and does not involve further computation).
   * <p>
   * Using the optional {@link #zrangeByScore(String, double, double, int, int) LIMIT} it's possible
   * to get only a range of the matching elements in an SQL-alike way. Note that if offset is large
   * the commands needs to traverse the list for offset elements and this adds up to the O(M)
   * figure.
   * <p>
   * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
   * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of returning the
   * actual elements in the specified interval, it just returns the number of matching elements.
   * <p>
   * <b>Exclusive intervals and infinity</b>
   * <p>
   * min and max can be -inf and +inf, so that you are not required to know what's the greatest or
   * smallest element in order to take, for instance, elements "up to a given value".
   * <p>
   * Also while the interval is for default closed (inclusive) it's possible to specify open
   * intervals prefixing the score with a "(" character, so for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (1.3 5}
   * <p>
   * Will return all the values with score &gt; 1.3 and &lt;= 5, while for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (5 (10}
   * <p>
   * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of
   * elements returned by the command, so if M is constant (for instance you always ask for the
   * first ten elements with LIMIT) you can consider it O(log(N))
   * @see #zrangeByScore(String, double, double)
   * @see #zrangeByScore(String, double, double, int, int)
   * @see #zrangeByScoreWithScores(String, double, double)
   * @see #zrangeByScoreWithScores(String, double, double, int, int)
   * @see #zcount(String, double, double)
   * @param key
   * @param min
   * @param max
   * @return Multi bulk reply specifically a list of elements in the specified score range.
   */
  @Override
  public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
    checkIsInMultiOrPipeline();
    client.zrangeByScoreWithScores(key, min, max);
    return getTupledSet();
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zrangeByScoreWithScores(key, min, max);
    return getTupledSet();
  }

  /**
   * Return the all the elements in the sorted set at key with a score between min and max
   * (including elements with score equal to min or max).
   * <p>
   * The elements having the same score are returned sorted lexicographically as ASCII strings (this
   * follows from a property of Redis sorted sets and does not involve further computation).
   * <p>
   * Using the optional {@link #zrangeByScore(String, double, double, int, int) LIMIT} it's possible
   * to get only a range of the matching elements in an SQL-alike way. Note that if offset is large
   * the commands needs to traverse the list for offset elements and this adds up to the O(M)
   * figure.
   * <p>
   * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
   * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of returning the
   * actual elements in the specified interval, it just returns the number of matching elements.
   * <p>
   * <b>Exclusive intervals and infinity</b>
   * <p>
   * min and max can be -inf and +inf, so that you are not required to know what's the greatest or
   * smallest element in order to take, for instance, elements "up to a given value".
   * <p>
   * Also while the interval is for default closed (inclusive) it's possible to specify open
   * intervals prefixing the score with a "(" character, so for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (1.3 5}
   * <p>
   * Will return all the values with score &gt; 1.3 and &lt;= 5, while for instance:
   * <p>
   * {@code ZRANGEBYSCORE zset (5 (10}
   * <p>
   * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of
   * elements returned by the command, so if M is constant (for instance you always ask for the
   * first ten elements with LIMIT) you can consider it O(log(N))
   * @see #zrangeByScore(String, double, double)
   * @see #zrangeByScore(String, double, double, int, int)
   * @see #zrangeByScoreWithScores(String, double, double)
   * @see #zrangeByScoreWithScores(String, double, double, int, int)
   * @see #zcount(String, double, double)
   * @param key
   * @param min
   * @param max
   * @return Multi bulk reply specifically a list of elements in the specified score range.
   */
  @Override
  public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrangeByScoreWithScores(key, min, max, offset, count);
    return getTupledSet();
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrangeByScoreWithScores(key, min, max, offset, count);
    return getTupledSet();
  }

  private Set<Tuple> getTupledSet() {
    checkIsInMultiOrPipeline();
    List<String> membersWithScores = client.getMultiBulkReply();
    if (membersWithScores == null) {
      return null;
    }
    if (membersWithScores.size() == 0) {
      return Collections.emptySet();
    }
    Set<Tuple> set = new LinkedHashSet<Tuple>(membersWithScores.size() / 2, 1.0f);
    Iterator<String> iterator = membersWithScores.iterator();
    while (iterator.hasNext()) {
      set.add(new Tuple(iterator.next(), Double.valueOf(iterator.next())));
    }
    return set;
  }

  @Override
  public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScore(key, max, min);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScore(key, max, min);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrevrangeByScore(final String key, final double max, final double min,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScore(key, max, min, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScoreWithScores(key, max, min);
    return getTupledSet();
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max,
      final double min, final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return getTupledSet();
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max,
      final String min, final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return getTupledSet();
  }

  @Override
  public Set<String> zrevrangeByScore(final String key, final String max, final String min,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScore(key, max, min, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByScoreWithScores(key, max, min);
    return getTupledSet();
  }

  /**
   * Remove all elements in the sorted set at key with rank between start and end. Start and end are
   * 0-based with rank 0 being the element with the lowest score. Both start and end can be negative
   * numbers, where they indicate offsets starting at the element with the highest rank. For
   * example: -1 is the element with the highest score, -2 the element with the second highest score
   * and so forth.
   * <p>
   * <b>Time complexity:</b> O(log(N))+O(M) with N being the number of elements in the sorted set
   * and M the number of elements removed by the operation
   */
  @Override
  public Long zremrangeByRank(final String key, final long start, final long end) {
    checkIsInMultiOrPipeline();
    client.zremrangeByRank(key, start, end);
    return client.getIntegerReply();
  }

  /**
   * Remove all the elements in the sorted set at key with a score between min and max (including
   * elements with score equal to min or max).
   * <p>
   * <b>Time complexity:</b>
   * <p>
   * O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of
   * elements removed by the operation
   * @param key
   * @param start
   * @param end
   * @return Integer reply, specifically the number of elements removed.
   */
  @Override
  public Long zremrangeByScore(final String key, final double start, final double end) {
    checkIsInMultiOrPipeline();
    client.zremrangeByScore(key, start, end);
    return client.getIntegerReply();
  }

  @Override
  public Long zremrangeByScore(final String key, final String start, final String end) {
    checkIsInMultiOrPipeline();
    client.zremrangeByScore(key, start, end);
    return client.getIntegerReply();
  }

  /**
   * Creates a union or intersection of N sorted sets given by keys k1 through kN, and stores it at
   * dstkey. It is mandatory to provide the number of input keys N, before passing the input keys
   * and the other (optional) arguments.
   * <p>
   * As the terms imply, the {@link #zinterstore(String, String...) ZINTERSTORE} command requires an
   * element to be present in each of the given inputs to be inserted in the result. The
   * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all elements across all
   * inputs.
   * <p>
   * Using the WEIGHTS option, it is possible to add weight to each input sorted set. This means
   * that the score of each element in the sorted set is first multiplied by this weight before
   * being passed to the aggregation. When this option is not given, all weights default to 1.
   * <p>
   * With the AGGREGATE option, it's possible to specify how the results of the union or
   * intersection are aggregated. This option defaults to SUM, where the score of an element is
   * summed across the inputs where it exists. When this option is set to be either MIN or MAX, the
   * resulting set will contain the minimum or maximum score of an element across the inputs where
   * it exists.
   * <p>
   * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the sizes of the input
   * sorted sets, and M being the number of elements in the resulting sorted set
   * @see #zunionstore(String, String...)
   * @see #zunionstore(String, ZParams, String...)
   * @see #zinterstore(String, String...)
   * @see #zinterstore(String, ZParams, String...)
   * @param dstkey
   * @param sets
   * @return Integer reply, specifically the number of elements in the sorted set at dstkey
   */
  @Override
  public Long zunionstore(final String dstkey, final String... sets) {
    checkIsInMultiOrPipeline();
    client.zunionstore(dstkey, sets);
    return client.getIntegerReply();
  }

  /**
   * Creates a union or intersection of N sorted sets given by keys k1 through kN, and stores it at
   * dstkey. It is mandatory to provide the number of input keys N, before passing the input keys
   * and the other (optional) arguments.
   * <p>
   * As the terms imply, the {@link #zinterstore(String, String...) ZINTERSTORE} command requires an
   * element to be present in each of the given inputs to be inserted in the result. The
   * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all elements across all
   * inputs.
   * <p>
   * Using the WEIGHTS option, it is possible to add weight to each input sorted set. This means
   * that the score of each element in the sorted set is first multiplied by this weight before
   * being passed to the aggregation. When this option is not given, all weights default to 1.
   * <p>
   * With the AGGREGATE option, it's possible to specify how the results of the union or
   * intersection are aggregated. This option defaults to SUM, where the score of an element is
   * summed across the inputs where it exists. When this option is set to be either MIN or MAX, the
   * resulting set will contain the minimum or maximum score of an element across the inputs where
   * it exists.
   * <p>
   * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the sizes of the input
   * sorted sets, and M being the number of elements in the resulting sorted set
   * @see #zunionstore(String, String...)
   * @see #zunionstore(String, ZParams, String...)
   * @see #zinterstore(String, String...)
   * @see #zinterstore(String, ZParams, String...)
   * @param dstkey
   * @param sets
   * @param params
   * @return Integer reply, specifically the number of elements in the sorted set at dstkey
   */
  @Override
  public Long zunionstore(final String dstkey, final ZParams params, final String... sets) {
    checkIsInMultiOrPipeline();
    client.zunionstore(dstkey, params, sets);
    return client.getIntegerReply();
  }

  /**
   * Creates a union or intersection of N sorted sets given by keys k1 through kN, and stores it at
   * dstkey. It is mandatory to provide the number of input keys N, before passing the input keys
   * and the other (optional) arguments.
   * <p>
   * As the terms imply, the {@link #zinterstore(String, String...) ZINTERSTORE} command requires an
   * element to be present in each of the given inputs to be inserted in the result. The
   * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all elements across all
   * inputs.
   * <p>
   * Using the WEIGHTS option, it is possible to add weight to each input sorted set. This means
   * that the score of each element in the sorted set is first multiplied by this weight before
   * being passed to the aggregation. When this option is not given, all weights default to 1.
   * <p>
   * With the AGGREGATE option, it's possible to specify how the results of the union or
   * intersection are aggregated. This option defaults to SUM, where the score of an element is
   * summed across the inputs where it exists. When this option is set to be either MIN or MAX, the
   * resulting set will contain the minimum or maximum score of an element across the inputs where
   * it exists.
   * <p>
   * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the sizes of the input
   * sorted sets, and M being the number of elements in the resulting sorted set
   * @see #zunionstore(String, String...)
   * @see #zunionstore(String, ZParams, String...)
   * @see #zinterstore(String, String...)
   * @see #zinterstore(String, ZParams, String...)
   * @param dstkey
   * @param sets
   * @return Integer reply, specifically the number of elements in the sorted set at dstkey
   */
  @Override
  public Long zinterstore(final String dstkey, final String... sets) {
    checkIsInMultiOrPipeline();
    client.zinterstore(dstkey, sets);
    return client.getIntegerReply();
  }

  /**
   * Creates a union or intersection of N sorted sets given by keys k1 through kN, and stores it at
   * dstkey. It is mandatory to provide the number of input keys N, before passing the input keys
   * and the other (optional) arguments.
   * <p>
   * As the terms imply, the {@link #zinterstore(String, String...) ZINTERSTORE} command requires an
   * element to be present in each of the given inputs to be inserted in the result. The
   * {@link #zunionstore(String, String...) ZUNIONSTORE} command inserts all elements across all
   * inputs.
   * <p>
   * Using the WEIGHTS option, it is possible to add weight to each input sorted set. This means
   * that the score of each element in the sorted set is first multiplied by this weight before
   * being passed to the aggregation. When this option is not given, all weights default to 1.
   * <p>
   * With the AGGREGATE option, it's possible to specify how the results of the union or
   * intersection are aggregated. This option defaults to SUM, where the score of an element is
   * summed across the inputs where it exists. When this option is set to be either MIN or MAX, the
   * resulting set will contain the minimum or maximum score of an element across the inputs where
   * it exists.
   * <p>
   * <b>Time complexity:</b> O(N) + O(M log(M)) with N being the sum of the sizes of the input
   * sorted sets, and M being the number of elements in the resulting sorted set
   * @see #zunionstore(String, String...)
   * @see #zunionstore(String, ZParams, String...)
   * @see #zinterstore(String, String...)
   * @see #zinterstore(String, ZParams, String...)
   * @param dstkey
   * @param sets
   * @param params
   * @return Integer reply, specifically the number of elements in the sorted set at dstkey
   */
  @Override
  public Long zinterstore(final String dstkey, final ZParams params, final String... sets) {
    checkIsInMultiOrPipeline();
    client.zinterstore(dstkey, params, sets);
    return client.getIntegerReply();
  }

  @Override
  public Long zlexcount(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zlexcount(key, min, max);
    return client.getIntegerReply();
  }

  @Override
  public Set<String> zrangeByLex(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zrangeByLex(key, min, max);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrangeByLex(final String key, final String min, final String max,
      final int offset, final int count) {
    checkIsInMultiOrPipeline();
    client.zrangeByLex(key, min, max, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByLex(key, max, min);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
    checkIsInMultiOrPipeline();
    client.zrevrangeByLex(key, max, min, offset, count);
    final List<String> members = client.getMultiBulkReply();
    if (members == null) {
      return null;
    }
    return SetFromList.of(members);
  }

  @Override
  public Long zremrangeByLex(final String key, final String min, final String max) {
    checkIsInMultiOrPipeline();
    client.zremrangeByLex(key, min, max);
    return client.getIntegerReply();
  }

  @Override
  public Long strlen(final String key) {
    client.strlen(key);
    return client.getIntegerReply();
  }

  @Override
  public Long lpushx(final String key, final String... string) {
    client.lpushx(key, string);
    return client.getIntegerReply();
  }

  /**
   * Undo a {@link #expire(String, int) expire} at turning the expire key into a normal key.
   * <p>
   * Time complexity: O(1)
   * @param key
   * @return Integer reply, specifically: 1: the key is now persist. 0: the key is not persist (only
   *         happens when key not set).
   */
  @Override
  public Long persist(final String key) {
    client.persist(key);
    return client.getIntegerReply();
  }

  /**
   * 将值value 插入到列表key 的表尾，当且仅当key 存在并且是一个列表。和RPUSH 命令相反，当key 不存在时，RPUSHX 命令什么也不做。
   * 返回值： RPUSHX 命令执行之后，表的长度。
 * @date 2015-12-21 上午10:09:24
 * @param key
 * @param string
 * @return
 * @see redis.clients.jedis.commands.JedisCommands#rpushx(java.lang.String, java.lang.String[])
 */
@Override
  public Long rpushx(final String key, final String... string) {
    client.rpushx(key, string);
    return client.getIntegerReply();
  }

  @Override
  public String echo(final String string) {
    client.echo(string);
    return client.getBulkReply();
  }

/**
 * 将值value 插入到列表key 当中，位于值pivot 之前或之后。当pivot 不存在于列表key 时，不执行任何操作。
 * 当key 不存在时，key 被视为空列表，不执行任何操作。如果key 不是列表类型，返回一个错误。可用版本： >= 2.2.0
 * 时间复杂度: O(N)，N 为寻找pivot 过程中经过的元素数量。
 * 返回值:如果命令执行成功，返回插入操作完成之后，列表的长度。如果没有找到pivot ，返回-1 。如果key 不存在或为空列表，返回0 。
 * note:当 列表中的元素有重复是 只会插入到离表头最近的一个pivot 之前或者之后
 * @date 2015-12-18 下午4:44:12
 * @param key
 * @param where
 * @param pivot
 * @param value
 * @return
 * @see redis.clients.jedis.commands.JedisCommands#linsert(java.lang.String, redis.clients.jedis.BinaryClient.LIST_POSITION, java.lang.String, java.lang.String)
 */
@Override
  public Long linsert(final String key, final LIST_POSITION where, final String pivot,
      final String value) {
    client.linsert(key, where, pivot, value);
    return client.getIntegerReply();
  }

  /**
   * Pop a value from a list, push it to another list and return it; or block until one is available
   * @param source
   * @param destination
   * @param timeout
   * @return the element
   */
  @Override
  public String brpoplpush(String source, String destination, int timeout) {
    client.brpoplpush(source, destination, timeout);
    client.setTimeoutInfinite();
    try {
      return client.getBulkReply();
    } finally {
      client.rollbackTimeout();
    }
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key
   * @param key
   * @param offset
   * @param value
   * @return
   */
  @Override
  public Boolean setbit(String key, long offset, boolean value) {
    client.setbit(key, offset, value);
    return client.getIntegerReply() == 1;
  }

  @Override
  public Boolean setbit(String key, long offset, String value) {
    client.setbit(key, offset, value);
    return client.getIntegerReply() == 1;
  }

  /**
   * Returns the bit value at offset in the string value stored at key
   * @param key
   * @param offset
   * @return
   */
  @Override
  public Boolean getbit(String key, long offset) {
    client.getbit(key, offset);
    return client.getIntegerReply() == 1;
  }

 /**
  * 用value 参数覆写(overwrite) 给定key 所储存的字符串值，从偏移量offset 开始。不存在的key 当作空白字符串处理。SETRANGE 命令会确保字符串足够长以便将value 
  * 设置在指定的偏移量上，如果给定key 原来储存的字符串长度比偏移量小(比如字符串只有5 个字符长，但你设置的offset 是10 )，那么原字符和偏移量之间的空白将用零字节(zerobytes, "\x00" ) 来填充。
  * 注意你能使用的最大偏移量是2^29-1(536870911) ，因为Redis 字符串的大小被限制在512 兆(megabytes)以内。如果你需要使用比这更大的空间，你可以使用多个key 。
  *Warning: 当生成一个很长的字符串时，Redis 需要分配内存空间，该操作有时候可能会造成服务器阻塞(block)。在2010 年的Macbook Pro 上，
  *设置偏移量为536870911(512MB 内存分配)，耗费约300 毫秒，设置偏移量为134217728(128MB 内存分配)，耗费约80 毫秒，设置偏移量33554432(32MB 内存分配)，耗费约30 毫秒，
  *设置偏移量为8388608(8MB 内存分配)，耗费约8 毫秒。注意若首次内存分配成功之后，再对同一个key 调用SETRANGE 操作，无须再重新内存。
  *可用版本： >= 2.2.0
  *时间复杂度：对小(small) 的字符串，平摊复杂度O(1)。(关于什么字符串是” 小” 的，请参考APPEND 命令)否则为O(M)，M 为value 参数的长度。
  *返回值： 被SETRANGE 修改之后，字符串的长度。
 * @date 2015-12-17 下午12:59:21
 * @param key
 * @param offset
 * @param value
 * @return
 * @see redis.clients.jedis.commands.JedisCommands#setrange(java.lang.String, long, java.lang.String)
 *
 */
@Override
  public Long setrange(String key, long offset, String value) {
    client.setrange(key, offset, value);
    return client.getIntegerReply();
  }

/**
 * 返回key 中字符串值的子字符串，字符串的截取范围由start 和end 两个偏移量决定(包括start 和end在内)。
 * 负数偏移量表示从字符串最后开始计数，-1 表示最后一个字符，-2 表示倒数第二个，以此类推。GETRANGE 通过保证子字符串的值域(range) 
 * 不超过实际字符串的值域来处理超出范围的值域请求。
 * Note: 在<= 2.0 的版本里，GETRANGE 被叫作SUBSTR
 * 可用版本： >= 2.4.0
 * @date 2015-12-17 上午11:58:33
 * @param key
 * @param startOffset
 * @param endOffset
 * @return
 * @see redis.clients.jedis.commands.JedisCommands#getrange(java.lang.String, long, long)
 * 返回值： 截取得出的子字符串
 */
@Override
  public String getrange(String key, long startOffset, long endOffset) {
    client.getrange(key, startOffset, endOffset);
    return client.getBulkReply();
  }

  @Override
  public Long bitpos(final String key, final boolean value) {
    return bitpos(key, value, new BitPosParams());
  }

  @Override
  public Long bitpos(final String key, final boolean value, final BitPosParams params) {
    client.bitpos(key, value, params);
    return client.getIntegerReply();
  }

  /**
   * Retrieve the configuration of a running Redis server. Not all the configuration parameters are
   * supported.
   * <p>
   * CONFIG GET returns the current configuration parameters. This sub command only accepts a single
   * argument, that is glob style pattern. All the configuration parameters matching this parameter
   * are reported as a list of key-value pairs.
   * <p>
   * <b>Example:</b>
   * 
   * <pre>
   * $ redis-cli config get '*'
   * 1. "dbfilename"
   * 2. "dump.rdb"
   * 3. "requirepass"
   * 4. (nil)
   * 5. "masterauth"
   * 6. (nil)
   * 7. "maxmemory"
   * 8. "0\n"
   * 9. "appendfsync"
   * 10. "everysec"
   * 11. "save"
   * 12. "3600 1 300 100 60 10000"
   * 
   * $ redis-cli config get 'm*'
   * 1. "masterauth"
   * 2. (nil)
   * 3. "maxmemory"
   * 4. "0\n"
   * </pre>
   * @param pattern
   * @return Bulk reply.
   */
  @Override
  public List<String> configGet(final String pattern) {
    client.configGet(pattern);
    return client.getMultiBulkReply();
  }

  /**
   * Alter the configuration of a running Redis server. Not all the configuration parameters are
   * supported.
   * <p>
   * The list of configuration parameters supported by CONFIG SET can be obtained issuing a
   * {@link #configGet(String) CONFIG GET *} command.
   * <p>
   * The configuration set using CONFIG SET is immediately loaded by the Redis server that will
   * start acting as specified starting from the next command.
   * <p>
   * <b>Parameters value format</b>
   * <p>
   * The value of the configuration parameter is the same as the one of the same parameter in the
   * Redis configuration file, with the following exceptions:
   * <p>
   * <ul>
   * <li>The save paramter is a list of space-separated integers. Every pair of integers specify the
   * time and number of changes limit to trigger a save. For instance the command CONFIG SET save
   * "3600 10 60 10000" will configure the server to issue a background saving of the RDB file every
   * 3600 seconds if there are at least 10 changes in the dataset, and every 60 seconds if there are
   * at least 10000 changes. To completely disable automatic snapshots just set the parameter as an
   * empty string.
   * <li>All the integer parameters representing memory are returned and accepted only using bytes
   * as unit.
   * </ul>
   * @param parameter
   * @param value
   * @return Status code reply
   */
  @Override
  public String configSet(final String parameter, final String value) {
    client.configSet(parameter, value);
    return client.getStatusCodeReply();
  }

  @Override
  public Object eval(String script, int keyCount, String... params) {
    client.setTimeoutInfinite();
    try {
      client.eval(script, keyCount, params);
      return getEvalResult();
    } finally {
      client.rollbackTimeout();
    }
  }

  @Override
  public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
    client.setTimeoutInfinite();
    try {
      jedisPubSub.proceed(client, channels);
    } finally {
      client.rollbackTimeout();
    }
  }

  @Override
  public Long publish(final String channel, final String message) {
    checkIsInMultiOrPipeline();
    connect();
    client.publish(channel, message);
    return client.getIntegerReply();
  }

  @Override
  public void psubscribe(final JedisPubSub jedisPubSub, final String... patterns) {
    checkIsInMultiOrPipeline();
    client.setTimeoutInfinite();
    try {
      jedisPubSub.proceedWithPatterns(client, patterns);
    } finally {
      client.rollbackTimeout();
    }
  }

  protected static String[] getParams(List<String> keys, List<String> args) {
    int keyCount = keys.size();
    int argCount = args.size();

    String[] params = new String[keyCount + args.size()];

    for (int i = 0; i < keyCount; i++)
      params[i] = keys.get(i);

    for (int i = 0; i < argCount; i++)
      params[keyCount + i] = args.get(i);

    return params;
  }

  @Override
  public Object eval(String script, List<String> keys, List<String> args) {
    return eval(script, keys.size(), getParams(keys, args));
  }

  @Override
  public Object eval(String script) {
    return eval(script, 0);
  }

  @Override
  public Object evalsha(String script) {
    return evalsha(script, 0);
  }

  private Object getEvalResult() {
    return evalResult(client.getOne());
  }

  private Object evalResult(Object result) {
    if (result instanceof byte[]) return SafeEncoder.encode((byte[]) result);

    if (result instanceof List<?>) {
      List<?> list = (List<?>) result;
      List<Object> listResult = new ArrayList<Object>(list.size());
      for (Object bin : list) {
        listResult.add(evalResult(bin));
      }

      return listResult;
    }

    return result;
  }

  @Override
  public Object evalsha(String sha1, List<String> keys, List<String> args) {
    return evalsha(sha1, keys.size(), getParams(keys, args));
  }

  @Override
  public Object evalsha(String sha1, int keyCount, String... params) {
    checkIsInMultiOrPipeline();
    client.evalsha(sha1, keyCount, params);
    return getEvalResult();
  }

  @Override
  public Boolean scriptExists(String sha1) {
    String[] a = new String[1];
    a[0] = sha1;
    return scriptExists(a).get(0);
  }

  @Override
  public List<Boolean> scriptExists(String... sha1) {
    client.scriptExists(sha1);
    List<Long> result = client.getIntegerMultiBulkReply();
    List<Boolean> exists = new ArrayList<Boolean>();

    for (Long value : result)
      exists.add(value == 1);

    return exists;
  }

  @Override
  public String scriptLoad(String script) {
    client.scriptLoad(script);
    return client.getBulkReply();
  }

  @Override
  public List<Slowlog> slowlogGet() {
    client.slowlogGet();
    return Slowlog.from(client.getObjectMultiBulkReply());
  }

  @Override
  public List<Slowlog> slowlogGet(long entries) {
    client.slowlogGet(entries);
    return Slowlog.from(client.getObjectMultiBulkReply());
  }

  @Override
  public Long objectRefcount(String string) {
    client.objectRefcount(string);
    return client.getIntegerReply();
  }

  @Override
  public String objectEncoding(String string) {
    client.objectEncoding(string);
    return client.getBulkReply();
  }

  @Override
  public Long objectIdletime(String string) {
    client.objectIdletime(string);
    return client.getIntegerReply();
  }

  @Override
  public Long bitcount(final String key) {
    client.bitcount(key);
    return client.getIntegerReply();
  }

  @Override
  public Long bitcount(final String key, long start, long end) {
    client.bitcount(key, start, end);
    return client.getIntegerReply();
  }

  @Override
  public Long bitop(BitOP op, final String destKey, String... srcKeys) {
    client.bitop(op, destKey, srcKeys);
    return client.getIntegerReply();
  }

  /**
   * <pre>
   * redis 127.0.0.1:26381&gt; sentinel masters
   * 1)  1) "name"
   *     2) "mymaster"
   *     3) "ip"
   *     4) "127.0.0.1"
   *     5) "port"
   *     6) "6379"
   *     7) "runid"
   *     8) "93d4d4e6e9c06d0eea36e27f31924ac26576081d"
   *     9) "flags"
   *    10) "master"
   *    11) "pending-commands"
   *    12) "0"
   *    13) "last-ok-ping-reply"
   *    14) "423"
   *    15) "last-ping-reply"
   *    16) "423"
   *    17) "info-refresh"
   *    18) "6107"
   *    19) "num-slaves"
   *    20) "1"
   *    21) "num-other-sentinels"
   *    22) "2"
   *    23) "quorum"
   *    24) "2"
   * 
   * </pre>
   * @return
   */
  @Override
  @SuppressWarnings("rawtypes")
  public List<Map<String, String>> sentinelMasters() {
    client.sentinel(Protocol.SENTINEL_MASTERS);
    final List<Object> reply = client.getObjectMultiBulkReply();

    final List<Map<String, String>> masters = new ArrayList<Map<String, String>>();
    for (Object obj : reply) {
      masters.add(BuilderFactory.STRING_MAP.build((List) obj));
    }
    return masters;
  }

  /**
   * <pre>
   * redis 127.0.0.1:26381&gt; sentinel get-master-addr-by-name mymaster
   * 1) "127.0.0.1"
   * 2) "6379"
   * </pre>
   * @param masterName
   * @return two elements list of strings : host and port.
   */
  @Override
  public List<String> sentinelGetMasterAddrByName(String masterName) {
    client.sentinel(Protocol.SENTINEL_GET_MASTER_ADDR_BY_NAME, masterName);
    final List<Object> reply = client.getObjectMultiBulkReply();
    return BuilderFactory.STRING_LIST.build(reply);
  }

  /**
   * <pre>
   * redis 127.0.0.1:26381&gt; sentinel reset mymaster
   * (integer) 1
   * </pre>
   * @param pattern
   * @return
   */
  @Override
  public Long sentinelReset(String pattern) {
    client.sentinel(Protocol.SENTINEL_RESET, pattern);
    return client.getIntegerReply();
  }

  /**
   * <pre>
   * redis 127.0.0.1:26381&gt; sentinel slaves mymaster
   * 1)  1) "name"
   *     2) "127.0.0.1:6380"
   *     3) "ip"
   *     4) "127.0.0.1"
   *     5) "port"
   *     6) "6380"
   *     7) "runid"
   *     8) "d7f6c0ca7572df9d2f33713df0dbf8c72da7c039"
   *     9) "flags"
   *    10) "slave"
   *    11) "pending-commands"
   *    12) "0"
   *    13) "last-ok-ping-reply"
   *    14) "47"
   *    15) "last-ping-reply"
   *    16) "47"
   *    17) "info-refresh"
   *    18) "657"
   *    19) "master-link-down-time"
   *    20) "0"
   *    21) "master-link-status"
   *    22) "ok"
   *    23) "master-host"
   *    24) "localhost"
   *    25) "master-port"
   *    26) "6379"
   *    27) "slave-priority"
   *    28) "100"
   * </pre>
   * @param masterName
   * @return
   */
  @Override
  @SuppressWarnings("rawtypes")
  public List<Map<String, String>> sentinelSlaves(String masterName) {
    client.sentinel(Protocol.SENTINEL_SLAVES, masterName);
    final List<Object> reply = client.getObjectMultiBulkReply();

    final List<Map<String, String>> slaves = new ArrayList<Map<String, String>>();
    for (Object obj : reply) {
      slaves.add(BuilderFactory.STRING_MAP.build((List) obj));
    }
    return slaves;
  }

  @Override
  public String sentinelFailover(String masterName) {
    client.sentinel(Protocol.SENTINEL_FAILOVER, masterName);
    return client.getStatusCodeReply();
  }

  @Override
  public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
    client.sentinel(Protocol.SENTINEL_MONITOR, masterName, ip, String.valueOf(port),
      String.valueOf(quorum));
    return client.getStatusCodeReply();
  }

  @Override
  public String sentinelRemove(String masterName) {
    client.sentinel(Protocol.SENTINEL_REMOVE, masterName);
    return client.getStatusCodeReply();
  }

  @Override
  public String sentinelSet(String masterName, Map<String, String> parameterMap) {
    int index = 0;
    int paramsLength = parameterMap.size() * 2 + 2;
    String[] params = new String[paramsLength];

    params[index++] = Protocol.SENTINEL_SET;
    params[index++] = masterName;
    for (Entry<String, String> entry : parameterMap.entrySet()) {
      params[index++] = entry.getKey();
      params[index++] = entry.getValue();
    }

    client.sentinel(params);
    return client.getStatusCodeReply();
  }

  public byte[] dump(final String key) {
    checkIsInMultiOrPipeline();
    client.dump(key);
    return client.getBinaryBulkReply();
  }

  public String restore(final String key, final int ttl, final byte[] serializedValue) {
    checkIsInMultiOrPipeline();
    client.restore(key, ttl, serializedValue);
    return client.getStatusCodeReply();
  }

  @Override
  public Long pexpire(final String key, final long milliseconds) {
    checkIsInMultiOrPipeline();
    client.pexpire(key, milliseconds);
    return client.getIntegerReply();
  }

  @Override
  public Long pexpireAt(final String key, final long millisecondsTimestamp) {
    checkIsInMultiOrPipeline();
    client.pexpireAt(key, millisecondsTimestamp);
    return client.getIntegerReply();
  }

  @Override
  public Long pttl(final String key) {
    checkIsInMultiOrPipeline();
    client.pttl(key);
    return client.getIntegerReply();
  }

  /**
   * 这个命令和SETEX 命令相似，但它以毫秒为单位设置key 的生存时间，而不是像SETEX 命令那样，以秒为单位。可用版本： >= 2.6.0时间复杂度： O(1)返回值： 设置成功时返回OK 。
   * Note: 因为SET 命令可以通过参数来实现和SETNX 、SETEX 和PSETEX 三个命令的效果，所以将来的Redis 版本可能会废弃并最终移除SETNX 、SETEX 和PSETEX 这三个命令
   * PSETEX works exactly like {@link #setex(String, int, String)} with the sole difference that the
   * expire time is specified in milliseconds instead of seconds. Time complexity: O(1)
   * @param key
   * @param milliseconds
   * @param value
   * @return Status code reply
   */

  @Override
  public String psetex(final String key, final long milliseconds, final String value) {
    checkIsInMultiOrPipeline();
    client.psetex(key, milliseconds, value);
    return client.getStatusCodeReply();
  }

  public String clientKill(final String client) {
    checkIsInMultiOrPipeline();
    this.client.clientKill(client);
    return this.client.getStatusCodeReply();
  }

  public String clientSetname(final String name) {
    checkIsInMultiOrPipeline();
    client.clientSetname(name);
    return client.getStatusCodeReply();
  }

  public String migrate(final String host, final int port, final String key,
      final int destinationDb, final int timeout) {
    checkIsInMultiOrPipeline();
    client.migrate(host, port, key, destinationDb, timeout);
    return client.getStatusCodeReply();
  }

/**
 * SCAN 命令用于迭代当前数据库中的数据库键。
 * 最简单的迭代，迭代数据库中的所有key 初始游标为 0 ，返回结果是封装好了的 ScanResult对象 ， 包含游标和 key 结果集
 * 不会出现像KEYS 命令、SMEMBERS 命令带来的问题——当KEYS 命令被用于处理一个大的数据库时，又或者SMEMBERS 命令被用于处理一个大的集合键时，它们可能会阻塞服务器达数秒之久
 * @date 2015-12-17 下午1:52:57
 * @param cursor
 * @return
 * @see redis.clients.jedis.commands.MultiKeyCommands#scan(java.lang.String)
 * scanResult.getCursor();// 返回的游标  继续用此 游标进行遍历，直到 返回 0 说明遍历完成。
 * scanResult.getResult();// 返回的结果集 里面包含返回的所有的key 
 */
@Override
  public ScanResult<String> scan(final String cursor) {
    return scan(cursor, new ScanParams());
  }

/**
 * count 参数：
 * 虽然增量式迭代命令不保证每次迭代所返回的元素数量，但我们可以使用COUNT 选项，对命令的行为进行一定程度上的调整。基本上，COUNT 选项的作用就是让用户告知迭代命令，
 * 在每次迭代中应该从数据集里返回多少元素。虽然COUNT 选项只是对增量式迭代命令的一种提示（hint），但是在大多数情况下，这种提示都是有效的。COUNT 参数的默认值为10 。
 * 在迭代一个足够大的、由哈希表实现的数据库、集合键、哈希键或者有序集合键时，如果用户没有使用MATCH 选项，那么命令返回的元素数量通常和COUNT 选项指定的一样，或者比COUNT 选项指定的数量稍多一些。 
 * 在迭代一个编码为整数集合（intset，一个只由整数值构成的小集合）、或者编码为压缩列表（ziplist，由不同值构成的一个小哈希或者一个小有序集合）时，增量式
 * Note: 并非每次迭代都要使用相同的COUNT 值。用户可以在每次迭代中按自己的需要随意改变COUNT 值，只要记得将上次迭代返回的游标用到下次迭代里面就可以了。
 * match:
 * 需要注意的是，对元素的模式匹配工作是在命令从数据集中取出元素之后，向客户端返回元素之前的这段时间内进行的，所以如果被迭代的数据集中只有少量元素和模式相匹配，那么迭代命令或许会在多次执行中都不返回任何元素。
 * 和KEYS 命令一样，增量式迭代命令也可以通过提供一个glob 风格的模式参数，让命令只返回和给定模式相匹配的元素，这一点可以通过在执行增量式迭代命令时，通过给定MATCH <pattern> 参数来实现
 * @date 2015-12-17 下午2:02:44
 * @param cursor
 * @param params
 * @return
 * @see redis.clients.jedis.commands.MultiKeyCommands#scan(java.lang.String, redis.clients.jedis.ScanParams)
 */
@Override
  public ScanResult<String> scan(final String cursor, final ScanParams params) {
    checkIsInMultiOrPipeline();
    client.scan(cursor, params);
    List<Object> result = client.getObjectMultiBulkReply();
    String newcursor = new String((byte[]) result.get(0));
    List<String> results = new ArrayList<String>();
    List<byte[]> rawResults = (List<byte[]>) result.get(1);
    for (byte[] bs : rawResults) {
      results.add(SafeEncoder.encode(bs));
    }
    return new ScanResult<String>(newcursor, results);
  }

  @Override
  public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor) {
    return hscan(key, cursor, new ScanParams());
  }

  @Override
  public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor,
      final ScanParams params) {
    checkIsInMultiOrPipeline();
    client.hscan(key, cursor, params);
    List<Object> result = client.getObjectMultiBulkReply();
    String newcursor = new String((byte[]) result.get(0));
    List<Map.Entry<String, String>> results = new ArrayList<Map.Entry<String, String>>();
    List<byte[]> rawResults = (List<byte[]>) result.get(1);
    Iterator<byte[]> iterator = rawResults.iterator();
    while (iterator.hasNext()) {
      results.add(new AbstractMap.SimpleEntry<String, String>(SafeEncoder.encode(iterator.next()),
          SafeEncoder.encode(iterator.next())));
    }
    return new ScanResult<Map.Entry<String, String>>(newcursor, results);
  }

  @Override
  public ScanResult<String> sscan(final String key, final String cursor) {
    return sscan(key, cursor, new ScanParams());
  }

  @Override
  public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
    checkIsInMultiOrPipeline();
    client.sscan(key, cursor, params);
    List<Object> result = client.getObjectMultiBulkReply();
    String newcursor = new String((byte[]) result.get(0));
    List<String> results = new ArrayList<String>();
    List<byte[]> rawResults = (List<byte[]>) result.get(1);
    for (byte[] bs : rawResults) {
      results.add(SafeEncoder.encode(bs));
    }
    return new ScanResult<String>(newcursor, results);
  }

  @Override
  public ScanResult<Tuple> zscan(final String key, final String cursor) {
    return zscan(key, cursor, new ScanParams());
  }

  @Override
  public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams params) {
    checkIsInMultiOrPipeline();
    client.zscan(key, cursor, params);
    List<Object> result = client.getObjectMultiBulkReply();
    String newcursor = new String((byte[]) result.get(0));
    List<Tuple> results = new ArrayList<Tuple>();
    List<byte[]> rawResults = (List<byte[]>) result.get(1);
    Iterator<byte[]> iterator = rawResults.iterator();
    while (iterator.hasNext()) {
      results.add(new Tuple(SafeEncoder.encode(iterator.next()), Double.valueOf(SafeEncoder
          .encode(iterator.next()))));
    }
    return new ScanResult<Tuple>(newcursor, results);
  }

  @Override
  public String clusterNodes() {
    checkIsInMultiOrPipeline();
    client.clusterNodes();
    return client.getBulkReply();
  }

  @Override
  public String readonly() {
    client.readonly();
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterMeet(final String ip, final int port) {
    checkIsInMultiOrPipeline();
    client.clusterMeet(ip, port);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterReset(final Reset resetType) {
    checkIsInMultiOrPipeline();
    client.clusterReset(resetType);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterAddSlots(final int... slots) {
    checkIsInMultiOrPipeline();
    client.clusterAddSlots(slots);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterDelSlots(final int... slots) {
    checkIsInMultiOrPipeline();
    client.clusterDelSlots(slots);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterInfo() {
    checkIsInMultiOrPipeline();
    client.clusterInfo();
    return client.getStatusCodeReply();
  }

  @Override
  public List<String> clusterGetKeysInSlot(final int slot, final int count) {
    checkIsInMultiOrPipeline();
    client.clusterGetKeysInSlot(slot, count);
    return client.getMultiBulkReply();
  }

  @Override
  public String clusterSetSlotNode(final int slot, final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterSetSlotNode(slot, nodeId);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterSetSlotMigrating(final int slot, final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterSetSlotMigrating(slot, nodeId);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterSetSlotImporting(final int slot, final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterSetSlotImporting(slot, nodeId);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterSetSlotStable(final int slot) {
    checkIsInMultiOrPipeline();
    client.clusterSetSlotStable(slot);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterForget(final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterForget(nodeId);
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterFlushSlots() {
    checkIsInMultiOrPipeline();
    client.clusterFlushSlots();
    return client.getStatusCodeReply();
  }

  @Override
  public Long clusterKeySlot(final String key) {
    checkIsInMultiOrPipeline();
    client.clusterKeySlot(key);
    return client.getIntegerReply();
  }

  @Override
  public Long clusterCountKeysInSlot(final int slot) {
    checkIsInMultiOrPipeline();
    client.clusterCountKeysInSlot(slot);
    return client.getIntegerReply();
  }

  @Override
  public String clusterSaveConfig() {
    checkIsInMultiOrPipeline();
    client.clusterSaveConfig();
    return client.getStatusCodeReply();
  }

  @Override
  public String clusterReplicate(final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterReplicate(nodeId);
    return client.getStatusCodeReply();
  }

  @Override
  public List<String> clusterSlaves(final String nodeId) {
    checkIsInMultiOrPipeline();
    client.clusterSlaves(nodeId);
    return client.getMultiBulkReply();
  }

  @Override
  public String clusterFailover() {
    checkIsInMultiOrPipeline();
    client.clusterFailover();
    return client.getStatusCodeReply();
  }

  @Override
  public List<Object> clusterSlots() {
    checkIsInMultiOrPipeline();
    client.clusterSlots();
    return client.getObjectMultiBulkReply();
  }

  public String asking() {
    checkIsInMultiOrPipeline();
    client.asking();
    return client.getStatusCodeReply();
  }

  public List<String> pubsubChannels(String pattern) {
    checkIsInMultiOrPipeline();
    client.pubsubChannels(pattern);
    return client.getMultiBulkReply();
  }

  public Long pubsubNumPat() {
    checkIsInMultiOrPipeline();
    client.pubsubNumPat();
    return client.getIntegerReply();
  }

  public Map<String, String> pubsubNumSub(String... channels) {
    checkIsInMultiOrPipeline();
    client.pubsubNumSub(channels);
    return BuilderFactory.PUBSUB_NUMSUB_MAP.build(client.getBinaryMultiBulkReply());
  }

  @Override
  public void close() {
    if (dataSource != null) {
      if (client.isBroken()) {
        this.dataSource.returnBrokenResource(this);
      } else {
        this.dataSource.returnResource(this);
      }
    } else {
      client.close();
    }
  }

  public void setDataSource(JedisPoolAbstract jedisPool) {
    this.dataSource = jedisPool;
  }

  @Override
  public Long pfadd(final String key, final String... elements) {
    checkIsInMultiOrPipeline();
    client.pfadd(key, elements);
    return client.getIntegerReply();
  }

  @Override
  public long pfcount(final String key) {
    checkIsInMultiOrPipeline();
    client.pfcount(key);
    return client.getIntegerReply();
  }

  @Override
  public long pfcount(String... keys) {
    checkIsInMultiOrPipeline();
    client.pfcount(keys);
    return client.getIntegerReply();
  }

  @Override
  public String pfmerge(final String destkey, final String... sourcekeys) {
    checkIsInMultiOrPipeline();
    client.pfmerge(destkey, sourcekeys);
    return client.getStatusCodeReply();
  }

  @Override
  public List<String> blpop(int timeout, String key) {
    return blpop(key, String.valueOf(timeout));
  }

  @Override
  public List<String> brpop(int timeout, String key) {
    return brpop(key, String.valueOf(timeout));
  }

  @Override
  public Long geoadd(String key, double longitude, double latitude, String member) {
    checkIsInMultiOrPipeline();
    client.geoadd(key, longitude, latitude, member);
    return client.getIntegerReply();
  }

  @Override
  public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
    checkIsInMultiOrPipeline();
    client.geoadd(key, memberCoordinateMap);
    return client.getIntegerReply();
  }

  @Override
  public Double geodist(String key, String member1, String member2) {
    checkIsInMultiOrPipeline();
    client.geodist(key, member1, member2);
    String dval = client.getBulkReply();
    return (dval != null ? new Double(dval) : null);
  }

  @Override
  public Double geodist(String key, String member1, String member2, GeoUnit unit) {
    checkIsInMultiOrPipeline();
    client.geodist(key, member1, member2, unit);
    String dval = client.getBulkReply();
    return (dval != null ? new Double(dval) : null);
  }

  @Override
  public List<String> geohash(String key, String... members) {
    checkIsInMultiOrPipeline();
    client.geohash(key, members);
    return client.getMultiBulkReply();
  }

  @Override
  public List<GeoCoordinate> geopos(String key, String... members) {
    checkIsInMultiOrPipeline();
    client.geopos(key, members);
    return BuilderFactory.GEO_COORDINATE_LIST.build(client.getObjectMultiBulkReply());
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    checkIsInMultiOrPipeline();
    client.georadius(key, longitude, latitude, radius, unit);
    return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    checkIsInMultiOrPipeline();
    client.georadius(key, longitude, latitude, radius, unit, param);
    return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit) {
    checkIsInMultiOrPipeline();
    client.georadiusByMember(key, member, radius, unit);
    return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    checkIsInMultiOrPipeline();
    client.georadiusByMember(key, member, radius, unit, param);
    return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
  }
}
