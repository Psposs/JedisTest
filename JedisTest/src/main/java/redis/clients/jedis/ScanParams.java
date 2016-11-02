package redis.clients.jedis;

import static redis.clients.jedis.Protocol.Keyword.COUNT;
import static redis.clients.jedis.Protocol.Keyword.MATCH;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import redis.clients.util.SafeEncoder;

public class ScanParams {
  private List<byte[]> params = new ArrayList<byte[]>();
  public final static String SCAN_POINTER_START = String.valueOf(0);
  public final static byte[] SCAN_POINTER_START_BINARY = SafeEncoder.encode(SCAN_POINTER_START);

  public ScanParams match(final byte[] pattern) {
    params.add(MATCH.raw);
    params.add(pattern);
    return this;
  }

/**
 * scan 命令迭代时的参数，类似正在表达式的作用，模糊查询
 * 和KEYS 命令一样，增量式迭代命令也可以通过提供一个glob 风格的模式参数，让命令只返回和给定模式相匹配的元素，这一点可以通过在执行增量式迭代命令时，通过给定MATCH <pattern> 参数来实现
 * 需要注意的是，对元素的模式匹配工作是在命令从数据集中取出元素之后，向客户端返回元素之前的这段时间内进行的，所以如果被迭代的数据集中只有少量元素和模式相匹配，那么迭代命令或许会在多次执行中都不返回任何元素。
 * @date 2015-12-17 下午1:46:15
 * @param pattern
 * @return
 * @return ScanParams
 */
public ScanParams match(final String pattern) {
    params.add(MATCH.raw);
    params.add(SafeEncoder.encode(pattern));
    return this;
  }

/**
 * scan 命令迭代时的参数，scan命令 默认每次返回十个数据，用此参数可以设置返回的数据量
 * count 参数：
 * 虽然增量式迭代命令不保证每次迭代所返回的元素数量，但我们可以使用COUNT 选项，对命令的行为进行一定程度上的调整。基本上，COUNT 选项的作用就是让用户告知迭代命令，
 * 在每次迭代中应该从数据集里返回多少元素。虽然COUNT 选项只是对增量式迭代命令的一种提示（hint），但是在大多数情况下，这种提示都是有效的。COUNT 参数的默认值为10 。
 * 在迭代一个足够大的、由哈希表实现的数据库、集合键、哈希键或者有序集合键时，如果用户没有使用MATCH 选项，那么命令返回的元素数量通常和COUNT 选项指定的一样，或者比COUNT 选项指定的数量稍多一些。 
 * 在迭代一个编码为整数集合（intset，一个只由整数值构成的小集合）、或者编码为压缩列表（ziplist，由不同值构成的一个小哈希或者一个小有序集合）时，增量式
 * Note: 并非每次迭代都要使用相同的COUNT 值。用户可以在每次迭代中按自己的需要随意改变COUNT 值，只要记得将上次迭代返回的游标用到下次迭代里面就可以了。
 * @date 2015-12-17 下午1:46:55
 * @param count
 * @return
 * @return ScanParams
 */
public ScanParams count(final int count) {
    params.add(COUNT.raw);
    params.add(Protocol.toByteArray(count));
    return this;
  }

  public Collection<byte[]> getParams() {
    return Collections.unmodifiableCollection(params);
  }
}
