package redis.clients.util;

import java.io.UnsupportedEncodingException;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * The only reason to have this is to be able to compatible with java 1.5 :(
 */
public class SafeEncoder {
/**
 * 将一维字符串数组转换为二维字节数组
 * @param strs
 * @return
 */
public static byte[][] encodeMany(final String... strs) {
    byte[][] many = new byte[strs.length][];
    for (int i = 0; i < strs.length; i++) {
      many[i] = encode(strs[i]);
    }
    return many;
  }

/**
 * 使用指定的字符集将此 String 编码为 byte 序列，并将结果存储到一个新的 byte 数组中。
 * @param str
 * @return
 */
public static byte[] encode(final String str) {
    try {
      if (str == null) {
        throw new JedisDataException("value sent to redis cannot be null");
      }
      return str.getBytes(Protocol.CHARSET); // 使用指定的字符集将此 String 编码为 byte 序列，并将结果存储到一个新的 byte 数组中。
    } catch (UnsupportedEncodingException e) {
      throw new JedisException(e);
    }
  }

/**
 * 将字节数组转换为字符串
 * @param data
 * @return
 */
public static String encode(final byte[] data) {
    try {
      return new String(data, Protocol.CHARSET);// 通过使用指定的 charset 解码指定的 byte 数组，构造一个新的 String
    } catch (UnsupportedEncodingException e) {
      throw new JedisException(e);
    }
  }
}
