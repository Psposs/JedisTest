package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.IOUtils;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

public class Connection implements Closeable {

  private static final byte[][] EMPTY_ARGS = new byte[0][];

  private String host = Protocol.DEFAULT_HOST;
  private int port = Protocol.DEFAULT_PORT;
  private Socket socket;
  private RedisOutputStream outputStream;
  private RedisInputStream inputStream;
  private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
  private int soTimeout = Protocol.DEFAULT_TIMEOUT;
  private boolean broken = false;

  public Connection() {
  }

  public Connection(final String host) {
    this.host = host;
  }

  public Connection(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  public Socket getSocket() {
    return socket;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public void setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public void setSoTimeout(int soTimeout) {
    this.soTimeout = soTimeout;
  }

  public void setTimeoutInfinite() {
    try {
      if (!isConnected()) {
        connect();
      }
      socket.setSoTimeout(0);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void rollbackTimeout() {
    try {
      socket.setSoTimeout(soTimeout);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  protected Connection sendCommand(final ProtocolCommand cmd, final String... args) {
    final byte[][] bargs = new byte[args.length][];
    for (int i = 0; i < args.length; i++) {
      bargs[i] = SafeEncoder.encode(args[i]);
    }
    return sendCommand(cmd, bargs);
  }

  protected Connection sendCommand(final ProtocolCommand cmd) {
    return sendCommand(cmd, EMPTY_ARGS);
  }

  protected Connection sendCommand(final ProtocolCommand cmd, final byte[]... args) {
    try {
      connect();
      Protocol.sendCommand(outputStream, cmd, args);
      return this;
    } catch (JedisConnectionException ex) {
      /*
       * When client send request which formed by invalid protocol, Redis send back error message
       * before close connection. We try to read it to provide reason of failure.
       */
      try {
        String errorMessage = Protocol.readErrorLineIfPossible(inputStream);
        if (errorMessage != null && errorMessage.length() > 0) {
          ex = new JedisConnectionException(errorMessage, ex.getCause());
        }
      } catch (Exception e) {
        /*
         * Catch any IOException or JedisConnectionException occurred from InputStream#read and just
         * ignore. This approach is safe because reading error message is optional and connection
         * will eventually be closed.
         */
      }
      // Any other exceptions related to connection?
      broken = true;
      throw ex;
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public void connect() {
    if (!isConnected()) {
      try {
        socket = new Socket();
        // ->@wjw_add
        socket.setReuseAddress(true);// 测试是否启用SO_REUSEADDR； 一般来说，一个端口释放后会等待两分钟之后才能再被使用，SO_REUSEADDR是让端口释放后立即就可以被再次使用
        socket.setKeepAlive(true); // Will monitor the TCP connection is  测试是否启用 SO_KEEPALIVE。
        //SO_KEEPALIVE 保持连接检测对方主机是否崩溃，避免（服务器）永远阻塞于TCP连接的输入。设置该选项后，如果2小时内在此套接口的任一方向都没有数据交换，TCP就自 动给对方 发一个保持存活探测分节(keepalive probe)。这是一个对方必须响应的TCP分节.它会导致以下三种情况：对方接收一切正常：以期望的ACK响应
        // valid
        socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to 测试是否启用 TCP_NODELAY  作用就是禁用 Nagle’s Algorithm
        // ensure timely delivery of data
        socket.setSoLinger(true, 0); // Control calls close () method, 返回 SO_LINGER 的设置
        // the underlying socket is closed
        // immediately
        // <-@wjw_add

        socket.connect(new InetSocketAddress(host, port), connectionTimeout);
        socket.setSoTimeout(soTimeout);
        outputStream = new RedisOutputStream(socket.getOutputStream());
        inputStream = new RedisInputStream(socket.getInputStream());
      } catch (IOException ex) {
        broken = true;
        throw new JedisConnectionException(ex);
      }
    }
  }

  @Override
  public void close() {
    disconnect();
  }

  public void disconnect() {
    if (isConnected()) {
      try {
        outputStream.flush();
        socket.close();
      } catch (IOException ex) {
        broken = true;
        throw new JedisConnectionException(ex);
      } finally {
        IOUtils.closeQuietly(socket);
      }
    }
  }

/**
 * 判断socket 连接是否正常  判断输入 输出流是否正常
 * @author jackson
 * @date 2015-12-16 上午9:14:47
 * @return
 * @return boolean
 */
  
public boolean isConnected() {
    return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
        && !socket.isInputShutdown() && !socket.isOutputShutdown();
/*
 * isClosed(): 如果Socket已经连接到远程主机, 并且还没有关闭, 则返回true , 否则返回false .
 *	isConnected(): 如果Socket曾经连接到远程主机, 则返回true , 否则返回false .
 *	isBound(): 如果Socket已经与一个本地端口绑定, 则返回true , 否则返回false .
 *虽然在大多数的时候可以直接使用Socket类或输入输出流的close方法关闭网络连接，但有时我们只希望关闭OutputStream或InputStream，
 *而在关闭输入输出流的同时，并不关闭网络连接。这就需要用到Socket类的另外两个方法：shutdownInput和shutdownOutput，
 *这两个方法只关闭相应的输入、输出流，而它们并没有同时关闭网络连接的功能。和isClosed、isConnected方法一样，
 *Socket类也提供了两个方法来判断Socket对象的输入、输出流是否被关闭，这两个方法是isInputShutdown()和isOutputShutdown()
     * */
  }

  public String getStatusCodeReply() {
    flush();
    final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
    if (null == resp) {
      return null;
    } else {
      return SafeEncoder.encode(resp);
    }
  }

  public String getBulkReply() {
    final byte[] result = getBinaryBulkReply();
    if (null != result) {
      return SafeEncoder.encode(result);
    } else {
      return null;
    }
  }

  public byte[] getBinaryBulkReply() {
    flush();
    return (byte[]) readProtocolWithCheckingBroken();
  }

  public Long getIntegerReply() {
    flush();
    return (Long) readProtocolWithCheckingBroken();
  }

  public List<String> getMultiBulkReply() {
    return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
  }

  @SuppressWarnings("unchecked")
  public List<byte[]> getBinaryMultiBulkReply() {
    flush();
    return (List<byte[]>) readProtocolWithCheckingBroken();
  }

  @SuppressWarnings("unchecked")
  public List<Object> getRawObjectMultiBulkReply() {
    return (List<Object>) readProtocolWithCheckingBroken();
  }

  public List<Object> getObjectMultiBulkReply() {
    flush();
    return getRawObjectMultiBulkReply();
  }

  @SuppressWarnings("unchecked")
  public List<Long> getIntegerMultiBulkReply() {
    flush();
    return (List<Long>) readProtocolWithCheckingBroken();
  }

  public Object getOne() {
    flush();
    return readProtocolWithCheckingBroken();
  }

  public boolean isBroken() {
    return broken;
  }

  /**
   * set 操作完成后 刷新流
 * @author jackson
 * @date 2015-12-16 上午9:25:04
 * @return void
 */
protected void flush() {
    try {
      outputStream.flush();//方法刷新此输出流并强制将所有缓冲的输出字节被写出。刷新的常规协定是，调用它是一个迹象表明，如果以前写的任何字节都被缓冲的输出流的实现，如字节应立即写入到它们的目的地。
    } catch (IOException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  protected Object readProtocolWithCheckingBroken() {
    try {
      return Protocol.read(inputStream);
    } catch (JedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  public List<Object> getMany(final int count) {
    flush();
    final List<Object> responses = new ArrayList<Object>(count);
    for (int i = 0; i < count; i++) {
      try {
        responses.add(readProtocolWithCheckingBroken());
      } catch (JedisDataException e) {
        responses.add(e);
      }
    }
    return responses;
  }
}
