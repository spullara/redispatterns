package redispatterns.queue;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Implements a queue with nofications. If there is nothing on the queue, pop
 * will block until something is pushed if
 */
public class RedisQueue {

  private static final int _24_HOURS = 3600 * 24 * 1000;
  private final JedisPool jedisPool;
  private final byte[] name;
  private final byte[] error;
  private final byte[] done;

  @Inject
  public RedisQueue(JedisPool jedisPool, @Named("redisQueueName") String name) {
    this.jedisPool = jedisPool;
    this.name = name.getBytes(Charsets.UTF_8);
    this.error = (name + ".error").getBytes(Charsets.UTF_8);
    this.done = (name + ".done").getBytes(Charsets.UTF_8);
  }

  /**
   * Push a new entry onto the queue.
   *
   * @param bytes
   */
  public void push(byte[] bytes) {
    Jedis jedis = jedisPool.getResource();
    try {
      jedis.rpush(name, bytes);
    } catch (JedisException e) {
      jedisPool.returnBrokenResource(jedis);
      jedis = null;
      throw e;
    } finally {
      if (jedis != null) {
        jedisPool.returnResource(jedis);
      }
    }
  }

  /**
   * Execute the current one and anything that happens to be sitting
   * in the error queue. Then clean up old entries in the done set.
   * <p/>
   * The predicate should return true if it has completely
   * processed the entry. If you crash during execute, you can still
   * fail to process an entry. This implements at-most-once semantics.
   *
   * Entries that may not have been delivered once will be in the error
   * queue. Since there is no way to do a transaction with the Predicate
   * we can only know that the execution started but may have been
   * incomplete.
   *
   * Values sent to the queue must be unique or will not be processed.
   *
   * @param execute
   */
  public void pop(Predicate<byte[]> execute) {
    Jedis jedis = jedisPool.getResource();
    try {
      // Pop off the main queue and enqueue to the error queue
      byte[] popped = jedis.brpoplpush(name, error, 0);
      // Confirm we haven't processed this one before
      if (jedis.zadd(done, System.currentTimeMillis(), popped) > 0) {
        if (execute.apply(popped)) {
          // Remove from the error queue on success
          jedis.lrem(error, -1, popped);
        } else {
          // Remove from the done set on failure
          jedis.zrem(done, popped);
        }
      }
      jedis.zremrangeByScore(done, 0, System.currentTimeMillis() - _24_HOURS);
    } catch (JedisException e) {
      jedisPool.returnBrokenResource(jedis);
      jedis = null;
      throw e;
    } finally {
      if (jedis != null) {
        jedisPool.returnResource(jedis);
      }
    }
  }
}
