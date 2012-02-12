package redispatterns.queue;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Correctness and benchmarking
 */
public class RedisQueueTest {

  private static final JedisPool JEDIS_POOL = new JedisPool("localhost", 6379);

  @Test
  public void success() {
    Jedis jedis = JEDIS_POOL.getResource();
    jedis.del("test_queue");
    jedis.del("test_queue.error");
    jedis.del("test_queue.done");
    RedisQueue testQueue = new RedisQueue(JEDIS_POOL, "test_queue");
    final String test = UUID.randomUUID().toString();
    try {
      testQueue.push(test.getBytes(Charsets.UTF_8));
      testQueue.pop(new Predicate<byte[]>() {
        @Override
        public boolean apply(@Nullable byte[] bytes) {
          assertEquals(test, new String(bytes, Charsets.UTF_8));
          return true;
        }
      });
      assertTrue(jedis.zrank("test_queue.done", test) != null);
      assertEquals(1, (long) jedis.zcard("test_queue.done"));
      assertEquals(0, (long) jedis.llen("test_queue"));
      assertEquals(0, (long) jedis.llen("test_queue.error"));
    } catch (Exception re) {
      fail("Threw exception: " + re);
    }
  }

  @Test
  public void failure() {
    Jedis jedis = JEDIS_POOL.getResource();
    jedis.del("test_queue");
    jedis.del("test_queue.error");
    jedis.del("test_queue.done");
    RedisQueue testQueue = new RedisQueue(JEDIS_POOL, "test_queue");
    final String test = UUID.randomUUID().toString();
    try {
      testQueue.push(test.getBytes(Charsets.UTF_8));
      testQueue.pop(new Predicate<byte[]>() {
        @Override
        public boolean apply(@Nullable byte[] bytes) {
          return false;
        }
      });
      assertTrue(jedis.zrank("test_queue.done", test) == null);
      assertEquals(0, (long) jedis.zcard("test_queue.done"));
      assertEquals(0, (long) jedis.llen("test_queue"));
      assertEquals(1, (long) jedis.llen("test_queue.error"));
    } catch (Exception re) {
      fail("Threw exception: " + re);
    }
  }

  @Test
  public void benchmark() {
    Jedis jedis = JEDIS_POOL.getResource();
    jedis.del("test_queue");
    jedis.del("test_queue.error");
    jedis.del("test_queue.done");
    RedisQueue testQueue = new RedisQueue(JEDIS_POOL, "test_queue");
    long start = System.currentTimeMillis();
    long end;
    int i = 0;
    for (; i % 1000 != 0 || ((end = System.currentTimeMillis()) - start) < 1000; i++) {
      final String test = String.valueOf(i);
      try {
        testQueue.push(test.getBytes(Charsets.UTF_8));
        testQueue.pop(new Predicate<byte[]>() {
          @Override
          public boolean apply(@Nullable byte[] bytes) {
            assertEquals(test, new String(bytes, Charsets.UTF_8));
            return true;
          }
        });
      } catch (Exception re) {
        fail("Threw exception: " + re);
      }
    }
    System.out.println(i * 1000 / (end - start));
    assertEquals(i, (long) jedis.zcard("test_queue.done"));
    assertEquals(0, (long) jedis.llen("test_queue"));
    assertEquals(0, (long) jedis.llen("test_queue.error"));
  }

}
