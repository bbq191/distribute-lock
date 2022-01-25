package com.example.distributelock.lock;

import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;

/** @author afu */
@Slf4j
public class RedisLock implements AutoCloseable {

  private final RedisTemplate redisTemplate;
  private final String key;
  private final String value;
  /** 超时时间 单位：秒 */
  private final int expireTime;

  public RedisLock(RedisTemplate redisTemplate, String key, int expireTime) {
    this.redisTemplate = redisTemplate;
    this.key = key;
    this.expireTime = expireTime;
    this.value = UUID.randomUUID().toString();
  }

  /**
   * 获取分布式锁
   *
   * @return
   */
  public boolean getLock() {
    RedisCallback<Boolean> redisCallback =
        connection -> {
          // 设置NX
          RedisStringCommands.SetOption setOption = RedisStringCommands.SetOption.ifAbsent();
          // 设置过期时间
          Expiration expiration = Expiration.seconds(expireTime);
          // 序列化key
          byte[] redisKey = redisTemplate.getKeySerializer().serialize(key);
          // 序列化value
          byte[] redisValue = redisTemplate.getValueSerializer().serialize(value);
          // 执行setnx操作
          assert redisKey != null;
          assert redisValue != null;
          return connection.set(redisKey, redisValue, expiration, setOption);
        };

    // 获取分布式锁
    Boolean lock = (Boolean) redisTemplate.execute(redisCallback);
    return Boolean.TRUE.equals(lock);
  }

  public void unLock() {
    String script =
        "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n"
            + "    return redis.call(\"del\",KEYS[1])\n"
            + "else\n"
            + "    return 0\n"
            + "end";
    RedisScript<Boolean> redisScript = RedisScript.of(script, Boolean.class);
    List<String> keys = List.of(key);

    Boolean result = (Boolean) redisTemplate.execute(redisScript, keys, value);
    log.info("释放锁的结果：" + result);
  }

  @Override
  public void close() throws Exception {
    unLock();
  }
}
