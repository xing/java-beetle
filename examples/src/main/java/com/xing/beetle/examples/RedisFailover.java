package com.xing.beetle.examples;

import com.xing.beetle.Client;
import com.xing.beetle.RedisConfiguration;
import com.xing.beetle.examples.util.AsyncStreamCopier;
import com.xing.beetle.examples.util.LoggerOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class RedisFailover {
  private static final Logger log = LoggerFactory.getLogger(RedisFailover.class);

  public static void main(String[] args) throws URISyntaxException {
    new RedisFailover().run("/usr/local/bin/redis-server");
  }

  private void run(String redisProcessPath) throws URISyntaxException {

    final ProcessBuilder pbMaster =
        new ProcessBuilder().command(redisProcessPath, "--port", "6379");
    pbMaster.redirectErrorStream(true);

    final ProcessBuilder pbSlave =
        new ProcessBuilder()
            .command(redisProcessPath, "--port", "6380", "--slaveof", "127.0.0.1", "6379");
    pbSlave.redirectErrorStream(true);

    try {
      final Process redisMaster = pbMaster.start();
      new AsyncStreamCopier(
              redisMaster.getInputStream(),
              new LoggerOutputStream(LoggerFactory.getLogger("redis-master")),
              "redis-master")
          .start();
      final Process redisSlave = pbSlave.start();
      new AsyncStreamCopier(
              redisSlave.getInputStream(),
              new LoggerOutputStream(LoggerFactory.getLogger("redis-slave")),
              "redis-slave")
          .start();

      log.info("Waiting 10 seconds to let the redis servers connect to each other for replication");

      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      final File redisFailoverFile = File.createTempFile("redis", "failover");
      final Client client =
          Client.builder()
              .addBroker(5671)
              .addBroker(5672)
              .setDeduplicationStore(new RedisConfiguration("127.0.0.1", 6379))
              .setRedisFailoverMasterFile(redisFailoverFile.getCanonicalPath())
              .build();

      client.start();

      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      redisMaster.destroy();
      redisSlave.destroy();

    } catch (IOException | InterruptedException e) {
      System.err.println(e);
    }
  }
}
