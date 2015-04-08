package net.schildmeijer;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.TokenAwarePolicy;

//import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;


public class Storage {

  private static final Logger log = (Logger) LoggerFactory.getLogger(Storage.class);

  private static final RateLimiter limiter = RateLimiter.create(256 * 6, 8, TimeUnit.SECONDS);
  private static final int CONCURRENCY = 256;

  private final Session session;
  private static final String CLUSTER_NAME = "cassandra9086";
  private static final String KEYSPACE = "cassandra9086";
  private static final int CONNECTIONS_PER_HOST = 8;

  private static final String INSERT_INTO_NAME_INDEX = "INSERT INTO name_index(name, id) VALUES(?, ?) IF NOT EXISTS";
  private final PreparedStatement insertName;

  public Storage(final HostAndPort singleton, final String dc) throws UnknownHostException {
    this(Collections.singleton(new InetSocketAddress(singleton.getHostText(), singleton.getPort())), dc);
  }

  private Storage(final Collection<InetSocketAddress> contactPoints, final String dc) {
    log.info("Using Cassandra storage");
    log.info("Using local dc: " + dc);
    log.info("Connecting against: " + contactPoints);

    final Cluster.Builder builder = Cluster.builder()
        .addContactPointsWithPorts(contactPoints)
        .withClusterName(CLUSTER_NAME)
        .withProtocolVersion(ProtocolVersion.V2)
        .withReconnectionPolicy(Policies.defaultReconnectionPolicy())
        .withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE)) // no retries, just log failures
        .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dc)))
        .withCompression(ProtocolOptions.Compression.LZ4)
        .withPoolingOptions(
            new PoolingOptions()
                // (100 default) Only related to the number of connections per host.
                //.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL.LOCAL, 100)

                // (1024 default) Only used for ProtocolVersion.V3
                //.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, 1024)

                // fast fail if no connections are available
                .setPoolTimeoutMillis(0)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, CONNECTIONS_PER_HOST)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, CONNECTIONS_PER_HOST));

    log.info("Connecting to Cassandra cluster");

    session = builder.build().connect(KEYSPACE);
    log.info("Preparing statements");
    insertName = session.prepare(INSERT_INTO_NAME_INDEX);
  }

  public ListenableFuture<Boolean> insert(final String name, final UUID id) {
    final BoundStatement statement = new BoundStatement(insertName).bind(name, ImmutableSet.of( id));
    final ResultSetFuture paxos = session.executeAsync(statement);

    final ListenableFuture<Boolean> result = Futures.transform(paxos, new AsyncFunction<ResultSet, Boolean>() {
      @Override
      public ListenableFuture<Boolean> apply(final ResultSet result) throws Exception {
        final boolean unique = result.wasApplied();
        if (!unique) {
          /**
           * This gets executed when I hit CASSANDRA-9086.
           *
           * Paxos claims that the insert wasn't applied and returns the existing ("colliding")
           * row. The curious part is that id that gets returned is the same id as we tried to insert?
           * Pretty sure we didn't run into a uuid (122 random bits) collision ;)
           */
          final Row existing = result.one();
          final String existingName = existing.getString("name");
          final UUID existingId = Iterables.getOnlyElement(existing.getSet("id", UUID.class));
          log.info(String.format("Name already exists. [name: %s, id: %s] [existing id: %s]", name, id, existingId));
          System.exit(1);
          throw new RuntimeException("Name already exists: " + name);
        }
        return Futures.immediateFuture(true);
      }
    });

    return result;
  }

  private void shutdown() {
    session.getCluster().close();
  }

  private static String randomName() {
    return "name:" + UUID.randomUUID().toString().replace("-", "");
  }

  private static UUID randomId() {
    return UUID.randomUUID();
  }

  public static void main(String[] args) throws UnknownHostException {
    ((Logger) LoggerFactory.getLogger("com.datastax.driver.core")).setLevel(Level.ERROR);
    ((Logger) LoggerFactory.getLogger("com.datastax.driver.core.exceptions.DriverException")).setLevel(Level.ERROR);

    final Storage storage = new Storage(HostAndPort.fromParts("localhost", 9042), "datacenter1");
    final ExecutorService executor = Executors.newWorkStealingPool(CONCURRENCY);
    final AtomicBoolean flag = new AtomicBoolean(true);
    final AtomicInteger outstanding = new AtomicInteger(0);
    final AtomicInteger sent = new AtomicInteger(0);

    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    for (int i = 0; i < CONCURRENCY; i++) {
      executor.execute(() -> {
        while (flag.get()) {
          limiter.acquire();
          final String name = randomName();
          final UUID id = randomId();
          final ListenableFuture<Boolean> f = storage.insert(name, id);
          outstanding.incrementAndGet();
          sent.incrementAndGet();
          Futures.addCallback(f, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(final Boolean result) {
              outstanding.decrementAndGet();
              if (sent.get() % 256 == 0) {
                System.out.println("outstanding: " + outstanding.get());
              }
            }

            @Override
            public void onFailure(final Throwable t) {
              outstanding.decrementAndGet();
              System.out.println("failure: " + t);
            }
          });
        }
      });
    }


    // run for 600s
    Uninterruptibles.sleepUninterruptibly(600, TimeUnit.SECONDS);
    flag.set(false);

    // graceful drain period
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    storage.shutdown();
    executor.shutdown();
  }

  /**
   * DDL
   *
   *

   CREATE KEYSPACE cassandra9086
     WITH REPLICATION = {
       'class' : 'NetworkTopologyStrategy',
       'datacenter1' : 1
     }
       AND durable_writes = true;

   create table name_index (
     name text PRIMARY KEY,
     id set<uuid>,
   ) WITH
     compaction={'class': 'LeveledCompactionStrategy'};

   */
}
