package ignite.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.*;

/**
 * Example01
 *
 * Demonstrates how a multi-task application works with data stored in Apache Ignite.
 * The application simulates money transferring between 10 bank accounts.
 *
 */
public class Example01 extends Example {
    public static void main(String[] args) throws IgniteException, InterruptedException {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start(new IgniteConfiguration()
                .setDeploymentMode(DeploymentMode.PRIVATE)
                .setPeerClassLoadingEnabled(true)
                .setBinaryConfiguration(new BinaryConfiguration()
                        .setCompactFooter(true))
                .setCommunicationSpi(new TcpCommunicationSpi()
                        .setLocalAddress("localhost"))
                        .setTransactionConfiguration(new TransactionConfiguration()
                                .setDefaultTxTimeout(1_000L))
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(new TcpDiscoveryVmIpFinder()
                                .setAddresses(Arrays.asList("127.0.0.1:47500..47509")
                                )
                        )
                ))) {

            CacheConfiguration<Integer, Account> cfg = new CacheConfiguration<>(CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setIndexedTypes(Integer.class, Account.class);

            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initializing the cache.
                for (int i = 1; i <= ENTRIES_COUNT; i++)
                  cache.put(i, new Account(i, 100));

                System.out.println("Accounts before transfers");
                System.out.println();
                printAccounts(cache);
                printTotalBalance(cache);

                IgniteRunnable run1 = new MyIgniteRunnable(cache, ignite,1);
                IgniteRunnable run2 = new MyIgniteRunnable(cache, ignite,2);
                List<IgniteRunnable> arr = Arrays.asList(run1, run2);
                ignite.compute().run(arr);

                Scanner keyIn = new Scanner(System.in);

                System.out.println();
                System.out.println("Accounts after transfers");
                printAccounts(cache);
                printTotalBalance(cache);
            }
        }
    }
}