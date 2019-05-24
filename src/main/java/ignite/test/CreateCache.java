package ignite.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * CreateCache
 */
public class CreateCache extends Example {
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
            cfg.setIndexedTypes(Integer.class, Account.class);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setBackups(2);
            cfg.setReadFromBackup(true);
            cfg.setWriteSynchronizationMode(FULL_SYNC);

            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initializing the cache.
                for (int i = 1; i <= ENTRIES_COUNT; i++)
                    cache.put(i, new Account(i, 100));

                System.out.println("Accounts before transfers");
                System.out.println();
                printAccounts(cache);
                printTotalBalance(cache);
            }
        }
    }
}