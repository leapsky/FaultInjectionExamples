package ignite.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

/**
 * Example00
 *
 * Demonstrates how a single-client application works with data stored in Apache Ignite.
 * The application simulates money transferring between 10 bank accounts.
 *
 */
public class Example00 extends Example {

    public static void main(String[] args) throws IgniteException, InterruptedException {
        try (Ignite ignite = Ignition.start(new IgniteConfiguration()
                    .setIgniteInstanceName("SingleClient")
                    .setDeploymentMode(DeploymentMode.PRIVATE)
                    .setPeerClassLoadingEnabled(true)
                    .setBinaryConfiguration(new BinaryConfiguration()
                            .setCompactFooter(true))
                    .setCommunicationSpi(new TcpCommunicationSpi()
                            .setLocalAddress("localhost"))
                    .setTransactionConfiguration(new TransactionConfiguration()
                            .setDefaultTxTimeout(5_000L))
                    .setDiscoverySpi(new TcpDiscoverySpi()
                            .setIpFinder(new TcpDiscoveryVmIpFinder()
                                    .setAddresses(Arrays.asList("127.0.0.1:47500..47509")
                                    )
                            )
                    )
            )) {

            CacheConfiguration<Integer, Account> cfg = new CacheConfiguration<>(CACHE_NAME);

            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initializing the cache.
                for (int i = 1; i <= ENTRIES_COUNT; i++)
                    cache.put(i, new Account(i, 100));

                System.out.println("Accounts before transfers");
                printAccounts(cache);
                printTotalBalance(cache);

                for (int i = 1; i <= 100; i++) {
                    int pairOfAccounts[] = getPairOfRandomAccounts();
                    transferMoney(cache, pairOfAccounts[0], pairOfAccounts[1]);
                }

                System.out.println();
                System.out.println("Accounts after transfers");
                printAccounts(cache);
                printTotalBalance(cache);
            }
        }
    }

    private static void transferMoney(IgniteCache<Integer, Account> cache, int fromAccountId, int toAccountId) {
        Account fromAccount = cache.get(fromAccountId);
        Account toAccount = cache.get(toAccountId);

        int amount = getRandomAmount(fromAccount.balance);
        if (amount < 1) {
            return;
        }

        // Withdraw from account
        fromAccount.withdraw(amount);

        // Deposit into account.
        toAccount.deposit(amount);

        // Store updated accounts in cache.
        cache.put(fromAccountId, fromAccount);
        cache.put(toAccountId, toAccount);

        System.out.println("Transfer $" + amount + " from account " + fromAccountId + " to account " + toAccountId);
    }
}