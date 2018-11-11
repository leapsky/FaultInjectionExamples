package ignite.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

import java.util.*;

/**
 * Example02
 *
 * Demonstrates how to run Java code with the JMeter JSR223 Sampler.
 * The application simulates money transferring between 10 bank accounts.
 *
 */
public class Example02 extends Example {
    public static void runClient(int clientId, int interations) throws IgniteException, InterruptedException {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client" + clientId)
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
            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                System.out.println("Accounts before transfers");
                printAccounts(cache);
                printTotalBalance(cache);

                for (int i = 1; i <= interations; i++) {
                    transferMoney(cache, ignite);
                }

                System.out.println();
                System.out.println("Accounts after transfers");
                printAccounts(cache);
                printTotalBalance(cache);
            }
        }
    }

    private static void transferMoney(IgniteCache<Integer, Account> cache, Ignite ignite) {
        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            int fromAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
            int toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);

            while (fromAccountId == toAccountId) {
                toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
            }

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
            tx.commit();
        }
    }
}