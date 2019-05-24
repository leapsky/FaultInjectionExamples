package ignite.test;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;

import javax.cache.CacheException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

import java.util.*;

/**
 * Example02
 *
 * Demonstrates how to run Java code with the JMeter JSR223 Sampler.
 * The application simulates money transferring between 10 bank accounts.
 *
 */
public class Example02 extends Example {

    private static int TX_TIMEOUT = 1000;

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
                System.out.println("Client " + clientId + " started");

                for (int i = 1; i <= interations; i++) {
                    transferMoney(cache, ignite);
                }

                System.out.println();
                System.out.println("Client " + clientId + " finished");
            }
        }
    }

    private static void transferMoney(IgniteCache<Integer, Account> cache, Ignite ignite) {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TX_TIMEOUT, 10)) {
            int fromAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
            int toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);

            while (fromAccountId == toAccountId) {
                toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
            }

            Account fromAccount = cache.get(fromAccountId);
            Account toAccount = cache.get(toAccountId);

            int amount = getRandomAmount(fromAccount.balance);
            if (amount < 1) {
                // No money in the account
                tx.commit();
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
        } catch (CacheException e) {
            if (e.getCause() instanceof IgniteCheckedException &&
                    e.getCause().getCause() instanceof TransactionDeadlockException) {

                System.out.println(">>> Deadlock Detected:");
                System.out.println(e.getCause().getCause().getMessage());
                System.out.println();

                return;
            }
        }

    }
}