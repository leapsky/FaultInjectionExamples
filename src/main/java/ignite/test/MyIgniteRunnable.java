package ignite.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

public class MyIgniteRunnable extends Example implements IgniteRunnable {
    private IgniteCache<Integer, Account> cache;
    private Integer clientId;
    @IgniteInstanceResource
    private Ignite ignite;

    public MyIgniteRunnable (IgniteCache<Integer, Account> cache, Ignite ignite, Integer clientId) {
        this.cache = cache;
        this.clientId = clientId;
        this.ignite = ignite;
    }

    @Override
    public void run() {
        for (int i = 1; i <= 1000; i++) {
            System.out.println("iteration number "+i);
            int pairOfAccounts[] = getPairOfRandomAccounts();
            transferMoney(pairOfAccounts[0], pairOfAccounts[1]);
        }
    }

    private void transferMoney(int fromAccountId, int toAccountId) {
        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
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

            // Print log message
            System.out.println("Client " + clientId + " transfers $" + amount + " from account " + fromAccountId + " to account " + toAccountId);
            tx.commit();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}