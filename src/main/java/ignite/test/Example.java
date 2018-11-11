package ignite.test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;

import java.util.Random;

/**
 * Example
*/

public class Example {
    /* Cache name. */
    protected static final String CACHE_NAME = "TestCache";

    /* Total number of entries to use in the example. */
    protected static int ENTRIES_COUNT = 10;

    protected static int getRandomAmount(int maxAmount) {
        return getRandomNumberInRange(0, maxAmount);
    }

    protected static int getRandomNumberInRange(int min, int max) {

        if (min > max) {
            throw new IllegalArgumentException("max (" + max + ") must be greater than min (" + min + ")");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    protected static int[] getPairOfRandomAccounts() {
        int fromAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
        int toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);

        while(fromAccountId == toAccountId) {
            toAccountId = getRandomNumberInRange(1, ENTRIES_COUNT);
        }

        return new int[] {fromAccountId, toAccountId};
    }

    protected static void printAccounts(IgniteCache<Integer, Account> cache) {
        for (int acctId = 1; acctId <= ENTRIES_COUNT; acctId++)
            System.out.println("[" + acctId + "] = " + cache.get(acctId));
    }

    protected static void printTotalBalance(IgniteCache<Integer, Account> cache) throws IgniteException {
        int totalBalance = 0;

        for (int acctId = 1; acctId <= ENTRIES_COUNT; acctId++) {
            Account account = cache.get(acctId);
            totalBalance += account.balance;
        }

        System.out.println("Total Balance: $" + totalBalance);
    }
}