package ignite.test;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import java.io.Serializable;

/**
 * Account
 */
class Account implements Serializable {
    /** Account ID. */
    @QuerySqlField (index = true)
    private int id;

    /** Account balance. */
    @QuerySqlField(index = true, descending = true)
    public int balance;

    /**
     * @param id Account ID.
     * @param balance Balance.
     */
    Account(int id, int balance) {
        this.id = id;
        this.balance = balance;
    }

    /**
     * Change balance by specified amount.
     *
     * @param amount Amount to add to balance (may be negative).
     */
    void deposit(int amount) {
        balance += amount;
    }

    /**
     * Change balance by specified amount.
     *
     * @param amount Amount to add to balance (may be negative).
     */
    void withdraw(int amount) {
        balance -= amount;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Account [id=" + id + ", balance=$" + balance + ']';
    }
}