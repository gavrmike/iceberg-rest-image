package org.apache.iceberg.jdbc;

import java.util.function.Function;

import org.mindrot.jbcrypt.BCrypt;

public class PasswordUtils {

    private static final int logRounds = 11;

    /*
     * Copy pasted from BCrypt internals :(. Ideally a method
     * to exports parts would be public. We only care about rounds
     * currently.
     */
    private static int getRounds(String salt) {
        char minor = (char) 0;
        int off = 0;

        if (salt.charAt(0) != '$' || salt.charAt(1) != '2')
            throw new IllegalArgumentException("Invalid salt version");
        if (salt.charAt(2) == '$')
            off = 3;
        else {
            minor = salt.charAt(2);
            if (minor != 'a' || salt.charAt(3) != '$')
                throw new IllegalArgumentException("Invalid salt revision");
            off = 4;
        }

        // Extract number of rounds
        if (salt.charAt(off + 2) > '$')
            throw new IllegalArgumentException("Missing salt rounds");
        return Integer.parseInt(salt.substring(off, off + 2));
    }

    public static boolean verifyHash(String password, String hash) {
        return BCrypt.checkpw(password, hash);
    }

    public static boolean verifyAndUpdateHash(String password, String hash, Function<String, Boolean> updateFunc) {
        if (BCrypt.checkpw(password, hash)) {
            int rounds = getRounds(hash);
            // It might be smart to only allow increasing the rounds.
            // If someone makes a mistake the ability to undo it would be nice though.
            if (rounds != logRounds) {
                String newHash = hash(password);
                return updateFunc.apply(newHash);
            }
            return true;
        }
        return false;
    }

    public static String hash(String password) {
        return BCrypt.hashpw(password, BCrypt.gensalt(logRounds));
    }
}
