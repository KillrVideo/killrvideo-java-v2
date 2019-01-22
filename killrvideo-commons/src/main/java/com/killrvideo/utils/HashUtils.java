package com.killrvideo.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Working with Hasehed passwords.
 *
 * @author DataStax Developer Advocates team.
 */
public class HashUtils {
    
    /**
     * Hiding default constructor.
     * 
     */
    private HashUtils() {}

    /**
     * Work with passwords. 
     *
     * @param password
     *      current password
     * @return
     *      passwortd hashed
     */
    public static String hashPassword(String password) {
        return new String(DigestUtils.getSha512Digest().digest(password.getBytes()));
    }

    /**
     * ATest password againast hashed version.
     *
     * @param realPassword
     *      clear text password
     * @param hash
     *      hash to evaluate
     * @return
     *      if vpaqssword is valid.
     */
    public static boolean isPasswordValid(String realPassword, String hash) {
        if (isBlank(realPassword) || isBlank(hash)) {
            return false;
        }
        return hashPassword(realPassword.trim()).compareTo(hash) == 0;
    }
}
