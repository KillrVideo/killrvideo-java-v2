package com.killrvideo.service.user.dto;

import java.io.Serializable;
import java.util.UUID;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.Length;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.killrvideo.model.CommonConstants;
import com.killrvideo.service.user.dao.UserDseDao;

/**
 * Pojo representing DTO for table 'user_credentials'
 *
 * @author DataStax Developer Advocates team.
 */
@Table(keyspace = 
        CommonConstants.KILLRVIDEO_KEYSPACE, 
       name = UserDseDao.TABLENAME_USER_CREDENTIALS)
public class UserCredentials implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = 2013590265131367178L;
    
    /** Column names in the DB. */
    public static final String COLUMN_USERID    = "userid";
    public static final String COLUMN_PASSWORD  = "password";
    public static final String COLUMN_EMAIL     = "email";

    @PartitionKey
    private String email;

    @Length(min = 1, message = "password must not be empty")
    @Column(name = COLUMN_PASSWORD)
    private String password;

    @NotNull
    @Column
    private UUID userid;

    /**
     * Default constructor (reflection)
     */
    public UserCredentials() {}

    /**
     * Constructor with all parameters.
     */
    public UserCredentials(String email, String password, UUID userid) {
        this.email = email;
        this.password = password;
        this.userid = userid;
    }

    /**
     * Getter for attribute 'email'.
     *
     * @return
     *       current value of 'email'
     */
    public String getEmail() {
        return email;
    }

    /**
     * Setter for attribute 'email'.
     * @param email
     * 		new value for 'email '
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * Getter for attribute 'password'.
     *
     * @return
     *       current value of 'password'
     */
    public String getPassword() {
        return password;
    }

    /**
     * Setter for attribute 'password'.
     * @param password
     * 		new value for 'password '
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Getter for attribute 'userid'.
     *
     * @return
     *       current value of 'userid'
     */
    public UUID getUserid() {
        return userid;
    }

    /**
     * Setter for attribute 'userid'.
     * @param userid
     * 		new value for 'userid '
     */
    public void setUserid(UUID userid) {
        this.userid = userid;
    }
    
}
