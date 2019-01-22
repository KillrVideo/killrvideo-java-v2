package com.killrvideo.service.user.dto;

import java.util.Date;
import java.util.UUID;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.Length;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.killrvideo.model.CommonConstants;
import com.killrvideo.service.user.dao.UserDseDao;

/**
 * Pojo representing DTO for table 'users'.
 *
 * @author DataStax Developer Advocates team.
 */
@Table(keyspace = 
            CommonConstants.KILLRVIDEO_KEYSPACE,
       name = UserDseDao.TABLENAME_USERS)
public class User {

    /** Column names in the DB. */
    public static final String COLUMN_USERID    = "userid";
    public static final String COLUMN_FIRSTNAME = "firstname";
    public static final String COLUMN_LASTNAME  = "lastname";
    public static final String COLUMN_EMAIL     = "email";
    public static final String COLUMN_CREATE    = "created_date";

    @PartitionKey
    private UUID userid;

    @Length(min = 1, message = "firstName must not be empty")
    @Column
    private String firstname;

    @Length(min = 1, message = "lastname must not be empty")
    @Column
    private String lastname;

    @Length(min = 1, message = "email must not be empty")
    @Column
    private String email;

    @NotNull
    @Column(name = COLUMN_CREATE)
    private Date createdAt;

    /**
     * Default constructor (reflection)
     */
    public User() {}

    /**
     * Constructor with all parameters.
     */
    public User(UUID userid, String firstname, String lastname, String email, Date createdAt) {
        this.userid = userid;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.createdAt = createdAt;
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

    /**
     * Getter for attribute 'firstname'.
     *
     * @return
     *       current value of 'firstname'
     */
    public String getFirstname() {
        return firstname;
    }

    /**
     * Setter for attribute 'firstname'.
     * @param firstname
     * 		new value for 'firstname '
     */
    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    /**
     * Getter for attribute 'lastname'.
     *
     * @return
     *       current value of 'lastname'
     */
    public String getLastname() {
        return lastname;
    }

    /**
     * Setter for attribute 'lastname'.
     * @param lastname
     * 		new value for 'lastname '
     */
    public void setLastname(String lastname) {
        this.lastname = lastname;
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
     * Getter for attribute 'createdAt'.
     *
     * @return
     *       current value of 'createdAt'
     */
    public Date getCreatedAt() {
        return createdAt;
    }

    /**
     * Setter for attribute 'createdAt'.
     * @param createdAt
     * 		new value for 'createdAt '
     */
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
    
    
}
