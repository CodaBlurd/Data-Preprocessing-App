package com.coda.core.dtos;

import lombok.Getter;

@Getter
public class ConnectionDetails {
    /**
     * The url of the database.
     */
    private final String url;
    /**
     * The username of the database.
     */
    private final String username;
    /**
     * The password of the database.
     */
    private final String password;

    /**
     * Constructor to initialize the connection details.
     * @param url the url of the database
     * @param username the username of the database
     * @param password the password of the database
     */

    public ConnectionDetails(String url,
                             String username,
                             String password){
        this.url = url;
        this.username = username;
        this.password = password;
    }

}
