package com.ing.data.cassandra.jdbc.optionset;

public class Liquibase extends AbstractOptionSet {

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public int getSQLUpdateResponse() {
        return -1;
    }
}
