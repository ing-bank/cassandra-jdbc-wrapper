package com.ing.data.cassandra.jdbc.optionset;

import com.ing.data.cassandra.jdbc.CassandraConnection;

public abstract class AbstractOptionSet implements OptionSet {

    private CassandraConnection connection;

    @Override
    public CassandraConnection getConnection() {
        return connection;
    }

    public void setConnection(CassandraConnection connection) {
        this.connection = connection;
    }
}
