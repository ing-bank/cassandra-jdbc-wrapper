package com.ing.data.cassandra.jdbc.optionset;

import com.ing.data.cassandra.jdbc.CassandraConnection;

/**
 * Option Set for compliance mode.
 * Different use cases require one or more adjustments to the wrapper, to be compatible.
 * Thus, OptionSet would provide convenience to set for different flavours.
 *
 */
public interface OptionSet {
    /**
      * There is no Catalog concept in cassandra. Different flavour requires different response.
     *
      * @return
     */
    String getCatalog();

    /**
     * There is no updateCount available in Datastax Java driver, different flavour requires different response.
     *
     * @return
     */
    int getSQLUpdateResponse();

    /**
     * Referenced connection. See @{@link AbstractOptionSet}
     * @param connection
     */
    void setConnection(CassandraConnection connection);
    CassandraConnection getConnection();
}
