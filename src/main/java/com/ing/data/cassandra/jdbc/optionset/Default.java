package com.ing.data.cassandra.jdbc.optionset;

import com.ing.data.cassandra.jdbc.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Default extends AbstractOptionSet {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOptionSet.class);

    @Override
    public String getCatalog() {
        // It requires a query to table system.local since DataStax driver 4+.
        // If the query fails, return null.
        try (final Statement stmt = getConnection().createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT cluster_name FROM system.local");
            if (rs.next()) {
                return rs.getString("cluster_name");
            }
        } catch (final SQLException e) {
            LOG.warn("Unable to retrieve the cluster name.", e);
            return null;
        }

        return null;
    }

    @Override
    public int getSQLUpdateResponse() {
        return 0;
    }
}
