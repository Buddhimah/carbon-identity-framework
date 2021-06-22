package org.wso2.carbon.identity.core.persistence;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.base.IdentityException;
import org.wso2.carbon.identity.base.IdentityRuntimeException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.xml.namespace.QName;
import java.sql.Connection;
import java.sql.SQLException;

public class CustomJDBCPersistenceManager {
    private static Log log = LogFactory.getLog(CustomJDBCPersistenceManager.class);
    private static volatile CustomJDBCPersistenceManager instance;
    private DataSource dataSource;
    // This property refers to Active transaction state of postgresql db
    private static final String PG_ACTIVE_SQL_TRANSACTION_STATE = "25001";
    private static final String POSTGRESQL_DATABASE = "PostgreSQL";

    private CustomJDBCPersistenceManager() {

        initDataSource();
    }

    /**
     * Get an instance of the JDBCPersistenceManager. It implements a lazy
     * initialization with double
     * checked locking, because it is initialized first by identity.core module
     * during the start up.
     *
     * @return JDBCPersistenceManager instance
     * @throws IdentityException Error when reading the data source configurations
     */
    public static CustomJDBCPersistenceManager getInstance() {

        if (instance == null) {
            synchronized (CustomJDBCPersistenceManager.class) {
                if (instance == null) {
                    instance = new CustomJDBCPersistenceManager();
                }
            }
        }
        return instance;
    }

    private void initDataSource() {


        try {
            String dataSourceName = "jdbc/WSO2IdentityDB";
            Context ctx = new InitialContext();
            dataSource = (DataSource) ctx.lookup(dataSourceName);
        } catch (NamingException e) {
            String errorMsg = "Error when looking up the Identity Data Source.";
            throw IdentityRuntimeException.error(errorMsg, e);
        }
    }

    public void initializeDatabase() {

        IdentityDBInitializer dbInitializer = new IdentityDBInitializer(dataSource);
        dbInitializer.createIdentityDatabase();
    }

    /**
     * Returns an database connection for Identity data source.
     *
     * @return dbConnection
     * @throws IdentityRuntimeException
     * @Deprecated The getDBConnection should handle both transaction and non-transaction connection. Earlier it
     * handle only the transactionConnection. Therefore this method was deprecated and changed as handle both
     * transaction and non-transaction connection. getDBConnection(boolean shouldApplyTransaction) method used as
     * alternative of this method.
     */
    @Deprecated
    public Connection getDBConnection() throws IdentityRuntimeException {

        return getDBConnection(true);
    }

    /**
     * Returns an database connection for Identity data source.
     *
     * @param shouldApplyTransaction apply transaction or not
     * @return Database connection.
     * @throws IdentityException Exception occurred when getting the data source.
     */
    public Connection getDBConnection(boolean shouldApplyTransaction) throws IdentityRuntimeException {

        try {
            Connection dbConnection = dataSource.getConnection();
            if (shouldApplyTransaction) {
                dbConnection.setAutoCommit(false);
                try {
                    dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                } catch (SQLException e) {
                    // Handling startup error for postgresql
                    // Active SQL Transaction means that connection is not committed.
                    // Need to commit before setting isolation property.
                    if (dbConnection.getMetaData().getDriverName().contains(POSTGRESQL_DATABASE)
                            && PG_ACTIVE_SQL_TRANSACTION_STATE.equals(e.getSQLState())) {
                        dbConnection.commit();
                        dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    }
                }
            }
            return dbConnection;
        } catch (SQLException e) {
            String errMsg = "Error when getting a database connection object from the Identity data source.";
            throw IdentityRuntimeException.error(errMsg, e);
        }
    }

    /**
     * Returns Identity data source.
     *
     * @return Data source.
     */
    public DataSource getDataSource() {

        return dataSource;
    }

    /**
     * Revoke the transaction when catch then sql transaction errors.
     *
     * @param dbConnection database connection.
     */
    public void rollbackTransaction(Connection dbConnection) {

        try {
            if (dbConnection != null) {
                dbConnection.rollback();
            }
        } catch (SQLException e1) {
            log.error("An error occurred while rolling back transactions. ", e1);
        }
    }

    /**
     * Commit the transaction.
     *
     * @param dbConnection database connection.
     */
    public void commitTransaction(Connection dbConnection) {

        try {
            if (dbConnection != null) {
                dbConnection.commit();
            }
        } catch (SQLException e1) {
            log.error("An error occurred while commit transactions. ", e1);
        }
    }
}
