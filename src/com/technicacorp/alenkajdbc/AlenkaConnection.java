/*
*Alenka JDBC - A JDBC Driver for Alenka
*Copyright (C) 2014  Technica Corporation
*
*This program is free software: you can redistribute it and/or modify
*it under the terms of the GNU General Public License as published by
*the Free Software Foundation, either version 3 of the License, or
*(at your option) any later version.

*This program is distributed in the hope that it will be useful,
*but WITHOUT ANY WARRANTY; without even the implied warranty of
*MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*GNU General Public License for more details.

*You should have received a copy of the GNU General Public License
*along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package com.technicacorp.alenkajdbc;

import java.io.File;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.bridj.BridJ;

import com.technicacorp.alenka.AlenkaLibrary;
import com.technicacorp.alenka.JDBC;

/**
 * Created by devin on 4/7/14.
 */
public class AlenkaConnection implements Connection {
    private final String url;
    private AlenkaDatabaseMetaData meta = null;
    private boolean autoCommit = true;  //All of Alenka's querys are automatically committed
    private boolean isClosed = true;
    private Properties clientInfo = null;

    /**
     * Constructor to create a connection to the Alenka Shared Library at the given location
     * Currently there is really no "Connection" to Alenka.  This connection object
     * is just a wrapper for ensuring the Alenka shared library is accessable and there are
     * no errors when reading it. 
     * Eventually this class will need to be more robust once a real Alenka connection is created.
     *
     * @param url The location of the shared library
     * @throws SQLException
     */
    public AlenkaConnection(String url) throws SQLException {

        if (url == null || url.length() == 0) {
            throw new IllegalArgumentException("URL must not be empty");
        } else if (!url.startsWith(AlenkaDriver.PREFIX)){ 
        	throw new IllegalArgumentException("Invalid JDBC prefix, must be " + AlenkaDriver.PREFIX);
        } else {
        	System.out.println("Loading Alenka URL: " + url);
        	this.url = url;
        	
        	//parse out library from URL
			try {
				File alenkaLibrary = new File(url.substring(AlenkaDriver.PREFIX.length()));
				BridJ.setNativeLibraryFile("alenka", alenkaLibrary);
				
				//Instantiate the native object to test that the library provided in the URL is correct
				//and can be loaded.
				@SuppressWarnings("unused")
				JDBC jdbc = new JDBC();
				
				isClosed = false;
				System.out.println("Connected to the Alenka library at: " + url);
			} catch (Exception e) {
				throw new SQLException("Unable to connect to Alenka at: " + url);
			}
        }
    }
    
    protected String getURL() {
    	return url;
    }

    public Statement createStatement() throws SQLException {
    	 return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
    	return prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
    	return prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
    	if(!autoCommit) {
    		throw new SQLException("Alenka only supports autoCommit = true");
    	}
    }

    public void commit() throws SQLException {
    	if(autoCommit) {
    		throw new SQLException("Autocommit is enabled");
    	} else {
    		throw new SQLException("Alenka only supports autoCommit");
    	}
    }

    public void rollback() throws SQLException {
    	if(autoCommit) {
    		throw new SQLException("Autocommit is enabled");
    	} else {
    		throw new SQLException("Alenka only supports autoCommit");
    	}
    }

    public void close() throws SQLException {
    	AlenkaLibrary.alenkaClose();
    	isClosed = true;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        if (meta == null) {
            meta = new AlenkaDatabaseMetaData(this);
        }
        return meta;
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
    	if(!readOnly) {
    		throw new SQLException("Alenka only supports readOnly");
    	}
    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setCatalog(String catalog) throws SQLException {
    	//Alenka does not support Catalogs, so per JDBC spec we will ignore this request
    }

    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public void setTransactionIsolation(int level) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support transactions");
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    	return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    	return prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    	throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public void setHoldability(int holdability) throws SQLException {
    	if(holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
    		throw new SQLFeatureNotSupportedException("Alenka only supports ResultSet.CLOSE_CURSORS_AT_COMMIT holdability");
    	}
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Alenka does not support transactions");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support transactions");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support transactions");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support transactions");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    	if(resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE) {
    		throw new SQLFeatureNotSupportedException("Alenka only supports ResultSet.TYPE_SCROLL_INSENSITIVE");
    	} else if(resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
    		throw new SQLFeatureNotSupportedException("Alenka only supports ResultSet.CONCUR_READ_ONLY");
    	} else if(resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
    		throw new SQLFeatureNotSupportedException("Alenka only supports ResultSet.CLOSE_CURSORS_AT_COMMIT");
    	}
    		
    	return new AlenkaStatement(this);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("PrepareStatement is not supported in Alenka");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Call is not supported in Alenka");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support auto generated keys");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support auto generated keys");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support auto generated keys");
    }

    public Clob createClob() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not clob datatype");
    }

    public Blob createBlob() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support blob datatype");
    }

    public NClob createNClob() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support nclob");
    }

    public SQLXML createSQLXML() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support sqlXML datatype");
    }

    public boolean isValid(int timeout) throws SQLException {
        if(timeout < 0) {
        	throw new SQLException("Timeout must be >= 0");
        }
        return isClosed();
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    	if(clientInfo == null) {
    		clientInfo = new Properties();
    	}
    	clientInfo.put(name,  value);
    }

    public String getClientInfo(String name) throws SQLException {
        if(clientInfo != null) {
        	return clientInfo.getProperty(name);
        } else {
        	return null;
        }
    }

    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    	clientInfo = properties;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    	throw new SQLFeatureNotSupportedException("createArrayOf is unsupported");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    	throw new SQLFeatureNotSupportedException("createStruct is unsupported");
    }

    public String getSchema() throws SQLException {
        return null;
    }

    public void setSchema(String schema) throws SQLException {
    	//Alenka does not support schemas, so per the JDBC-spec we will ignore this 
    }

    public void abort(Executor executor) throws SQLException {
    	throw new SQLFeatureNotSupportedException("abort not currently supported");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    	throw new SQLFeatureNotSupportedException("setNetworkTimeout not currently supported");
    }

    public int getNetworkTimeout() throws SQLException {
    	throw new SQLFeatureNotSupportedException("getNetworkTimeout not currently supported");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
