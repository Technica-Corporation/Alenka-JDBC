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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.bridj.Pointer;

import com.technicacorp.alenka.AlenkaLibrary;

public class AlenkaStatement implements Statement {
    protected ResultSet lastResultSet = null;
    protected int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    private AlenkaConnection connection;
    private int maxRows = 0;
    private boolean closed;
    private boolean poolable = false;
    private boolean closeOnCompletion = false;

    public AlenkaStatement(AlenkaConnection conn) {
    	connection = conn;
    }
    
    protected void checkOpen() throws SQLException {
        if (closed)
            throw new SQLException();
    }

    /**
     * Will execute the given SQL in Alenka.
     * @param sql
     * @return The last variable in the statement.  This is the variable name that must be used
     * to look up the final CudaSet in Alenka to pull out the result set
     * @throws SQLException
     */
    private String executeAlenka(String sql) throws SQLException {
    	System.out.println("Executing the query: \n" + sql);
    	//Native.setProtected(true);
    	AlenkaLibrary.alenkaInit(null);
    	
    	Pointer<Byte> cSQL = Pointer.pointerToCString(sql);
    	int rc = AlenkaLibrary.alenka_JDBC(cSQL);
    	
    	System.out.println("Alenka executed and returned with " + rc);
    	
    	//Now find the last variable name.  The delimiting string in Alenka between the variable name
    	//and the "SQL" command is ":=" and end of statement string is ";" so we will split on those
    	String[] tokens = sql.trim().split(":=|;");
    	if(tokens.length < 2) {
    		throw new SQLException("Unable to determine result set variable name, Alenka SQL command not in correct format");
    	} else {
    		return tokens[tokens.length - 2].trim();
    	}
    	
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
    	String resultSetName = executeAlenka(sql);
    	lastResultSet = new AlenkaResultSet(this, maxRows, resultSetName);
    	return lastResultSet;
    }

    public int executeUpdate(String sql) throws SQLException {
    	executeAlenka(sql);
    	return 0;
    }

    public void close() throws SQLException {
    		System.out.println("Closing alenka connection");
    		AlenkaLibrary.alenkaClose();
    }

    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Method Not Supported: setMaxFieldSize(int)");
    }

    public int getMaxRows() throws SQLException {
        return 0;
    }

    public void setMaxRows(int max) throws SQLException {
    	maxRows = max;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Escape processing is not supported");
    }

    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    public void setQueryTimeout(int seconds) throws SQLException {
    	if(seconds < 0) {
    		throw new SQLException("Query Timeout must be >= 0");
    	} else {
    		throw new SQLFeatureNotSupportedException("QueryTimeout not supported");
    	}
    }

    public void cancel() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Cancel not supported");
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public void setCursorName(String name) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Setting the curson name is not supported");
    }

    public boolean execute(String sql) throws SQLException {
    	String resultSetName = executeAlenka(sql);
    	lastResultSet = new AlenkaResultSet(this, maxRows, resultSetName);
        return true;
    }

    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    public int getUpdateCount() throws SQLException {
        return 0;
    }

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    public void setFetchDirection(int direction) throws SQLException {
    	if(direction != ResultSet.FETCH_FORWARD) {
    		throw new SQLException("Only ResultSet.FETCH_FORWARD is supported");
    	}
    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public void setFetchSize(int rows) throws SQLException {
    	if(rows < 0) {
    		throw new SQLException("FetchSize must be > 0");
    	}
    	//Currently we hook directly into the Alenka CudaSet which has ALL of the results, so this doesn't really apply
    }

    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public void addBatch(String sql) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Batch execution not supported yet");
    }

    public void clearBatch() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Batch execution not supported yet");
    }

    public int[] executeBatch() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Batch execution not supported yet");
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

    public boolean getMoreResults(int current) throws SQLException {
    	//Alenka currently only has one result set.
        return false;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    	if(autoGeneratedKeys == RETURN_GENERATED_KEYS) {
    		throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    	} else {
    		executeUpdate(sql);
            return 0;
    	}
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    	if(autoGeneratedKeys == RETURN_GENERATED_KEYS) {
    		throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    	} else {
    		execute(sql);
            return true;
    	}
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Retrieving Generated Keys is not supported");
    }

    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public boolean isPoolable() throws SQLException {
        return poolable;
    }

    public void setPoolable(boolean poolable) throws SQLException {
    	this.poolable = poolable;
    }

    public void closeOnCompletion() throws SQLException {
    	closeOnCompletion = true; 
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
