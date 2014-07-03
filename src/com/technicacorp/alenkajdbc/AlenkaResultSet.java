/*
*Alenka JDBC - A JDBC Driver for Alenka
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package com.technicacorp.alenkajdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.bridj.Pointer;

import com.technicacorp.alenka.AlenkaLibrary;
import com.technicacorp.alenka.JDBC;
import com.technicacorp.alenka.JDBC.CudaSet;

public class AlenkaResultSet implements ResultSet {
	private AlenkaStatement statement = null;
	private AlenkaResultSetMetaData metaData = null;
	private Pointer<CudaSet> cudaSet = null;
	private JDBC alenkaWrapper = null;
	private int currentRow = -1;
	private int maxRows = 0;
	
	protected AlenkaResultSet(AlenkaStatement stmt, int maxR, String alenkaResultSetName) throws SQLException {
		statement = stmt;
		maxRows = maxR;
	
		System.out.println("Retrieving the result set named \"" + alenkaResultSetName + "\" from Alenka");
		cudaSet = AlenkaLibrary.initializeResultSet_JDBC(Pointer.pointerToCString(alenkaResultSetName));
		if(cudaSet == null) {
			throw new SQLException("Unable to find result set in Alenka");
			
		}
		
		alenkaWrapper = new JDBC(cudaSet);
		System.out.println("JDBC->record count: " + alenkaWrapper.getRecordCount());
		
		metaData = new AlenkaResultSetMetaData(alenkaWrapper);
	}
	
    @Override
    public boolean next() throws SQLException {
    	if(maxRows > 0 && maxRows == currentRow) {
    		return false;
    	} else if(currentRow == -1 && alenkaWrapper.getRecordCount() > 0) {
    		currentRow++;
    		return true;
    	} else if(++currentRow < alenkaWrapper.getRecordCount()) {
    		return true;
    	} else {
    		return false;
    	}
    }

    @Override
    public void close() throws SQLException {
    	if(statement != null) {
    		try {
    			if(statement.isCloseOnCompletion()) {
    				statement.close();
    			}
    		} catch(SQLException e) {}
    	}
    	
    	statement = null;
    	metaData = null;
    	
    	AlenkaLibrary.resultSetClose_JDBC();
    	cudaSet = null;
    	alenkaWrapper = null;
    	currentRow = -1;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
    	Pointer<Byte> stringData = Pointer.allocateBytes(metaData.getColumnDisplaySize(columnIndex) + 1);
    	alenkaWrapper.retrieveChar(currentRow, columnIndex-1, stringData);
    	
    	return stringData.getCString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Boolean data type not supported in Alenka");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Byte data type not supported in Alenka");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Short data type not supported in Alenka");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return (int)alenkaWrapper.retrieveInt(currentRow, columnIndex - 1);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return alenkaWrapper.retrieveInt(currentRow, columnIndex - 1);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (float)alenkaWrapper.retrieveFloat(currentRow, columnIndex - 1);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return alenkaWrapper.retrieveFloat(currentRow, columnIndex - 1);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    	throw new SQLFeatureNotSupportedException("BigDecimal data type not supported in Alenka");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("byte[] data type not supported in Alenka");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Date data type not supported in Alenka");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Time data type not supported in Alenka");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Timestamp data type not supported in Alenka");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("AsciiStream not supported in Alenka");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Unicode not supported in Alenka");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Binary not supported in Alenka");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
    	//TODO
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    	//TODO
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Alenka does not support cursor name");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
    	if(columnIndex < 0 || columnIndex >= metaData.getColumnCount()) {
    		throw new SQLException("Invalid column index, " + columnIndex);
    	}
    	
    	int jdbcType = metaData.getColumns().get(columnIndex).getType();
    	if(jdbcType == Types.INTEGER) {
    		return getInt(columnIndex);
    	} else if(jdbcType == Types.FLOAT) {
    		return new Double(getDouble(columnIndex)); //Alenka float is really a Java double
    	} else {
    		return getString(columnIndex);
    	}
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
    	return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
    	List<AlenkaColumn> columns = metaData.getColumns();
    	for(int i = 0; i < columns.size(); i++) {
    			AlenkaColumn col = (AlenkaColumn)columns.get(i);
    			if(col.getLabel().equals(columnLabel)) {
    				return i;
    			}
    		}
    	throw new SQLException("Invalid Column Name: " + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support CharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support BigDecimal");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
       if(currentRow == -1) {
    	   return true;
       } else {
    	   return false;
       }
    }

    @Override
    public boolean isAfterLast() throws SQLException {
    	if(currentRow == alenkaWrapper.getRecordCount()) {
    		return true;
    	}  else {
    		return false;
    	}
    }

    @Override
    public boolean isFirst() throws SQLException {
        if(currentRow == 0) {
        	return true;
        } else {
        	return false;
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        if(currentRow == alenkaWrapper.getRecordCount() -1) {
        	return true;
        } else {
        	return false;
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
    	currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
    	currentRow = alenkaWrapper.getRecordCount();
    }

    @Override
    public boolean first() throws SQLException {
        currentRow = 0;
        if(alenkaWrapper.getRecordCount() <= 0) {
        	return false;
        } else {
        	return true;
        }
    }

    @Override
    public boolean last() throws SQLException {
        currentRow = (alenkaWrapper.getRecordCount() - 1);
        if(alenkaWrapper.getRecordCount() <= 0) {
        	return false;
        } else {
        	return true;
        }
    }

    @Override
    public int getRow() throws SQLException {
        return currentRow + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if(row >= 0) {
        	currentRow = row - 1;
        } else {
        	//In this case row is a negative number.  So adding it to the record count
        	//will subtract it from the record count which is what we want
        	currentRow = alenkaWrapper.getRecordCount() + row;
        }
        
        if(currentRow < 0 || currentRow >= alenkaWrapper.getRecordCount()) {
        	return false;
        } else {
        	return true;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        currentRow = currentRow + rows;
        if(currentRow < 0 || currentRow >= alenkaWrapper.getRecordCount()) {
        	return false;
        } else {
        	return true;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        currentRow--;
        if(currentRow < 0) {
        	return false;
        } else {
        	return true;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    	if(direction != FETCH_FORWARD) {
    		throw new SQLException("Fetch_Forward only direction supported");
    	}
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    	if(rows != 0) {
    		throw new SQLException("Alenka only supports fetch size of 1");
    	}
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
    	updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    	updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
    	updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
    	updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
    	updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
    	updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
    	updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
    	updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    	updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
    	updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    	updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
    	updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
    	updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    	updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    	updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    	updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    	updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    	updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
    	updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRow() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void deleteRow() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void refreshRow() throws SQLException {
    	throw new SQLFeatureNotSupportedException("Alenka does not support refreshing the row");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject(columnIndex, Map<String, Class<?>> map) is not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef is not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getBlob is not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getClob is not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getArray is not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getDate is not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getTime is not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getTimestamp is not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getURL is not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getRowId is not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public int getHoldability() throws SQLException {
    	return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
       if(statement == null) {
    	   return true;
       } else {
    	   return false;
       }
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    
    public NClob getNClob(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getNClob is not supported");
    }

    
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getSQLXML is not supported");
    }

    
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    
    public String getNString(int columnIndex) throws SQLException {
    	throw new SQLFeatureNotSupportedException("getNString is not supported");
    }

    
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    /**
     * @see java.sql.ResultSet#getNCharacterStream(int)
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unsupported by Alenka: Get NCharacter Stream - Alenka does not " +
                "support NCHAR, NVARCHAR, or LONGVARCHAR ");
    }

    /**
     * @see java.sql.ResultSet#getNCharacterStream(String)
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    /**
     * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateNCharacterStream(String, java.io.Reader, long)
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateAsciiStream(String, java.io.InputStream, long)
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBinaryStream(String, java.io.InputStream, long)
     */
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateCharacterStream(String, java.io.Reader, long)
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBlob(String, java.io.InputStream, long)
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateClob(String, java.io.Reader, long)
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateNClob(String, java.io.Reader, long)
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
     */
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateNCharacterStream(String, java.io.Reader)
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
     */
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
     */
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
     */
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateAsciiStream(String, java.io.InputStream)
     */
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBinaryStream(String, java.io.InputStream)
     */
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateCharacterStream(String, java.io.Reader)
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateBlob(String, java.io.InputStream)
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");

    }

    /**
     * @see java.sql.ResultSet#updateClob(String, java.io.Reader)
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#updateClob(String, java.io.Reader)
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    	throw new SQLException("Result set is CONCUR_READ_ONLY");
    }

    /**
     * @see java.sql.ResultSet#getObject(int, Class)
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject Unsupported by Alenka");
    }

    /**
     * @see java.sql.ResultSet#getObject(String, Class)
     */
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    /**
     * @see java.sql.ResultSet#unwrap(Class)
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("No a wrapper");
    }

    /**
     * @see java.sql.ResultSet#isWrapperFor(Class)
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
