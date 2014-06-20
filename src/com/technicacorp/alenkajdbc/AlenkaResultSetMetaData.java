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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.technicacorp.alenka.JDBC;

public class AlenkaResultSetMetaData implements ResultSetMetaData {
	private static final String ALENKA_VARCHAR = "varchar";
	private static final String ALENKA_INT = "int";
	private static final String ALENKA_FLOAT = "decimal";
	
	private JDBC alenkaWrapper = null;
	private List<AlenkaColumn> columns = new ArrayList<AlenkaColumn>();
	
	protected AlenkaResultSetMetaData(JDBC wrapper) {
		alenkaWrapper = wrapper;
		initResultSet();
	}
	
	protected List<AlenkaColumn> getColumns() {
		return columns;
	}
	
	private void initResultSet() {
		//get the column info
		
		//Add a dummy column to the first (0) index of the ArrayList.  ArrayList are 0 based indexes
		//but all the JDBC methods are 1 based.  
		columns.add(new AlenkaColumn("zerobased"));
		
		for(int i = 1; i <= alenkaWrapper.getColumnCount(); i++) {
			AlenkaColumn currCol = new AlenkaColumn(alenkaWrapper, i);
			System.out.println("Column #" + i + ": " + currCol.toString());
			columns.add(currCol);
		}
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return columns.size() - 1;  //-1 to account for the dummy first column 
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		checkColumnIndex(column);
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		// Not sure if Alenka is or not, but true should be safer
		checkColumnIndex(column);
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		checkColumnIndex(column);
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		//Not a good way to look this up in Alenka, but there is no currency data type so no way to tell for user
		checkColumnIndex(column);
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// I think all can be null in Alenka, will need to modify if that isn't the case
		checkColumnIndex(column);
		return ResultSetMetaData.columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// All should be unsigned in Alenka
		checkColumnIndex(column);
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getDisplaySize();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getLabel();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getSchemaName();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getPrecision();
	}

	@Override
	public int getScale(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getScale();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getTableName();
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		checkColumnIndex(column);
		
		//Not sure what this is in Alenka so just return "" per JDBC spec
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		checkColumnIndex(column);
		return columns.get(column).getType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		checkColumnIndex(column);
		if(columns.get(column).getType() == Types.INTEGER) {
			return ALENKA_INT;
		} else if (columns.get(column).getType() == Types.FLOAT) {
			return ALENKA_FLOAT;
		} else {
			return ALENKA_VARCHAR;
		}
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		//No updates in Alenka for now
		checkColumnIndex(column);
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		//No updates in Alenka for now
		checkColumnIndex(column);
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		//No updates in Alenka for now
		checkColumnIndex(column);
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		//No custom class names available as of yet
		checkColumnIndex(column);
		return null;
	}

	private void checkColumnIndex(int column) throws SQLException {
		//This will prevent any access to the dummy first column to fix the 1 based indexing of the columns in JDBC
		if(column <= 0 || column > columns.size()) {
			throw new SQLException("Invalid column index, " + column);
		}
	}
}
