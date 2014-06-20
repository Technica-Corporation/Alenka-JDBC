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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Test Driver for the Alenka-JDBC Driver or any JDBC driver.  
 *
 */
public class TestAlenka {
	private static final int ROW_LIMIT = 10;
	
	public TestAlenka() throws Exception {

	}
	
	public Connection initializeJDBC(String driver, String url) throws Exception {
		System.out.println("Registering the driver: " + driver);
		Class.forName(driver);
		
		System.out.println("Connecting to Database at URL: " + url);
		return DriverManager.getConnection(url);
		
	}
	
	public ResultSet executeQuery(Connection conn, String fileName) throws SQLException, IOException {
		Statement stmt = conn.createStatement();
		stmt.setMaxRows(ROW_LIMIT);
		
		//read the query statements from the file
		File queryFile = new File(fileName);
		if(!queryFile.exists() || !queryFile.canRead()) {
			throw new IOException("Invalid query file name: " + fileName);
		}
		
		String queryString = new String();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(queryFile));
			String currString;
			while((currString = br.readLine()) != null) {
				queryString = queryString + currString;
			}
		} finally {
			try { br.close(); } catch(IOException e) {}
		}
		
		return stmt.executeQuery(queryString);
	}
	
	public void processResults(ResultSet rs) throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		int colCount = metaData.getColumnCount();
		int rowCount = 0;
		
		long startTime = System.currentTimeMillis();
		printColumnNames(metaData);
		while(rs.next()) {
			rowCount++;
			
			System.out.print("Row #" + rowCount + ": ");
			for(int i = 1; i <= colCount; i++) {
				if(metaData.getColumnType(i) == Types.VARCHAR || metaData.getColumnType(i) == Types.CHAR) {
					System.out.print(rs.getString(i) + " | ");
				} else if(metaData.getColumnType(i) == Types.FLOAT
						|| metaData.getColumnType(i) == Types.DECIMAL) {
					System.out.printf("%.2f %s", rs.getDouble(i), " | ");
				} else if(metaData.getColumnType(i) == Types.INTEGER 
						|| metaData.getColumnType(i) == Types.BIGINT) {
					System.out.printf("%d %s", rs.getInt(i), " | ");
				} 
			}
			System.out.println();
			
			if(ROW_LIMIT > 0 && rowCount >= ROW_LIMIT) break;
		}
		long endTime = System.currentTimeMillis();
		
		System.out.println("Processed " + rowCount + " rows in " + (endTime - startTime) + "ms");
	}
	
	public void close(Connection conn, ResultSet rs) throws SQLException {
		rs.close();
		conn.close();
	}
	
	private void printColumnNames(ResultSetMetaData rsm) throws SQLException {
		for(int i = 1; i <= rsm.getColumnCount(); i++) {
			System.out.print(rsm.getColumnLabel(i) + "[" + rsm.getColumnTypeName(i) + "(" + rsm.getPrecision(i) +")] | ");
		}
		System.out.println();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connection conn = null;
		
		try {
			if(!(args.length >= 3)) {
				System.out.println("Usage: TestAlenka <JDBC_DRIVER_CLASS> <JDBC_URL> <QUERY_FILE_NAME1> <QUERY_FILE_NAME2> ..");
				System.exit(1);
			}
			
			String driver = args[0];
			String url = args[1];
			
			//Anything after arg[1] will be the query files to execute, if there are more than one
			//execute them serially 
			TestAlenka alenka = new TestAlenka();
			conn = alenka.initializeJDBC(driver, url);
			
			System.out.println("There are " + (args.length - 2) + " queries to execute");
			int queryNum = 0;
			for(int i = 2; i < args.length; i++) {
				queryNum = i - 1;
				System.out.println("Executing Query #" + queryNum);
				String queryFile = args[i];
				
				ResultSet rs = null;
				
				try {
					long startTime = System.currentTimeMillis();
					rs = alenka.executeQuery(conn, queryFile);
					alenka.processResults(rs);
					long endTime = System.currentTimeMillis();
					
					System.out.println("Query #" + queryNum + " took " + (endTime-startTime) + "ms to process");
				} catch(SQLException e) {
					System.out.println("Error while executing query #" + queryNum);
					e.printStackTrace();
				} finally {
					try { rs.close(); } catch(SQLException e) {}
				}
			}
		} catch(SQLException se) {
			se.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try { if(conn != null) conn.close(); } catch(SQLException c) {}
			System.exit(1);
		}
	}
}
