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

import java.sql.Types;

import org.bridj.IntValuedEnum;
import org.bridj.Pointer;

import com.technicacorp.alenka.JDBC;

public class AlenkaColumn {
	public static final int MAX_COLUMN_SIZE = 128;
	
	private String name;
	private int type;
	private String label;
	private int displaySize;
	private int precision;
	private int scale;
	private String schemaName;
	private String tableName;
	
	/**
	 * Create an empty column
	 * @param n
	 */
	protected AlenkaColumn(String n) {
		name = n;
		type = Types.INTEGER;
		label = n;
		displaySize = 0;
		precision = 0;
		scale = 0;
		schemaName = "";
		tableName = "";
		
	}
	
	protected AlenkaColumn(JDBC alenkaWrapper, int colNum) {
		Pointer<Byte> nativeName = Pointer.allocateBytes(MAX_COLUMN_SIZE);
		alenkaWrapper.getColumnName(colNum - 1, nativeName);
		name = nativeName.getCString();
		label = nativeName.getCString();
		schemaName = "";
		tableName = "";
		
		//Set the type.  Currently Alenka only has three types. 
		//Char is really a VARCHAR
		//Float is a 'decimal' but it doesn't look like it defines the precision
		IntValuedEnum<JDBC.alenka_types> alenkaType = alenkaWrapper.getColumnTypes(colNum - 1);
		if(alenkaType == JDBC.alenka_types.alenka_integer) {
			type = Types.INTEGER;
			precision = 32;
			displaySize = 15;
			scale = 0;
		} else if(alenkaType == JDBC.alenka_types.alenka_float) {
			type = Types.FLOAT;
			precision = 15;
			displaySize = 15;
			scale = 2;
		} else if(alenkaType == JDBC.alenka_types.alenka_char) {
			type = Types.VARCHAR;
			precision = (int)alenkaWrapper.getColumnSize(colNum - 1);
			displaySize = (int)alenkaWrapper.getColumnSize(colNum - 1);
			scale = 0;
			
		}
	}
	
	public String getName() {
		return name;
	}

	public int getType() {
		return type;
	}

	public String getLabel() {
		return label;
	}

	public int getDisplaySize() {
		return displaySize;
	}
	
	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public String toString() {
		return "AlenkaColumn [name=" + name + ", type=" + type + ", label="
				+ label + ", displaySize=" + displaySize + ", precision="
				+ precision + ", scale=" + scale + ", schemaName=" + schemaName
				+ ", tableName=" + tableName + "]";
	}
}
