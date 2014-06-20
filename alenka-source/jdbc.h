/*
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
#include "strings.h"
#include "cm.h"

class JDBC
{
public:
	enum alenka_types {
		alenka_integer = 1,
		alenka_float = 10,
		alenka_char = 100
	};

	CudaSet* cudaSet;

	JDBC(CudaSet* cs);
	
	int getRecordCount();
	int connected();
	int getColumnCount();
	void getColumnNames(char** colNames);
	void getColumnName(int colNum, char* colName);
	alenka_types getColumnTypes(int colNum);
	size_t getColumnSize(int colNum);
	void retrieveRow(int rowNum);

	void retrieveChar(int rowNum, int colNum, char* data);
	long long int retrieveInt(int rowNum, int colNum);
	double retrieveFloat(int rowNum, int colNum);
};
