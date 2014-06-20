#include "jdbc.h"
#include "cm.h"

#ifdef _WIN64
#define atoll(S) _atoi64(S)
#include <windows.h>
#else
#include <unistd.h>
#endif

JDBC::JDBC(CudaSet* cs) {
	cudaSet = cs;
}
;

int JDBC::getRecordCount() {
	return cudaSet->mRecCount;
}
;

int JDBC::getColumnCount() {
	return cudaSet->mColumnCount;
}
;

void JDBC::getColumnNames(char** colNames) {
	for (size_t i = 0; i < cudaSet->columnNames.size(); i++) {
		strcpy(colNames[i], cudaSet->columnNames[i].c_str());
		//cout << colNames[i] << endl;
	}
}
;

void JDBC::getColumnName(int colNum, char* colName) {
	if (colNum < 0 || colNum >= cudaSet->mColumnCount) {
		colName = NULL;
	} else {
		strcpy(colName, cudaSet->columnNames[colNum].c_str());
	}
}
;

JDBC::alenka_types JDBC::getColumnTypes(int colNum) {
	if(cudaSet->type[cudaSet->columnNames[colNum]] == 0) {
		return alenka_integer;
	} else if(cudaSet->type[cudaSet->columnNames[colNum]] == 1){
		return alenka_float;
	} else {
		return alenka_char;
	}
}
;

size_t JDBC::getColumnSize(int colNum) {
	return cudaSet->char_size[cudaSet->columnNames[colNum]];
}
;

void JDBC::retrieveChar(int rowNum, int colNum, char* data) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		data = NULL;
		return;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		data = NULL;
		return;
	}

	strncpy(data, cudaSet->h_columns_char[cudaSet->columnNames[colNum]] + (rowNum*cudaSet->char_size[cudaSet->columnNames[colNum]]), cudaSet->char_size[cudaSet->columnNames[colNum]]);
}

long long int JDBC::retrieveInt(int rowNum, int colNum) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		return 0;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		return 0;
	}

	return (cudaSet->h_columns_int[cudaSet->columnNames[colNum]])[rowNum];
}

double JDBC::retrieveFloat(int rowNum, int colNum) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		return 0;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		return 0;
	}
	return (cudaSet->h_columns_float[cudaSet->columnNames[colNum]])[rowNum];
}
;

//required functions from bison.cu
extern bool scan_state;
extern void process_error(int, string);
extern void clean_queues();

/*
 * Is used to initialize the resultset and prepare what we need to iterate through the results.
 * Passed in string is the name of the variable that we need the result set for.  Probably
 * not the best way but should work for now.
*/
extern "C" CudaSet* initializeResultSet_JDBC(char *f) {
        //Can only get the resultset if we are in scan_state 1
        if (scan_state == 0) {
                process_error(1, "Unable to get resultset in current scan_state");
                return NULL;
        }

        if(varNames.find(f) == varNames.end()) {
                process_error(1, "Unable to find CudaSet");
                clean_queues();
                return NULL;
        }

        CudaSet* cs = varNames.find(f)->second;
        cout << "Found CudaSet for variable " << f << endl;
        return cs;
}

extern "C" void resultSetClose_JDBC() {
        //Clean Up variables
        for (map<string, CudaSet*>::iterator it = varNames.begin();
                        it != varNames.end(); ++it) {
                (*it).second->free();
        };
        varNames.clear();
}
