Alenka-JDBC
===========

Alenka JDBC is a library for accessing and manipulating data with the open-source GPU database Alenka.

http://technica-corporation.github.io/Alenka-JDBC/

The Alenka JDBC driver relies on BridJ (https://code.google.com/p/bridj/) to hook into the Alenka code to make the calls to execute the queries and to retireve the results. 

### How to build 

#####Alenka:
 * Download the code base for the Alenka project from https://github.com/antonmks/Alenka
 * Follow the instructions at the Alenka page on how to build.  Ensure that Alenka builds and functions before continuing.  This includes running the TPC-H data generation and loading these tables into Alenka if you do not have other data to use.
 * Copy the master Alenka Makefile to Makefile.orig and replace the original makefile with the one alenka-source/Makefile.  If any customizations were made to the original makefile, such as updating the CUDA architecture (sm\_20 to sm\_30 for example), make the same changes to the new makefile.
 * Copy the alenka-source/jdbc.cu and alenka-source/jdbc.h files to the Alenka source directory
 * Copy the entire contents of the bison.cu.snipit and paste it to the end of the bison.cu file in the Alenka source directory.  
 * Remove the existing .o files from the Alenka source directory.
 * Make the Alenka source again.  Ensure that the make completes successfully and that the shared library libAlenka.so was created

#####JDBC Driver:
 * Build the driver using ant to create the alenkajdbc.jar file.  Just type `ant` in the make JDBC directory. 

###Run a Test:
 * In order to run the Alenka code, you MUST execute the java command in the same directory that
contains the Alenka shared library and the Alenka data files.  The alenkajdbc.sh file will do that but if you are incorporating the JDBC driver into your own code, keep this in mind. 
 * Edit the bin/alenkajdbc.sh file and set the ALENKA\_JDBC\_HOME to the directory that holds the Alenka JDBC source and ALENKA\_DIR to the directory that contains the Alenka shared library and data files. 
 * run the alenkajdbc.sh file and pass in the full path of one of the Alenka queries in the sql/alenka directory (assuming the TPC-H data has been loaded).
 * NOTE:  The queries passed into Alenka via JDBC should NOT have a STORE or DISPLAY command as those are interpreted by the Alenka engine and it will do the appropriate action.  Instead, just ensure that that last variable contains the result set that you need to process.
 
