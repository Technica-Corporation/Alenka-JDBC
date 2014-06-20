#!/bin/bash

ALENKA_JDBC_HOME=.
LIB_DIR=$ALENKA_JDBC_HOME/lib
ALENKA_DIR=/home/test/Alenka

JDBC_DRIVER="com.technicacorp.alenkajdbc.AlenkaDriver"
JDBC_URL="jdbc:alenka:$ALENKA_DIR/libAlenka.so"

PWD=`pwd`
cd $ALENKA_DIR
java -classpath $ALENKA_JDBC_HOME/target/alenkajdbc.jar:$LIB_DIR/bridj-0.7.jar com.technicacorp.alenkajdbc.TestAlenka $JDBC_DRIVER $JDBC_URL $1 
cd $PWD
