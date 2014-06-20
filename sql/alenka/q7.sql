LF := FILTER lineitem BY shipdate >= 19950101 AND shipdate <= 19961231;

NF1 := FILTER nation BY n_name == "JAPAN" OR n_name == "BRAZIL";
  
SN := SELECT  s_suppkey AS s_suppkey, n_name AS n_name1
      FROM supplier JOIN NF1 ON s_nationkey = n_nationkey;
  
LJ := SELECT suppkey AS suppkey, price AS price, discount AS discount, shipdate AS shipdate, n_name AS n_name
       FROM LF JOIN orders ON orderkey = o_orderkey
	           JOIN customer ON o_custkey = c_custkey
	           JOIN NF1 ON c_nationkey = n_nationkey;  	
			   
LS := SELECT price AS price, discount AS discount, n_name1 AS n_name1, shipdate AS shipdate, n_name AS n_name
       FROM LJ JOIN SN ON suppkey = s_suppkey;   

LF1 := FILTER LS BY (n_name1 == "JAPAN" AND n_name == "BRAZIL")
				OR (n_name1 == "BRAZIL"  AND n_name == "JAPAN");
				
T := SELECT n_name1 AS n1, n_name AS n2, shipdate/10000 AS shipdate3, price AS price1, discount AS discount1
     FROM LF1;				
	 
G := SELECT n1 AS supp_nation, n2 AS cust_nation, shipdate3 AS one_year, SUM(price1*(1-discount1)) AS revenue
     FROM T
     GROUP BY n1, n2, shipdate3; 		
	 
RES := ORDER G BY supp_nation ASC, cust_nation ASC, one_year ASC;	 
