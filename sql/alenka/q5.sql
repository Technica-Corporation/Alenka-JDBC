RF := FILTER region BY r_name == "AFRICA";
OFI := FILTER orders BY o_orderdate >= 19950101 AND o_orderdate < 19960101;
			 
J1 := SELECT c_nationkey AS c_nationkey, n_name AS n_name, suppkey AS suppkey, price AS price, discount AS discount
      FROM lineitem JOIN OFI ON orderkey = o_orderkey
             JOIN customer ON o_custkey = c_custkey
             JOIN nation ON c_nationkey = n_nationkey
	         JOIN RF ON n_regionkey = r_regionkey;
			 
J2 := SELECT n_name AS n_name, price AS price, discount AS discount
      FROM J1 JOIN supplier ON suppkey = s_suppkey AND c_nationkey = s_nationkey;

F := SELECT n_name AS n_name1, SUM(price*(1-discount)) AS revenue FROM J2
      GROUP BY n_name;
	  
RES := ORDER F BY revenue DESC;	  
