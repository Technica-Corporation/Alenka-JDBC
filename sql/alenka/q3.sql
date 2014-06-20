OFI := FILTER orders BY o_orderdate < 19950315;
CF := FILTER customer BY c_mktsegment == "BUILDING";
LF := FILTER lineitem BY shipdate > 19950315;

OLC := SELECT o_orderkey AS o_orderkey, o_orderdate AS o_orderdate, o_shippriority AS o_shippriority, price AS price, discount AS discount 
       FROM LF JOIN OFI ON orderkey = o_orderkey
	           JOIN CF ON o_custkey = c_custkey;
			  
F := SELECT o_orderkey AS l_orderkey, 
            	SUM(price*(1-discount)) AS revenue, 
		o_orderdate AS o_orderdate, 
		o_shippriority AS o_shippriority
	FROM OLC GROUP BY o_orderkey, o_orderdate, o_shippriority;	
	  
RES := ORDER F BY revenue DESC, o_orderdate ASC;	   
