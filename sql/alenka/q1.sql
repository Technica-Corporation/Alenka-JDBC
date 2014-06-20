B := FILTER lineitem BY shipdate <= 19980902;
D := SELECT 
	returnflag AS l_returnflag, 
	linestatus AS l_linestatus, 
	SUM(qty) AS sum_qty, 
	SUM(price) AS sum_base_price, 
	SUM((1-discount)*price) AS sum_disc_price, 
	SUM((1+tax)*(1-discount)*price) AS sum_charge, 
	AVG(qty) AS avg_qty, 
	AVG(price) AS avg_price, 
	AVG(discount) AS avg_disc, 
	COUNT(qty) AS count_order 
FROM B GROUP BY returnflag, linestatus;
RES := ORDER D BY l_returnflag ASC, l_linestatus ASC;
