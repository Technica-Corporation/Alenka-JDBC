B := FILTER lineitem BY discount >= 0.05 AND discount <= 0.07 AND qty < 24 AND shipdate >= 19950101 AND shipdate < 19960101;
C := SELECT SUM(price*discount) AS revenue FROM B;			
RES := ORDER C by revenue ASC;
