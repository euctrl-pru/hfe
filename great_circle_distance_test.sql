-- Test for great circle distance
-- For values see http://www.ga.gov.au/geodesy/datums/vincenty_inverse.jsp
SET SERVEROUTPUT ON
DECLARE
  counter PLS_INTEGER := 0;
  distance BINARY_DOUBLE;
  start_time NUMBER;
  end_time NUMBER;
  elapsed_time NUMBER;
  execution_time NUMBER;
  num_iterations CONSTANT PLS_INTEGER := 1000000; 
BEGIN
  start_time := DBMS_UTILITY.GET_TIME ;
  WHILE counter <= num_iterations LOOP
    select GREAT_CIRCLE_DISTANCE(144.424868, -37.951033, 143.926495, -37.652821) into distance FROM DUAL;
--    select CFMU_DIST_KM(144.424868, -37.951033, 143.926495, -37.652821) into distance FROM DUAL;
    counter := counter + 1;
  END LOOP;
  end_time := DBMS_UTILITY.GET_TIME ;

  elapsed_time := (end_time - start_time)/100;
  execution_time := (elapsed_time / num_iterations) * 1000;

  DBMS_OUTPUT.PUT_LINE('Elapsed Time '|| elapsed_time ||' secs');
  DBMS_OUTPUT.PUT_LINE('(Average) Execution Time '|| execution_time ||' ms');
END;