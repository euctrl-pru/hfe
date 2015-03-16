--
-- Distance as calculated by CFMU/NM
-- 
-- Input:
--   lat  latitude in decimal degrees (North is positive)
--   lon  longitude in decimal degrees (East is positive)
--
-- Output
--   distance in km

CREATE OR REPLACE FUNCTION cfmu_dist_km (lon1 IN number,lat1 IN number, lon2 IN number,lat2 IN number) RETURN NUMBER IS



distance NUMBER;
pi CONSTANT BINARY_DOUBLE := 3.14159265359;
deg2rad CONSTANT  BINARY_DOUBLE := pi/180;

BEGIN
  -- 40000 / (2 pi); where 40000 km is (an approximation [40075.017] of the) equatorial circumference of the Earth
  distance :=  (20000 / pi) * ACOS ( 
    round(SIN(lat1 * deg2rad) * SIN(lat2 * deg2rad)
      + COS(lat1 * deg2rad) * COS(lat2 * deg2rad) 
      * COS ((lon2 - lon1) * deg2rad), 15) 
    );
  RETURN  distance;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
  NULL;
  WHEN OTHERS THEN
  -- Consider logging the error and then re-raise
  NULL;
  --RAISE;
END cfmu_dist_km;
/
SHOW ERRORS