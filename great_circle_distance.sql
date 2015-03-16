CREATE OR REPLACE FUNCTION great_circle_distance (lon1 IN NUMBER,lat1 IN NUMBER, lon2 IN NUMBER,lat2 IN NUMBER) RETURN BINARY_DOUBLE IS
--
-- Great Circle Distance
-- 
-- Calculates using the special case for the sphere of the Vincenty's formula, see [1]
--
-- Input:
--   lat  latitude [decimal degrees], see [2] (North is positive)
--   lon  longitude [decimal degrees], see [2] (East is positive)
--
-- Output
--   distance [km]
--
-- References
-- [1] http://en.wikipedia.org/wiki/Great-circle_distance
-- [2] http://www.fcc.gov/encyclopedia/degrees-minutes-seconds-tofrom-decimal-degrees

distance BINARY_DOUBLE;
delta_lon BINARY_DOUBLE;
y BINARY_DOUBLE;
x BINARY_DOUBLE;

pi CONSTANT BINARY_DOUBLE := 3.14159265359;
deg2rad CONSTANT  BINARY_DOUBLE := pi/180;

-- equatorial circumference of the Earth
--earth_circumferece CONSTANT BINARY_DOUBLE := 40075.017;
--earth_radius CONSTANT BINARY_DOUBLE := earth_circumferece / (2 * pi);
earth_radius CONSTANT BINARY_DOUBLE := 6378.137;

BEGIN
  delta_lon := ABS(lon2 - lon1);
  y := SQRT(POWER(COS(lat2*deg2rad) * SIN(delta_lon*deg2rad), 2) + 
    POWER(cos(lat1*deg2rad)*SIN(lat2*deg2rad) - SIN(lat1*deg2rad)*COS(lat2*deg2rad)*COS(delta_lon*deg2rad), 2));
  x := SIN(lat1*deg2rad)*SIN(lat2*deg2rad) + COS(lat1*deg2rad)*COS(lat2*deg2rad)*COS(delta_lon*deg2rad);
  distance :=  earth_radius * ATAN2(y, x);

  RETURN  distance;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
  NULL;
  WHEN OTHERS THEN
  -- Consider logging the error and then re-raise
  NULL;
  --RAISE;
END great_circle_distance;
/
SHOW ERRORS