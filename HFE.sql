CREATE OR REPLACE PACKAGE         Hfe
IS
   hfe_pkg_version                 CONSTANT NUMBER := 4.9;

   
   /*
   ----- 4.9
   Reverts to no keys used, except GK.
  Uses program to check availability of days instead of rough check on last LOBT available 
   ----- 4.3
   ----- Corrects bug when loading information airspaces of different type but with the same name
   ----- Redefines query in "difference in ftfm" to take advantage of index
   ----- 4.2
   ----- Adds checks on code ranges (<GK_FIR_RESERVED_RANGE for FIR) and LOBT times (23:59:00 as indication all loaded in fsd tables)
   ----- Adds error handling
   */

   /************************************
   Produces:
      HFE_FLIGHTS             to store information by flight
      HFE_SPACES              to store airspace information and gaps
      HFE_LOG                     to store logs

   ************************************/

   TYPE area_rec_t IS TABLE OF integer; --area_rec;

   TYPE parameters_rec IS RECORD
   (
      model_type     HFE_FLIGHTS.model_type%TYPE
,     day            DATE
,     block_type     HFE_AREA_DEFINITIONS.block_type%TYPE
,     min_sam_id     INTEGER
,     max_sam_id     INTEGER
,     ref_area_gk_id    integer
,     ref_area_set   area_group
,     mes_areas      area_rec_t
   );

   curr_parameters                          parameters_rec;

   GK_FIR_RESERVED_RANGE           CONSTANT SIMPLE_INTEGER := 200000;
   Ok_status                       CONSTANT SIMPLE_INTEGER := 1;
   Inconsistent_time_space         CONSTANT SIMPLE_INTEGER := 3;
   Inconsistent_asp_sequence       CONSTANT SIMPLE_INTEGER := 5;
   Difference_in_ftfm              CONSTANT SIMPLE_INTEGER := 7;
   Apt_too_close                   CONSTANT SIMPLE_INTEGER := 11;
   No_en_route                     CONSTANT SIMPLE_INTEGER := 13;
   Inconsistent_asp_tma_sequence   CONSTANT SIMPLE_INTEGER := 17;

   Tma_radius_nm                   CONSTANT SIMPLE_INTEGER := 40;
   From_nm_to_km_mult              CONSTANT SIMPLE_FLOAT := 1.852;
   Ftfm_literal                    CONSTANT HFE_FLIGHTS.Model_type%TYPE
                                               := 'FTFM' ;
   Cpf_literal                     CONSTANT HFE_FLIGHTS.Model_type%TYPE
                                               := 'CPF' ;
   Apt_literal                     CONSTANT HFE_FLIGHTS.Origin_type%TYPE
                                               := 'APT' ;
   Border_literal                  CONSTANT HFE_FLIGHTS.Origin_type%TYPE
                                               := 'BDR' ;
   Tma_literal                     CONSTANT HFE_FLIGHTS.Enroute_start_type%TYPE
      := 'TMA' ;
   Dep_tma_letter                  CONSTANT VARCHAR2 (1) := 'F';
   Arr_tma_letter                  CONSTANT VARCHAR2 (1) := 'L';
   Default_models_literal          CONSTANT VARCHAR2 (3) := 'DEF';
   Gap_special_code                CONSTANT INTEGER := 0;

   start_elapsed                            TIMESTAMP := SYSTIMESTAMP;
   start_timed                              TIMESTAMP := SYSTIMESTAMP;
   elapsed_message                          VARCHAR2 (32767);
   timed_message                            VARCHAR2 (32767);


   PROCEDURE Load_hfe_day (
      Date_in          IN DATE
,     Model_in         IN HFE_FLIGHTS.Model_type%TYPE := Default_models_literal
,     Ref_tabname_in   IN VARCHAR2
,     Mes_tabname_in   IN VARCHAR2
,     ad_hoc_mode      IN BOOLEAN := FALSE);

   PROCEDURE log_note (MESSAGE IN VARCHAR2);

   PROCEDURE log_time (log_type IN INTEGER, MESSAGE IN VARCHAR2 := NULL);

   PROCEDURE raise_hfe_error (keepgoing IN BOOLEAN);
END Hfe;
/


CREATE OR REPLACE PACKAGE BODY         Hfe
IS
   /*********************************************
   See package body for dependencies
   **********************************************/
   FUNCTION Parameters_string
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (   'Parameters: '
              || Curr_parameters.DAY
              || ' * '
              || Curr_parameters.Model_type
              || ' * '
              || Curr_parameters.Block_type
              || ' * '
              || Curr_parameters.Ref_area_gk_id
              || ' * Sam_ids: '
              || Curr_parameters.min_sam_id
              || '-'
              || Curr_parameters.max_sam_id);
   END Parameters_string;

   PROCEDURE Insert_fact_flights_status
   IS
      /*++++++++++++++++++++++++++++++++++++++++++++++
          Generates the basic information for all flights for which ALL the information
          is available (i.e., both circles and airspaces)
      +++++++++++++++++++++++++++++++++++++++++++++++*/
      Log_time_l   DATE;

   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Loading flights ' || Parameters_string ());

      INSERT INTO HFE_FLIGHTS (GK_REF_AREA_ID
,                                  SAM_ID
,                                  MODEL_TYPE
,                                  STATUS
,                                  ADEP
,                                  ADES
,                                  APT_GCD_DIST_KM
,                                  TRAJ_LENGTH_KM
,                                  OD_GCD_DIST_KM
,                                  ADEP_TIME
,                                  DTMA40_TIME
,                                  ATMA40_TIME
,                                  ADES_TIME
,                                  ORIGIN_TYPE
,                                  ORIGIN_LOC
,                                  ORIGIN_TIME
,                                  ORIGIN_DIST_KM
,                                  ORIGIN_LAT
,                                  ORIGIN_LON
,                                  DESTINATION_TYPE
,                                  DESTINATION_LOC
,                                  DESTINATION_TIME
,                                  DESTINATION_DIST_KM
,                                  DESTINATION_LAT
,                                  DESTINATION_LON
,                                  ENROUTE_START_TYPE
,                                  ENROUTE_START_LOC
,                                  ENROUTE_START_TIME
,                                  ENROUTE_START_DIST_KM
,                                  ENROUTE_START_LAT
,                                  ENROUTE_START_LON
,                                  ENROUTE_END_TYPE
,                                  ENROUTE_END_LOC
,                                  ENROUTE_END_TIME
,                                  ENROUTE_END_DIST_KM
,                                  ENROUTE_END_LAT
,                                  ENROUTE_END_LON
,                                  LAST_TOUCHED
,                                  HFE_PKG_VERSION)
         WITH Od_data
              AS ( /***************
                       OD_DATA provides information on the origin and destination.
                           - origin is the FIRST entry into the REFERENCE AREA
                           - destination is the LAST exit from the REFERENCE AREA
                       First and last are based on SEQ_ID values (consistency with order by distance and time is verified in separate functions)
                       NB the windowing with UNBOUNDED for the last value -- the default would take the current row and give wrong results
                   ***************/
                  SELECT DISTINCT
                         /* All rows for the same sam_id will be equal */
                         Model_type
,                        Sam_id
,                        FIRST_VALUE (
                            Airspace_id)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            Origin_fir
,                        FIRST_VALUE (
                            Entry_time)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            Origin_time
,                        FIRST_VALUE (
                            Entry_dist)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            Origin_dist_km
,                        FIRST_VALUE (
                            Entry_lat)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            Origin_lat
,                        FIRST_VALUE (
                            Entry_lon)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            Origin_lon
,                        LAST_VALUE (
                            Airspace_id)
                         OVER (
                            PARTITION BY Lobt, Model_type, Sam_id
                            ORDER BY Seq_id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND     UNBOUNDED FOLLOWING)
                            Destination_fir
,                        LAST_VALUE (
                            Exit_time)
                         OVER (
                            PARTITION BY Lobt, Model_type, Sam_id
                            ORDER BY Seq_id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND     UNBOUNDED FOLLOWING)
                            Destination_time
,                        LAST_VALUE (
                            Exit_dist)
                         OVER (
                            PARTITION BY Lobt, Model_type, Sam_id
                            ORDER BY Seq_id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND     UNBOUNDED FOLLOWING)
                            Destination_dist_km
,                        LAST_VALUE (
                            Exit_lat)
                         OVER (
                            PARTITION BY Lobt, Model_type, Sam_id
                            ORDER BY Seq_id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND     UNBOUNDED FOLLOWING)
                            Destination_lat
,                        LAST_VALUE (
                            Exit_lon)
                         OVER (
                            PARTITION BY Lobt, Model_type, Sam_id
                            ORDER BY Seq_id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND     UNBOUNDED FOLLOWING)
                            Destination_lon
                    FROM FSD.ALL_FT_ASP_PROFILE N
                   WHERE     N.Lobt >= curr_parameters.DAY
                         AND N.Lobt < curr_parameters.DAY + 1
                         AND N.Model_type = curr_parameters.Model_type
                         AND airspace_type = curr_parameters.Block_type
                         AND N.airspace_id
                                 MEMBER OF curr_parameters.ref_area_set)
,             Circle_data
              AS ( /*
                       circle_DATA provides information on
                           - the arrival and departure airports
                           - the intersection with the TMA cylindeer
                   */
                  SELECT                                                --Lobt
                        Sam_id
,                        Model_type
,                        PRUTEST.Cfmu_dist_km (Dep.Entry_lon
,                                              Dep.Entry_lat
,                                              Arr.Exit_lon
,                                              Arr.Exit_lat)
                            Apt_gcd_dist_km
,                        Dep.Airport_icao_code Adep
,                        Dep.Entry_time Adep_time
,                        Dep.Entry_dist Adep_dist_km
,                        Dep.Entry_lat Adep_lat
,                        Dep.Entry_lon Adep_lon
,                        Dep.Exit_time Dtma_time
,                        Dep.Exit_dist Dtma_dist_km
,                        Dep.Exit_lat Dtma_lat
,                        Dep.Exit_lon Dtma_lon
,                        Arr.Entry_time Atma_time
,                        Arr.Entry_dist Atma_dist_km
,                        Arr.Entry_lat Atma_lat
,                        Arr.Entry_lon Atma_lon
,                        Arr.Exit_time Ades_time
,                        Arr.Exit_dist Ades_dist_km
,                        Arr.Exit_lat Ades_lat
,                        Arr.Exit_lon Ades_lon
,                        Arr.Airport_icao_code Ades
                    FROM FSD.ALL_FT_CIRCLE_PROFILE Dep
                         JOIN FSD.ALL_FT_CIRCLE_PROFILE Arr
                            USING (Lobt, Model_type, Sam_id)
                   WHERE     Lobt >= curr_parameters.DAY
                         AND Lobt < curr_parameters.DAY + 1
                         AND Model_type = curr_parameters.Model_type
                         AND Dep.Airspace_id =
                                Dep_tma_letter || Tma_radius_nm
                         AND Arr.Airspace_id =
                                Arr_tma_letter || Tma_radius_nm)
         SELECT curr_parameters.Ref_area_gk_id
,               Sam_id
,               Model_type
,               CASE
                   -- Checks that the airports are at least "two circles" away */
                   WHEN Apt_gcd_dist_km <=
                           2 * Tma_radius_nm * From_nm_to_km_mult
                   THEN
                      Apt_too_close
                   -- Greatest gives the beginning of en route, while least is the end of en route */
                   WHEN GREATEST (Origin_time, Dtma_time) >=
                           LEAST (Destination_time, Atma_time)
                   THEN
                      No_en_route
                   ELSE
                      Ok_status
                END
                   Status -- Initial value -- might be modified by later checks
,               Adep
,               Ades
,               Apt_gcd_dist_km
,               Ades_dist_km Traj_length
,               PRUTEST.Cfmu_dist_km (Origin_lon
,                                     Origin_lat
,                                     Destination_lon
,                                     Destination_lat)
                   Od_gcd_dist_km
,               Adep_time
,               Dtma_time
,               Atma_time
,               Ades_time
,               CASE
                   WHEN Origin_time = Adep_time THEN Apt_literal
                   ELSE Border_literal
                END
                   Origin_type
,               CASE
                   WHEN Origin_time = Adep_time THEN Adep
                   ELSE Origin_fir
                END
                   Origin_loc
,               Origin_time
,               Origin_dist_km
,               Origin_lat
,               Origin_lon
,               CASE
                   WHEN Destination_time = Ades_time THEN Apt_literal
                   ELSE Border_literal
                END
                   Destination_type
,               CASE
                   WHEN Destination_time = Ades_time THEN Ades
                   ELSE Destination_fir
                END
                   Destination_loc
,               Destination_time
,               Destination_dist_km
,               Destination_lat
,               Destination_lon
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Tma_literal
                   ELSE Border_literal
                END
                   Enroute_start_type
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Adep || Tma_radius_nm
                   ELSE Origin_fir
                END
                   Enroute_start_loc
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Dtma_time
                   ELSE Origin_time
                END
                   Enroute_start_time
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Dtma_dist_km
                   ELSE Origin_dist_km
                END
                   Enroute_start_dist_km
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Dtma_lat
                   ELSE Origin_lat
                END
                   Enroute_start_lat
,               CASE
                   WHEN Origin_time <= Dtma_time THEN Dtma_lon
                   ELSE Origin_lon
                END
                   Enroute_start_lon
,               CASE
                   WHEN Destination_time >= Atma_time THEN Tma_literal
                   ELSE Border_literal
                END
                   Enroute_end_type
,               CASE
                   WHEN Destination_time >= Atma_time
                   THEN
                      Ades || Tma_radius_nm
                   ELSE
                      Destination_fir
                END
                   Enroute_end_loc
,               CASE
                   WHEN Destination_time >= Atma_time THEN Atma_time
                   ELSE Destination_time
                END
                   Enroute_end_time
,               CASE
                   WHEN Destination_time >= Atma_time THEN Atma_dist_km
                   ELSE Destination_dist_km
                END
                   Enroute_end_dist_km
,               CASE
                   WHEN Destination_time >= Atma_time THEN Atma_lat
                   ELSE Destination_lat
                END
                   Enroute_end_dist_km
,               CASE
                   WHEN Destination_time >= Atma_time THEN Atma_lon
                   ELSE Destination_lon
                END
                   Enroute_end_lon
,               Log_time_l
,               HFE_PKG_VERSION
           FROM Od_data JOIN Circle_data USING (Model_type, Sam_id)
          WHERE                           /* Might filter out more flights  */
               Adep NOT IN ('AFIL', 'ZZZZ') AND Ades NOT IN ('AFIL', 'ZZZZ');
      
      log_note (SQL%ROWCOUNT || ' flights inserted');
      log_time (2);
   END Insert_fact_flights_status;

   PROCEDURE Verify_asp_sequence
   IS
      /*++++++++++++++++++++++++++++++++++++++++++++++
          Verifies consistency of information, i.e., same ordering of seq_id, times, distances
          The check concerns all the trajectory, not only the part within the reference area
      +++++++++++++++++++++++++++++++++++++++++++++++*/
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Verifying asp sequences ' || Parameters_string ());

      UPDATE HFE_FLIGHTS
         SET Status = Status * Inconsistent_asp_sequence
,            Last_touched = Log_time_l
       WHERE (                                                       /*Lobt,*/
              Model_type, GK_REF_AREA_ID, Sam_id) IN
                (SELECT                                                --Lobt,
                       Model_type, curr_parameters.ref_area_gk_id, Sam_id
                   FROM (SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               CASE
                                   WHEN        /* Single area inconsistency */
                                       Entry_dist > Exit_dist
                                        OR Entry_time > Exit_time
                                        /* Bordering areas inconsistency */
                                        OR Exit_time <
                                              LAG (
                                                 Entry_time)
                                              OVER (
                                                 PARTITION BY Lobt
,                                                             Sam_id
,                                                             Model_type
                                                 ORDER BY Seq_id)
                                        OR Exit_dist <
                                              LAG (
                                                 Entry_dist)
                                              OVER (
                                                 PARTITION BY Lobt
,                                                             Sam_id
,                                                             Model_type
                                                 ORDER BY Seq_id)
                                   THEN
                                      1
                                   ELSE
                                      0
                                END
                                   Error_flag
                           FROM FSD.ALL_FT_ASP_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                                AND Airspace_type =
                                       curr_parameters.Block_type)
                  WHERE Error_flag > 0);
      log_note (SQL%ROWCOUNT || ' status updates');
      log_time (2);
   END Verify_asp_sequence;

   PROCEDURE Verify_asp_tma_sequence
   IS
      /*++++++++++++++++++++++++++++++++++++++++++++++
          Verifies consistency of information for the circles with information for the spaces
      +++++++++++++++++++++++++++++++++++++++++++++++*/
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Verifying asp/tma sequences ' || Parameters_string ());

      UPDATE HFE_FLIGHTS
         SET Status = Status * Inconsistent_asp_tma_sequence
,            Last_touched = Log_time_l
       WHERE (                                                       /*Lobt,*/
              Model_type, GK_REF_AREA_ID, Sam_id) IN
                (SELECT                                                --Lobt,
                       Model_type, curr_parameters.ref_area_gk_id, Sam_id
                   FROM (SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               CASE
                                   WHEN     C.Entry_time BETWEEN S.Entry_time
                                                             AND S.Exit_time
                                        AND C.Entry_dist NOT BETWEEN S.Entry_dist
                                                                 AND S.Exit_dist
                                   THEN
                                      1
                                   WHEN     C.Exit_time BETWEEN S.Entry_time
                                                            AND S.Exit_time
                                        AND C.Exit_dist NOT BETWEEN S.Entry_dist
                                                                AND S.Exit_dist
                                   THEN
                                      1
                                   ELSE
                                      0
                                END
                                   Error_flag
                           FROM FSD.ALL_FT_ASP_PROFILE S
                                JOIN FSD.ALL_FT_CIRCLE_PROFILE C
                                   USING (Lobt, Sam_id, Model_type)
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                                AND S.Airspace_type =
                                       curr_parameters.Block_type
                                /* Limit to spaces intervals which overlap circle intervals */
                                AND C.Entry_time <= S.Exit_time
                                AND S.Entry_time <= C.Exit_time)
                  WHERE Error_flag > 0);
      log_note (SQL%ROWCOUNT || ' status updates');
      log_time (2);
   END Verify_asp_tma_sequence;

   PROCEDURE Verify_time_position
   IS
      -- Note: should check if it is possible to use WITH instead of using twice the big query
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Verifying time/position ' || Parameters_string ());

      UPDATE HFE_FLIGHTS
         SET Status = Status * Inconsistent_time_space
,            Last_touched = Log_time_l
       WHERE (                                                       /*Lobt,*/
              Model_type, GK_REF_AREA_ID, Sam_id) IN
                (SELECT DISTINCT                                       --Lobt,
                        Model_type, curr_parameters.ref_area_gk_id, Sam_id
                   FROM (SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Entry_time Time_over
,                               Entry_dist Distance
                           FROM FSD.ALL_FT_ASP_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Airspace_type =
                                       curr_parameters.Block_type
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Exit_time
,                               Exit_dist
                           FROM FSD.ALL_FT_ASP_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Airspace_type =
                                       curr_parameters.Block_type
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Entry_time
,                               Entry_dist
                           FROM FSD.ALL_FT_CIRCLE_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Exit_time
,                               Exit_dist
                           FROM FSD.ALL_FT_CIRCLE_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Time_over
,                               Point_dist
                           FROM FSD.ALL_FT_POINT_PROFILE P
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type) T1
                        JOIN
                        (SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Entry_time Time_over
,                               Entry_dist Distance
                           FROM FSD.ALL_FT_ASP_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Airspace_type =
                                       curr_parameters.Block_type
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Exit_time
,                               Exit_dist
                           FROM FSD.ALL_FT_ASP_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Airspace_type =
                                       curr_parameters.Block_type
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                        --Lobt,
                               Sam_id
,                               Model_type
,                               Entry_time
,                               Entry_dist
                           FROM FSD.ALL_FT_CIRCLE_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                         --Lobt
                               Sam_id
,                               Model_type
,                               Exit_time
,                               Exit_dist
                           FROM FSD.ALL_FT_CIRCLE_PROFILE
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type
                         UNION
                         SELECT                                         --Lobt
                               Sam_id
,                               Model_type
,                               Time_over
,                               Point_dist
                           FROM FSD.ALL_FT_POINT_PROFILE P
                          WHERE     Lobt >= curr_parameters.DAY
                                AND Lobt < curr_parameters.DAY + 1
                                AND Model_type = curr_parameters.Model_type) T2
                           USING (                                   /*Lobt,*/
                                  Sam_id, Model_type, Time_over)
                  WHERE T1.Distance > T2.Distance);
      log_note (SQL%ROWCOUNT || ' status updates');
      log_time (2);
   END Verify_time_position;

   PROCEDURE Verify_ftfm
   IS
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Verifying ftfm presence ' || Parameters_string ());

      UPDATE HFE_FLIGHTS G
         SET Status = Status * Difference_in_ftfm, Last_touched = Log_time_l
       WHERE (Sam_id, Model_type, GK_REF_AREA_ID) IN
                (SELECT G.Sam_id, G.Model_type, G.GK_REF_AREA_ID
                   FROM HFE_FLIGHTS G
                        LEFT JOIN
                        HFE_FLIGHTS F
                           ON (    F.Sam_id = G.Sam_id
                               AND F.Model_type = Ftfm_literal
                               AND f.GK_REF_AREA_ID = g.GK_REF_AREA_ID)
                  WHERE     g.sam_id BETWEEN curr_parameters.min_sam_id
                                         AND curr_parameters.max_sam_id
                        AND G.Model_type = curr_parameters.Model_type
                        AND g.GK_REF_AREA_ID = curr_parameters.ref_area_gk_id
                        AND (   F.Ades IS NULL
                             OR G.Ades <> F.Ades
                             OR G.Adep <> F.Adep));
      log_note (SQL%ROWCOUNT || ' status updates');
      log_time (2);
   END Verify_ftfm;

   PROCEDURE Insert_fact_gaps (curr_parameters IN parameters_rec)
   IS
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Inserting gaps information ' || Parameters_string ());

      INSERT INTO HFE_SPACES
         /* This inserts the gaps */
         WITH Find_gap
              AS (SELECT Lobt
,                        Model_type
,                        Sam_id
,                        CASE
                            WHEN Exit_time <
                                    LEAD (
                                       Entry_time)
                                    OVER (
                                       PARTITION BY Lobt, Model_type, Sam_id
                                       ORDER BY Seq_id)
                            THEN
                               1
                            ELSE
                               0
                         END
                            Is_gap
,                        Seq_id
,                        Exit_time Start_gap_time
,                        Exit_dist Start_gap_dist_km
,                        Exit_lat Start_gap_lat
,                        Exit_lon Start_gap_lon
,                        LEAD (
                            Entry_time)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            End_gap_time
,                        LEAD (
                            Entry_dist)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            End_gap_dist_km
,                        LEAD (
                            Entry_lat)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            End_gap_lat
,                        LEAD (
                            Entry_lon)
                         OVER (PARTITION BY Lobt, Model_type, Sam_id
                               ORDER BY Seq_id)
                            End_gap_lon
                    FROM FSD.ALL_FT_ASP_PROFILE
                   WHERE     Lobt >= curr_parameters.DAY
                         AND Lobt < curr_parameters.DAY + 1
                         AND Model_type = curr_parameters.Model_type
                         AND Airspace_type = curr_parameters.Block_type)
         SELECT GK_REF_AREA_ID
,               Sam_id
,               Model_type
,               Gap_special_code
,               ROW_NUMBER ()
                OVER (PARTITION BY Lobt, Model_type, Sam_id ORDER BY Seq_id)
,               End_gap_dist_km - Start_gap_dist_km
,               PRUTEST.Cfmu_dist_km (Start_gap_lon
,                                     Start_gap_lat
,                                     End_gap_lon
,                                     End_gap_lat)
,                 (  PRUTEST.Cfmu_dist_km (Start_gap_lon
,                                          Start_gap_lat
,                                          Destination_lon
,                                          Destination_lat)
                   - PRUTEST.Cfmu_dist_km (End_gap_lon
,                                          End_gap_lat
,                                          Destination_lon
,                                          Destination_lat)
                   + PRUTEST.Cfmu_dist_km (Origin_lon
,                                          Origin_lat
,                                          End_gap_lon
,                                          End_gap_lat)
                   - PRUTEST.Cfmu_dist_km (Origin_lon
,                                          Origin_lat
,                                          Start_gap_lon
,                                          Start_gap_lat))
                / 2
,               Start_gap_time
,               Start_gap_dist_km
,               Start_gap_lat
,               Start_gap_lon
,               End_gap_time
,               End_gap_dist_km
,               End_gap_lat
,               End_gap_lon
,               Log_time_l
,               HFE_PKG_VERSION
           FROM Find_gap F JOIN HFE_FLIGHTS H USING (Model_type, Sam_id)
          WHERE     Lobt >= curr_parameters.DAY
                AND Lobt < curr_parameters.DAY + 1
                AND Model_type = curr_parameters.Model_type
                AND F.End_gap_time > H.Enroute_start_time
                AND F.Start_gap_time < H.Enroute_end_time
                AND H.Enroute_start_time < H.Enroute_end_time
                AND H.GK_REF_AREA_ID = curr_parameters.ref_area_gk_id
                AND H.Status = 1
                AND Is_gap = 1;
      log_note (SQL%ROWCOUNT || ' gaps inserted');
      log_time (2);
   END Insert_fact_gaps;

   PROCEDURE Insert_fact_spaces (curr_parameters IN parameters_rec)
   IS
      Log_time_l   DATE;
   BEGIN
      Log_time_l := SYSDATE;
      log_time (1, 'Inserting HFE for spaces ' || Parameters_string ());

      INSERT INTO HFE_SPACES
         /* This inserts the entries (correcting for the case in which the en-route begins or ends within the space */
         WITH Find_entry_exit
              AS (SELECT H.GK_REF_AREA_ID
,                        Lobt
,                        Model_type
,                        Sam_id
,                        M.GK_MES_AREA_ID
,                        Seq_id
,                        CASE
                            WHEN Entry_time =
                                    LAG (
                                       Exit_time)
                                    OVER (
                                       PARTITION BY Lobt
,                                                   Model_type
,                                                   Sam_id
,                                                   GK_MES_AREA_ID
                                       ORDER BY Seq_id)
                            THEN
                               0
                            ELSE
                               1
                         END
                            Is_entry
,                        CASE
                            WHEN Exit_time =
                                    LEAD (
                                       Entry_time)
                                    OVER (
                                       PARTITION BY Lobt
,                                                   Model_type
,                                                   Sam_id
,                                                   GK_Mes_Area_id
                                       ORDER BY Seq_id)
                            THEN
                               0
                            ELSE
                               1
                         END
                            Is_exit
,                        Entry_time
,                        Entry_dist
,                        Entry_lat
,                        Entry_lon
,                        Exit_time
,                        Exit_dist
,                        Exit_lat
,                        Exit_lon
,                        Origin_lat
,                        Origin_lon
,                        Destination_lat
,                        Destination_lon
,                        Enroute_start_time
,                        Enroute_start_dist_km
,                        Enroute_start_lat
,                        Enroute_start_lon
,                        Enroute_end_time
,                        Enroute_end_dist_km
,                        Enroute_end_lat
,                        Enroute_end_lon
                    FROM FSD.ALL_FT_ASP_PROFILE N
                         JOIN HFE_TMP_BLOCKS_MEASURED M
                            ON (N.Airspace_id = M.Block_name)
                         JOIN HFE_FLIGHTS H USING (Model_type, Sam_id)
                   WHERE     Lobt >= curr_parameters.DAY
                         AND Lobt < curr_parameters.DAY + 1
                         AND Model_type = curr_parameters.Model_type
                         AND N.Airspace_type = curr_parameters.Block_type
                         AND N.Exit_time > H.Enroute_start_time
                         AND N.Entry_time < H.Enroute_end_time
                         AND H.Enroute_start_time < H.Enroute_end_time
                         AND H.GK_REF_AREA_ID =
                                curr_parameters.ref_area_gk_id
                         AND H.Status = 1)
,             Entries
              AS (SELECT GK_REF_AREA_ID
,                        Lobt
,                        Model_type
,                        Sam_id
,                        GK_MES_AREA_ID
,                        Is_entry
,                        Is_exit
,                        SUM (
                            Is_entry)
                         OVER (
                            PARTITION BY Lobt
,                                        Model_type
,                                        Sam_id
,                                        GK_MES_AREA_ID
                            ORDER BY Seq_id)
                            Entry_nb
,                        SUM (
                            Is_exit)
                         OVER (
                            PARTITION BY Lobt
,                                        Model_type
,                                        Sam_id
,                                        GK_MES_AREA_ID
                            ORDER BY Seq_id)
                            Exit_nb
,                        CASE
                            WHEN Entry_time <= Enroute_start_time
                            THEN
                               Enroute_start_time
                            ELSE
                               Entry_time
                         END
                            Entry_time
,                        CASE
                            WHEN Entry_time <= Enroute_start_time
                            THEN
                               Enroute_start_dist_km
                            ELSE
                               Entry_dist
                         END
                            Entry_dist_km
,                        CASE
                            WHEN Entry_time <= Enroute_start_time
                            THEN
                               Enroute_start_lat
                            ELSE
                               Entry_lat
                         END
                            Entry_lat
,                        CASE
                            WHEN Entry_time <= Enroute_start_time
                            THEN
                               Enroute_start_lon
                            ELSE
                               Entry_lon
                         END
                            Entry_lon
,                        CASE
                            WHEN Exit_time >= Enroute_end_time
                            THEN
                               Enroute_end_time
                            ELSE
                               Exit_time
                         END
                            Exit_time
,                        CASE
                            WHEN Exit_time >= Enroute_end_time
                            THEN
                               Enroute_end_dist_km
                            ELSE
                               Exit_dist
                         END
                            Exit_dist_km
,                        CASE
                            WHEN Exit_time >= Enroute_end_time
                            THEN
                               Enroute_end_lat
                            ELSE
                               Exit_lat
                         END
                            Exit_lat
,                        CASE
                            WHEN Exit_time >= Enroute_end_time
                            THEN
                               Enroute_end_lon
                            ELSE
                               Exit_lon
                         END
                            Exit_lon
,                        Origin_lat
,                        Origin_lon
,                        Destination_lat
,                        Destination_lon
                    FROM Find_entry_exit
                   WHERE Is_entry + Is_exit > 0)
         SELECT GK_REF_AREA_ID
,               Sam_id
,               Model_type
,               GK_MES_AREA_ID
,               N.Entry_nb
,               X.Exit_dist_km - N.Entry_dist_km Nx_flown_km
,               PRUTEST.Cfmu_dist_km (N.Entry_lon
,                                     N.Entry_lat
,                                     X.Exit_lon
,                                     X.Exit_lat)
                   Nx_direct_km
,                 (  PRUTEST.Cfmu_dist_km (N.Entry_lon
,                                          N.Entry_lat
,                                          X.Destination_lon
,                                          X.Destination_lat)
                   - PRUTEST.Cfmu_dist_km (X.Exit_lon
,                                          X.Exit_lat
,                                          X.Destination_lon
,                                          X.Destination_lat)
                   + PRUTEST.Cfmu_dist_km (N.Origin_lon
,                                          N.Origin_lat
,                                          X.Exit_lon
,                                          X.Exit_lat)
                   - PRUTEST.Cfmu_dist_km (N.Origin_lon
,                                          N.Origin_lat
,                                          N.Entry_lon
,                                          N.Entry_lat))
                / 2
,               N.Entry_time
,               N.Entry_dist_km
,               N.Entry_lat
,               N.Entry_lon
,               X.Exit_time
,               X.Exit_dist_km
,               X.Exit_lat
,               X.Exit_lon
,               Log_time_l
,               HFE_PKG_VERSION
           FROM Entries N
                JOIN
                Entries X
                   USING (GK_REF_AREA_ID
,                         Lobt
,                         Model_type
,                         Sam_id
,                         GK_MES_AREA_ID
)
          WHERE N.Is_entry = 1 AND N.Entry_nb = X.Exit_nb AND X.Is_exit = 1;
      log_note (SQL%ROWCOUNT || ' portions inserted');
      log_time (2);
   END Insert_fact_spaces;



/*   PROCEDURE Add_extra_information
   IS
   BEGIN
      log_time (1, 'Adding extra information ');

      MERGE INTO HFE_FLIGHTS H
           USING SWH_FCT.FAC_FLIGHT F
              ON (    H.Sam_id = F.ID
                  AND f.lobt >= curr_parameters.day
                  AND f.lobt < curr_parameters.day + 1)
      WHEN MATCHED
      THEN
         UPDATE SET
            flt_uid = f.flt_uid
,           SK_AC_TYPE_ID =
               (SELECT SK_AC_TYPE_ID
                  FROM SWH_FCT.DIM_AIRCRAFT_TYPE dt
                 WHERE     dt.ec_type_code = f.Aircraft_type_icao_id
                       AND curr_parameters.day >= dt.valid_from
                       AND curr_parameters.day < dt.valid_to)
,           BK_AC_TYPE_ID =
               (SELECT BK_AC_TYPE_ID
                  FROM SWH_FCT.DIM_AIRCRAFT_TYPE dt
                 WHERE     dt.ec_type_code = f.Aircraft_type_icao_id
                       AND curr_parameters.day >= dt.valid_from
                       AND curr_parameters.day < dt.valid_to)
,           sk_Ac_operator_id =
               (SELECT SK_AC_OP_ID
                  FROM pru_hfe.DIM_AIRCRAFT_OPERATOR dt
                 WHERE     DT.AC_OP_CODE = f.Aircraft_operator
                       AND curr_parameters.day >= dt.valid_from
                       AND curr_parameters.day < dt.valid_to)
,           bk_Ac_operator_id =
               (SELECT bK_AC_OP_ID
                  FROM pru_hfe.DIM_AIRCRAFT_OPERATOR dt
                 WHERE     DT.AC_OP_CODE = f.Aircraft_operator
                       AND curr_parameters.day >= dt.valid_from
                       AND curr_parameters.day < dt.valid_to);

      UPDATE HFE_FLIGHTS H
         SET (SK_ADEP_ID, BK_ADEP_ID) =
                (SELECT SK_AP_ID, BK_AP_ID
                   FROM SWH_FCT.DIM_AIRPORT dt
                  WHERE     DT.CFMU_AP_CODE = H.adep
                        AND curr_parameters.day >= dt.valid_from
                        AND curr_parameters.day < dt.valid_to)
       WHERE sam_id BETWEEN curr_parameters.min_sam_id
                        AND curr_parameters.max_sam_id;

      UPDATE HFE_FLIGHTS H
         SET (SK_ADES_ID, BK_ADES_ID) =
                (SELECT SK_AP_ID, BK_AP_ID
                   FROM SWH_FCT.DIM_AIRPORT dt
                  WHERE     DT.CFMU_AP_CODE = H.ades
                        AND curr_parameters.day >= dt.valid_from
                        AND curr_parameters.day < dt.valid_to)
       WHERE sam_id BETWEEN curr_parameters.min_sam_id
                        AND curr_parameters.max_sam_id;

      log_time (2);
   END Add_extra_information;
*/
   FUNCTION get_stored_gk_id (set_in IN area_group)
      RETURN INTEGER
   IS
      retrieved   HFE_AREA_DEFINITIONS%ROWTYPE;
   BEGIN
      FOR retrieved IN (SELECT * FROM HFE_AREA_DEFINITIONS)
      LOOP
         IF set_in = retrieved.members
         THEN
            RETURN retrieved.gk_id;
         END IF;
      END LOOP;

      RETURN 0;
   END get_stored_gk_id;

   FUNCTION Airspace_definitions_ok (Ref_tabname_in   IN VARCHAR2
,                                    Mes_tabname_in   IN VARCHAR2
,                                    keep_going       IN BOOLEAN)
      RETURN BOOLEAN
   IS
      TYPE verifier_rec IS RECORD
      (
         gk_id               INTEGER
,        block_type          HFE_AREA_DEFINITIONS.block_type%TYPE
,        m_block_type        INTEGER
,        num_blocks          INTEGER
,        stored_block_type   HFE_AREA_DEFINITIONS.block_type%TYPE
      );

      TYPE verifier_t IS TABLE OF verifier_rec;

      tmp            verifier_t;
      proposed_set   HFE_AREA_DEFINITIONS.members%TYPE;
      stored_set     HFE_AREA_DEFINITIONS.members%TYPE;
      area           VARCHAR2 (30);
      check_gk       INTEGER;
   BEGIN
      log_time (1, 'Checking airspace definitions');

      ------------------------------ REFERENCE AREA

      BEGIN
         EXECUTE IMMEDIATE
               'select gk_id'
            || ', max(i.block_type), count(distinct i.block_type)'
            || ', count(block_name)'
            || ', max(h.block_type)'
            || ' from '
            || ref_tabname_in
            || ' i left join HFE_AREA_DEFINITIONS h using(gk_id) '
            || ' where :1 between use_from and use_to'
            || ' group by gk_id order by gk_id'
            BULK COLLECT INTO tmp
            USING curr_parameters.day;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            log_note ('ERROR: No information found for REFERENCE AREA');
            RETURN FALSE;
      END;


      IF tmp.COUNT > 1
      THEN
         log_note ('ERROR: more than one gk_id in REFERENCE AREA definition');
         RETURN FALSE;
      END IF;

      FOR indx IN tmp.FIRST .. tmp.LAST     --should be one for reference area
      LOOP
         area := 'REFERENCE AREA ' || tmp (indx).gk_id;

         --log_note (area);

         IF (tmp (indx).m_block_type > 1)
         THEN
            log_note (
                  'ERROR: multiple block types in '
               || area
               || ' definition');
            RETURN FALSE;
         END IF;

         IF (tmp (indx).block_type <> tmp (indx).stored_block_type)
         THEN
            log_note (
                  'ERROR: block type for '
               || area
               || ' different from the one stored');
            RETURN FALSE;
         END IF;

         IF (    tmp (indx).block_type <> 'FIR'
             AND tmp (indx).gk_id < GK_FIR_RESERVED_RANGE)
         THEN
            log_note (
                  'ERROR: '
               || tmp (indx).gk_id
               || ' group code reserved for FIRs');
            RETURN FALSE;
         END IF;

         EXECUTE IMMEDIATE
               'select block_name from '
            || ref_tabname_in
            || ' where :1 between use_from and use_to'
            || ' and gk_id = :2'
            BULK COLLECT INTO proposed_set
            USING curr_parameters.day, tmp (indx).gk_id;

         IF tmp (indx).stored_block_type IS NOT NULL
         THEN
            SELECT members
              INTO stored_set
              FROM HFE_AREA_DEFINITIONS
             WHERE gk_id = tmp (indx).gk_id;

            IF proposed_set != stored_set
            THEN
               log_note (
                     'ERROR: set for '
                  || area
                  || ' different from the one stored');
               RETURN FALSE;
            END IF;
         ELSE
            check_gk := get_stored_gk_id (proposed_set);

            IF check_gk > 0
            THEN
               log_note (
                     'ERROR: set for '
                  || area
                  || ' already stored with different gk_id: '
                  || check_gk);
               RETURN FALSE;
            ELSE
               INSERT INTO HFE_AREA_DEFINITIONS
                    VALUES (
                              tmp (indx).gk_id
,                             tmp (indx).block_type
,                             proposed_set);
            END IF;
         END IF;

         curr_parameters.block_type := tmp (indx).block_type;
         curr_parameters.ref_area_gk_id := tmp (indx).gk_id;

         curr_parameters.ref_area_set := proposed_set;
      END LOOP;

      ------------------------------ MEASURED AREAS

      BEGIN
         EXECUTE IMMEDIATE
               'select gk_id'
            || ', max(i.block_type), count(distinct i.block_type)'
            || ', count(block_name)'
            || ', max(h.block_type)'
            || ' from '
            || mes_tabname_in
            || ' i left join HFE_AREA_DEFINITIONS h using(gk_id) '
            || ' where :1 between use_from and use_to'
            || ' group by gk_id order by gk_id'
            BULK COLLECT INTO tmp
            USING curr_parameters.day;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            log_note ('ERROR: No information found for MEASURED AREA');
            RETURN FALSE;
      END;

     <<areas_loop>>
      FOR indx IN tmp.FIRST .. tmp.LAST
      LOOP
         area := 'MEASURED AREA ' || tmp (indx).gk_id;

         IF (tmp (indx).block_type <> curr_parameters.block_type)
         THEN
            log_note (
                  'WARNING: '
               || area
               || ' skipped -- block type different from the one of the REFERENCE AREA');
            raise_hfe_error (keep_going);
            CONTINUE areas_loop;
         END IF;

         IF (tmp (indx).m_block_type > 1)
         THEN
            log_note (
                  'WARNING: '
               || area
               || ' skipped -- multiple block_types in definition');
            raise_hfe_error (keep_going);
            CONTINUE areas_loop;
         END IF;

         IF (tmp (indx).block_type <> tmp (indx).stored_block_type)
         THEN
            log_note (
                  'WARNING: '
               || area
               || ' skipped -- block type different from the one stored');
            raise_hfe_error (keep_going);
            CONTINUE areas_loop;
         END IF;

         EXECUTE IMMEDIATE
               'select block_name from '
            || mes_tabname_in
            || ' where :1 between use_from and use_to'
            || ' and gk_id = :2'
            BULK COLLECT INTO proposed_set
            USING curr_parameters.day, tmp (indx).gk_id;

         IF tmp (indx).stored_block_type IS NOT NULL
         THEN
            SELECT members
              INTO stored_set
              FROM HFE_AREA_DEFINITIONS
             WHERE gk_id = tmp (indx).gk_id;

            IF proposed_set != stored_set
            THEN
               log_note (
                     'WARNING: '
                  || area
                  || ' skipped -- set different from the one stored');
               raise_hfe_error (keep_going);
               CONTINUE areas_loop;
            END IF;
         ELSE
            check_gk := get_stored_gk_id (proposed_set);

            IF check_gk > 0
            THEN
               log_note (
                     'WARNING: set for '
                  || area
                  || ' skipped -- set already stored with different gk_id: '
                  || check_gk);
               raise_hfe_error (keep_going);
               CONTINUE areas_loop;
            END IF;

            IF (proposed_set NOT SUBMULTISET curr_parameters.ref_area_set)
            THEN
               log_note (
                     'WARNING: '
                  || area
                  || ' skipped -- set not contained in REFERENCE AREA');
               raise_hfe_error (keep_going);
               CONTINUE areas_loop;
            END IF;

            INSERT INTO HFE_AREA_DEFINITIONS
                 VALUES (
                           tmp (indx).gk_id
,                          tmp (indx).block_type
,                          proposed_set);
         END IF;

         curr_parameters.mes_areas.EXTEND;
         curr_parameters.mes_areas (curr_parameters.mes_areas.LAST) :=
            tmp (indx).gk_id;
      END LOOP areas_loop;

      log_note (parameters_string ());

      RETURN TRUE;
   END Airspace_definitions_ok;

   FUNCTION Flights_loaded
      RETURN BOOLEAN
   IS
      Dummy_l   INTEGER;
   BEGIN
      SELECT sam_id
        INTO Dummy_l
        FROM HFE_FLIGHTS
       WHERE     GK_REF_AREA_ID = curr_parameters.ref_area_gk_id
             AND Model_type = curr_parameters.Model_type
             AND sam_id BETWEEN curr_parameters.min_sam_id
                            AND curr_parameters.max_sam_id
             AND ROWNUM = 1;

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END Flights_loaded;

   FUNCTION Area_loaded (curr_parameters   IN parameters_rec
,                        area_in           IN INTEGER)
      RETURN BOOLEAN
   IS
      Dummy_l   INTEGER;
   BEGIN
      --log_note('Looking for ' || area_in);
      SELECT sam_id
        INTO Dummy_l
        FROM HFE_SPACES
       WHERE     entry_time >= curr_parameters.day
             AND entry_time < curr_parameters.day + 1
             AND Model_type = curr_parameters.Model_type
             AND GK_REF_AREA_ID = curr_parameters.ref_area_gk_id
             AND GK_MES_AREA_ID = area_in
             AND sam_id BETWEEN curr_parameters.min_sam_id
                            AND curr_parameters.max_sam_id
             AND ROWNUM = 1;

      --log_note(Dummy_l || ' in area ' || area_in || ' found');
      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         --log_note(area_in || ' not found');
         RETURN FALSE;
   END Area_loaded;


   PROCEDURE Load_flights
   IS
   BEGIN
      IF Flights_loaded ()
      THEN
         log_note (
               'SKIPPED loading flights - data available '
            || Parameters_string ());
         RETURN;
      END IF;

      Insert_fact_flights_status ();
      Verify_asp_sequence ();
      Verify_asp_tma_sequence ();
      Verify_time_position ();

      IF curr_parameters.Model_type <> Ftfm_literal
      THEN
         Verify_ftfm ();
      END IF;

      /*Add_extra_information ();*/
      COMMIT;
   END load_flights;

   PROCEDURE Load_spaces
   IS
      tmp_blocks   area_group;
   BEGIN
      IF NOT Flights_loaded ()
      THEN
         log_note (
               'SKIPPED loading spaces - information on flights not available '
            || Parameters_string ());
         RETURN;
      END IF;

      EXECUTE IMMEDIATE 'truncate table HFE_TMP_BLOCKS_MEASURED';

      IF Area_loaded (curr_parameters, Gap_special_code)
      THEN
         log_note (
               'SKIPPED loading gaps - results are available '
            || Parameters_string ());
      ELSE
         Insert_fact_gaps (curr_parameters);

         INSERT INTO HFE_TMP_BLOCKS_MEASURED
              VALUES ('==GAP=='
,                     Gap_special_code
     );
      END IF;


      FOR indx IN curr_parameters.mes_areas.FIRST ..
                  curr_parameters.mes_areas.LAST
      LOOP
         IF Area_loaded (curr_parameters
,                        curr_parameters.mes_areas (indx))
         THEN
            log_note (
                  'SKIPPED loading measured area '
               || curr_parameters.mes_areas (indx)
               || ' - results are available '
               || Parameters_string ());
         ELSE
            SELECT members
              INTO tmp_blocks
              FROM HFE_AREA_DEFINITIONS
             WHERE gk_id = curr_parameters.mes_areas (indx);

            FORALL idx IN tmp_blocks.FIRST .. tmp_blocks.LAST
               INSERT INTO HFE_TMP_BLOCKS_MEASURED
                    VALUES (tmp_blocks (idx)
,                           curr_parameters.mes_areas (indx));
         END IF;
      END LOOP;

      Insert_fact_spaces (curr_parameters);
      COMMIT;
   END Load_spaces;

   PROCEDURE UpdateMinMaxIds
   --- NB Check only on circle profiles because smaller table which is anyway joined with airspaces when generating flight information
   -- That means that the flight table will never contain sam_ids outsides the values retrieved here
   IS
   BEGIN
      SELECT MIN (sam_id), MAX (sam_id)
        INTO curr_parameters.min_sam_id, curr_parameters.max_sam_id
        FROM FSD.ALL_FT_CIRCLE_PROFILE
       WHERE lobt >= curr_parameters.day AND lobt < curr_parameters.day + 1;
   END UpdateMinMaxIds;

   FUNCTION days_fully_loaded (entry_day_in IN DATE)
      RETURN BOOLEAN
   IS
      date_from   DATE := entry_day_in - 1;
      date_to     DATE := entry_day_in + 1;
   BEGIN
      log_time (
         1
,           'Checking availability of data from '
         || date_from
         || ' to '
         || date_to);

      IF   swh_psm.get_entity_status ('FSD.ALL_FT_CIRCLE'
,                                     date_from
,                                     date_to)
         + swh_psm.get_entity_status ('FSD.ALL_FT_CIRCLE'
,                                     date_from
,                                     date_to)
         + swh_psm.get_entity_status ('FSD.ALL_FT_CIRCLE'
,                                     date_from
,                                     date_to) = 3
      THEN
         RETURN TRUE;
      ELSE
         log_note (
            'ERROR -- data for ' || entry_day_in || ' not fully available');
         log_time (2);
         RETURN FALSE;
      END IF;
   END days_fully_loaded;

   PROCEDURE Load_hfe_day (
      Date_in          IN DATE
,     Model_in         IN HFE_FLIGHTS.Model_type%TYPE := Default_models_literal
,     Ref_tabname_in   IN VARCHAR2
,     Mes_tabname_in   IN VARCHAR2
,     ad_hoc_mode      IN BOOLEAN := FALSE)
   IS
      TYPE Model_types_t IS TABLE OF HFE_FLIGHTS.Model_type%TYPE;

      Models_list   Model_types_t;
   BEGIN
      CASE
         WHEN Model_in = Default_models_literal
         THEN                                        -- Default list of models
            Models_list := Model_types_t (Ftfm_literal, Cpf_literal);
         WHEN Model_in = Ftfm_literal
         THEN
            Models_list := Model_types_t (Ftfm_literal);
         WHEN Model_in IN ('CPF', 'CTFM', 'SCR', 'SRR', 'SUR')
         THEN
            -- As models are compared to FTFM, we make sure it is there -- either computing it now or because it was before (and skipped now)*/
            Models_list := Model_types_t (Ftfm_literal, Model_in);
         ELSE
            log_note ('ERROR: Unrecognised flight model');
            raise_hfe_error (ad_hoc_mode);
      END CASE;

      curr_parameters.day := date_in;
      curr_parameters.model_type := 'N/A';
      curr_parameters.min_sam_id := NULL;
      curr_parameters.max_sam_id := NULL;
      curr_parameters.mes_areas := area_rec_t ();

      log_time (0, 'Day: ' || date_in);

      IF NOT days_fully_loaded (date_in)
      THEN
         raise_hfe_error (ad_hoc_mode);
      ELSIF NOT Airspace_definitions_ok (Ref_tabname_in
,                                        Mes_tabname_in
,                                        ad_hoc_mode)
      THEN
         log_note ('ERROR found -- day skipped');
         log_time (2);
         raise_hfe_error (ad_hoc_mode);
      ELSE
         UpdateMinMaxIds ();

        <<Models_loop>>
         FOR Loop_ndx IN Models_list.FIRST .. Models_list.LAST
         LOOP
            Curr_parameters.Model_type := Models_list (Loop_ndx);
            Load_flights ();
            Load_spaces ();
         END LOOP Models_loop;
      END IF;

      log_time (3);
   END Load_hfe_day;

   PROCEDURE log_note (MESSAGE IN VARCHAR2)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO HFE_log
           VALUES (SYSTIMESTAMP, MESSAGE);

      COMMIT;
   END log_note;

   PROCEDURE log_time (Log_type IN INTEGER, MESSAGE IN VARCHAR2 := NULL)
   /*
       0: start timer
       1: start elapsed time
       2: elapsed time
       3: timed
   */
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      Time_taken       INTERVAL DAY TO SECOND;
      Current_tm       TIMESTAMP := SYSTIMESTAMP;
      Message_logged   VARCHAR2 (1000);
   BEGIN
      CASE Log_type
         WHEN 0
         THEN
            BEGIN
               Start_timed := Current_tm;
               Timed_message := MESSAGE;
               Message_logged := MESSAGE || ' (Start)';
            END;
         WHEN 1
         THEN
            BEGIN
               Start_elapsed := Current_tm;
               Elapsed_message := MESSAGE;
               Message_logged := MESSAGE || ' (Start)';
            END;
         WHEN 2
         THEN
            BEGIN
               Time_taken := Current_tm - Start_elapsed;
               Message_logged :=
                     'Time taken: '
                  || Time_taken
                  || ' ('
                  || NVL (MESSAGE, Elapsed_message)
                  || ')';
            END;
         WHEN 3
         THEN
            BEGIN
               Time_taken := Current_tm - Start_timed;
               Message_logged :=
                     'Time taken: '
                  || Time_taken
                  || ' ('
                  || NVL (MESSAGE, Timed_message)
                  || ')';
            END;
      END CASE;

      log_note (Message_logged);
   END log_time;


   PROCEDURE raise_hfe_error (keepgoing IN BOOLEAN)
   IS
      hfe_error_message   CONSTANT VARCHAR2 (100)
                                      := 'HFE error -- details in HFE_LOG' ;
      hfe_error_code      CONSTANT BINARY_INTEGER := -20999;
   BEGIN
      IF keepgoing
      THEN
         RETURN;
      END IF;

      raise_application_error (hfe_error_code, hfe_error_message);
   END;
END Hfe;
/
