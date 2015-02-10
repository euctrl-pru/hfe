CREATE OR REPLACE PACKAGE PRUTEST.Hfe
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