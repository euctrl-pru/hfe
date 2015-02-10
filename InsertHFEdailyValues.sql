/* Formatted on 17-12-2014 14:06:39 (QP5 v5.240.12305.39446) */
DECLARE
   start_date   DATE := '01-jan-2015';
   end_date     DATE := '31-jan-2015';
   curr_day     DATE;
   processed    INTEGER;

   PROCEDURE log_note (MESSAGE IN VARCHAR2)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO HFE_log
           VALUES (SYSTIMESTAMP, MESSAGE);

      COMMIT;
   END log_note;

   FUNCTION days_fully_loaded (entry_day_in IN DATE)
      RETURN BOOLEAN
   IS
      date_from   DATE := entry_day_in - 1;
      date_to     DATE := entry_day_in + 1;
   BEGIN
      log_note (
            'Checking availability of FSD data from '
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
         RETURN FALSE;
      END IF;
   END days_fully_loaded;

BEGIN
   curr_day := start_date;

   log_note ('Start: ' || start_date || ' End: ' || end_date);

  <<daily_loop>>
   WHILE curr_day <= end_date
   LOOP
      BEGIN
         IF (days_fully_loaded (curr_day))
         THEN
            INSERT INTO HFE_DAILY
                 SELECT curr_day
,                       MODEL_TYPE
,                       ref_area
,                       mes_area
,                       COUNT (DISTINCT sam_id) flights
,                       COUNT (*) entries
,                       SUM (NX_FLOWN_KM) flown_km
,                       SUM (NX_DIRECT_KM) direct_km
,                       SUM (NX_ACHIEVED_KM) achieved_km
                   FROM HFE_RESULTS
                  WHERE entry_time >= curr_day AND entry_time < (curr_day + 1)
               GROUP BY curr_day
,                       MODEL_TYPE
,                       ref_area
,                       mes_area;

            processed := SQL%ROWCOUNT;
            log_note (
                  'Processed: '
               || curr_day
               || ' ('
               || processed
               || ' rows inserted)');
         END IF;
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            hfe.log_note ('Skipped duplicate: ' || curr_day);
      END;


      curr_day := curr_day + 1;
      COMMIT;
   END LOOP daily_loop;

   log_note ('Completed (' || start_date || ' to ' || end_date || ')');
END;