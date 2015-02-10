/* Formatted on 15/12/2014 10:59:08 (QP5 v5.240.12305.39446) */
DECLARE
   start_date   DATE := '01-jan-2015';
   end_date     DATE := '31-jan-2015';
   curr_day     DATE;
BEGIN
   curr_day := start_date;
   hfe.log_note ('Start: ' || start_date || ' End: ' || end_date);

  <<daily_loop>>
   WHILE curr_day <= end_date
   LOOP
      BEGIN
         INSERT INTO HFE_STATE_FAB
            SELECT ROUND (KF.FLOWN_KM)
,                  ROUND (KF.DIRECT_KM)
,                  ROUND (KF.ACHIEVED_KM)
,                  ROUND ( (KF.FLOWN_KM / KF.ACHIEVED_KM - 1) * 100, 2)
                      FAB_HFE_CPF
,                  fab_rep
,                  KF.ENTRY_DATE
,                  state_rep
,                  ROUND ( (Ks.FLOWN_KM / Ks.ACHIEVED_KM - 1) * 100, 2)
                      STATE_HFE_CPF
,                  ROUND (Ks.FLOWN_KM)
,                  ROUND (Ks.DIRECT_KM)
,                  ROUND (Ks.ACHIEVED_KM)
              FROM HFE_FAB_STATE_MES_REP 
                   JOIN HFE_DAILY kf ON (fab_mes = kf.mes_area)
                   JOIN
                   HFE_DAILY ks
                      ON (    state_mes = ks.mes_area
                          AND kf.model_type = ks.model_type
                          AND kf.entry_date = ks.entry_date
                          AND kf.ref_area = ks.ref_area)
             WHERE kf.entry_date = curr_day AND kf.model_type = 'CPF';

         hfe.log_note (
            'Processed: ' || curr_day || ' (' || SQL%ROWCOUNT || ')');
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            hfe.log_note ('Skipped duplicate: ' || curr_day);
      END;

      curr_day := curr_day + 1;
      COMMIT;
   END LOOP daily_loop;

   hfe.log_note ('Completed (' || start_date || '  ' || end_date || ')');
END;