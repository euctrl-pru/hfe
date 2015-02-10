/* Formatted on 06-02-2015 13:02:10 (QP5 v5.240.12305.39446) */
DECLARE
   start_date     DATE := '31-dec-2014';
   end_date       DATE := '31-jan-2015';
   kea_day        DATE;
   days_in_year   INTEGER;
BEGIN
   kea_day := start_date;
   hfe.log_note ('Start: ' || start_date || ' End: ' || end_date);

  <<daily_loop>>
   WHILE kea_day <= end_date
   LOOP
      BEGIN
         days_in_year := 365 + correctforleap (kea_day);

         INSERT INTO HFE_FAB_KEA
            WITH inp
                 AS (SELECT DISTINCT
                            fab
,                           kea_day
,                           entry_date
,                           fab_flown
,                           fab_achieved
,                           CASE
                               WHEN DENSE_RANK ()
                                    OVER (PARTITION BY fab, kea_day
                                          ORDER BY fab_flown / fab_achieved) <=
                                       10
                               THEN
                                  'EL'
                               WHEN DENSE_RANK ()
                                    OVER (PARTITION BY fab, kea_day
                                          ORDER BY fab_flown / fab_achieved) >
                                       days_in_year - 10
                               THEN
                                  'EH'
                               ELSE
                                  'I'
                            END
                               cat
                       FROM hfe_state_fab
                      WHERE     entry_date > kea_day - days_in_year
                            AND entry_date <= kea_day)
,                inter
                 AS (SELECT DISTINCT
                            fab
,                           kea_day
,                           cat
,                             SUM (fab_flown)
                                 OVER (PARTITION BY fab, kea_day)
                            / SUM (fab_achieved)
                                 OVER (PARTITION BY fab, kea_day)
                               hfe
,                           SUM (fab_flown)
                               OVER (PARTITION BY fab, kea_day, cat)
/                           SUM (fab_achieved)
                               OVER (PARTITION BY fab, kea_day, cat)
                               hfe_cat
,                           LISTAGG (entry_date, ', ')
                               WITHIN GROUP (ORDER BY entry_date)
                               OVER (PARTITION BY fab, kea_day, cat)
                               dates_cat
                       FROM inp)
            SELECT fab
,                  kea_day
,                  ROUND ( (i.hfe - 1) * 100, 2) hfe
,                  ROUND ( (i.hfe_cat - 1) * 100, 2) kea
,                  l.dates_cat excluded_low
,                  h.dates_cat excluded_high
              FROM inter i
                   JOIN inter l USING (fab, kea_day)
                   JOIN inter h USING (fab, kea_day)
             WHERE l.cat = 'EL' AND h.cat = 'EH' AND i.cat = 'I';

         hfe.log_note (
            'Processed: ' || kea_day || ' (' || SQL%ROWCOUNT || ')');
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            hfe.log_note ('Skipped duplicate: ' || kea_day);
      END;

      kea_day := kea_day + 1;
      COMMIT;
   END LOOP daily_loop;

   hfe.log_note ('Completed (' || start_date || '  ' || end_date || ')');
END;