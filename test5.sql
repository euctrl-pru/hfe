-- take FROM and TO dates as input and compute HFE for the date interval
DECLARE
curr_date DATE := '&1';
end_date DATE := '&2';
BEGIN
WHILE curr_date <= end_date LOOP
   hfe.load_hfe_day(curr_date, 'CPF', 'GG_REF_AREA_TBL', 'GG_MES_AREA_TBL', true);
   curr_date := curr_date + 1;
END LOOP;
END;
