-- take FROM and TO dates as input and compute HFE for the date interval
-- TODO: remove dependency from PRUTEST, i.e. GG_REF_AREA_TBL and GG_MES_AREA_TBL
DECLARE
cur_date DATE := to_date('&1', 'dd-mon-yyyy');
end_date DATE := to_date('&2', 'dd-mon-yyyy');
BEGIN
WHILE cur_date <= end_date LOOP
   HFE.load_hfe_day(cur_date, 'CPF', 'GG_REF_AREA_TBL', 'GG_MES_AREA_TBL', true);
   cur_date := cur_date + 1;
END LOOP;
END;
/