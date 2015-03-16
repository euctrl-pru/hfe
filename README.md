# Horizontal Flight Efficiency
This is the repository for the code responsible to calculater the HFE performance indicators.

## Usage ##
In order to calculate HFE data for a given date interval, execute:

```shell
$ compute "01/02/2015" "02/02/2015"
```


## What is What ##

### `create`
Creates all the relevant tables and installs the relevant packages, function, procedures ...

### `drop` ###
Drops all the relevant tables and removes the relevant packages, function, procedures ...


### `hfe.pks and hfe.pkb` ###
The package (definition and body respectively) with all the intelligence (or some form of it ;-)

### `cfmu_dist_km.sql`
The function to calculate the great circle distance the (subtle) way NM systems do.

### `gg.dmp` ###
Dump of `GG_MES_AREA_TBL` and `GG_REF_AREA_TBL`.
To import it execute:

```bash
$ imp $DBUSR/$DBPWD@$DBNAME FILE=gg.dmp
```

### TODO:`InsertHFEdailyValues.sql`
Inserts the aggregated daily values

### TODO:`HFEInsertFabStateDaily.sql` and `HFEInsertFabKEADaily.sql`
Generate the table per FAB/State (base for what is sent out every month -- and should be published on Dashboard?)