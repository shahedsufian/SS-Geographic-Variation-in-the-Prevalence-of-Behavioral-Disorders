/*
DISCLAIMER: READY-TO-RUN SCRIPT REQUIREMENTS

This script is syntactically correct, but it is NOT ready to run "out-of-the-box." It was
developed using a proprietary All-Payer Claims Database (APCD) and other specific datasets.

To use this script, you MUST:
1. Have access to similar member enrollment, medical claims, and socio-demographic data.
2. Replace all placeholder paths and data source names (e.g., 'path/to/your/data', 'YOUR-DSN')
   with the actual locations of YOUR datasets.

The code serves as a template for conducting this type of analysis.
*/

/*
TITLE: Geographic Variation in the Prevalence of Behavioral Disorders

DESCRIPTION: This script processes member enrollment and medical claims data to identify children
aged 4-17 with ADHD, ODD, or CD in 2019. It merges this data with ZIP code-level
socio-demographic data to analyze the geographic distribution of these disorders.
*/


/* PART 0: LIBNAME STATEMENTS */
/* Description: Define libraries for database connections and local SAS datasets. */

/* Connect to your database via ODBC. Replace 'YOUR-DSN' with your data source name. */
libname _odbclib odbc noprompt="dsn=YOUR-DSN;Trusted_connection=yes;UseDeclareFetch=32767,schema=public";

/* Define a library to store the final enrollment dataset. */
libname enroll 'path/to/your/project/enrollment_data';

/* Define a library to store the final claims dataset. */
libname claims 'path/to/your/project/claims_data';

/* Define a library to store merged and final analysis datasets. */
libname merged 'path/to/your/project/merged_data';


/* PART 1: MEMBER ENROLLMENT DATA PREPARATION */
/* Description: Filter the member enrollment file to create the study's denominator population. */

/* Count the total number of unique members in the raw member file as a baseline. */
/* Expected count: 4,277,304 entries. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from _odbclib.member);
quit;

/* Step 1.1: Restrict sample to children aged 4-17 in the study year (2019). */
proc sql;
	create table enrl_ageres (compress=yes) as
	select*
	from _odbclib.member
	where me014_year between 2002 and 2015; /* ME014_YEAR = Member Year of Birth */
quit;

/* Sort the age-restricted data. */
data enrl_ageres;
	set enrl_ageres;
	proc sort;
		by me998 me013 me162a; /* me998/me013 = unique ID, me162a = Enrollment Date */
run;

/* Count unique children after age restriction. */
/* Expected count: 726,344 children. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enrl_ageres);
quit;

/* Step 1.2: Exclude children without medical coverage or with non-relevant plans. */
data enrl_ageres;
	set enrl_ageres;
	if (me018 ne 1 or me003="AW" or me003="DNT") then delete;
run;

/* Count unique children after coverage restrictions. */
/* Expected count: 631,762 children. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enrl_ageres);
quit;

/* Step 1.3: Create categorical variables for analysis. */
proc sql;
	create table enrl_catarg as
	select *,
		/* Create 'age' based on the study year 2019. */
		(2019 - me014_year) as age,

		/* Create dummy variables for gender. */
		case when me013 in ('M') then 1 else 0 end as male,
		case when me013 in ('F') then 1 else 0 end as female,

		/* Create dummy variables for insurance type. */
		case when me001_cat in ('PRIV') then 1 else 0 end as private,
		case when me001_cat in ('MCR') then 1 else 0 end as medicare,
		case when me001_cat in ('MCD') then 1 else 0 end as medicaid,

		/* Create initial race/ethnicity flags. */
		case when me025 in ('13' '14' '15' '16' '17' '18' '19' '20' '21' '22' '34') then 1 else 0 end as hispanic_race,
		case when me021 in ('2106-3') then 1 else 0 end as white_race,
		case when me021 in ('2054-5') then 1 else 0 end as black_race,
		case when me021 in ('1002-5' '2028-9' '2076-8' '2131-1') then 1 else 0 end as other_race,
		case when me021 in ('9999-9') then 1 else 0 end as unknown_race,

		/* Flag members enrolled for at least one day in 2019. */
		case when me162a <= '31Dec19'd and me163a >= '01Jan19'd then 1 else 0 end as en19
	from enrl_ageres;
quit;

/* Create a single unique member ID. */
data enrl_catarg;
	set enrl_catarg;
	me998_013 = CATS(me998, me013);
run;

/* Step 1.4: Create a unique patient-level file for the 2019 cohort. */
data enrl_catarg;
	set enrl_catarg;
	if en19=1 then output; /* Keep only those enrolled in 2019 */
	proc sort;
		by me998_013 me162a; /* Sort by ID and enrollment date */
run;

data enrl_catarg;
	set enrl_catarg;
	by me998_013;
	if first.me998_013; /* Keep only the first record for each member */
	year=2019; /* Set the study year */
run;

/* Count the unique children in the final 2019 cohort. */
/* Expected count: 457,225 children. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enrl_catarg);
quit;

/* Step 1.5: Create final, mutually exclusive race categories. */
data enrl_racefixed;
	set enrl_catarg;
	race_white_new = 0;
	if white_race = 1 and hispanic_race = 0 then race_white_new = 1; /* Non-Hispanic White */
	race_black_new = 0;
	if black_race = 1 and hispanic_race = 0 then race_black_new = 1; /* Non-Hispanic Black */
	race_hispanic_new = hispanic_race;
	race_other_new = 0;
	if other_race = 1 and hispanic_race = 0 then race_other_new = 1; /* Non-Hispanic Other */
	race_missing_new = 0;
	if race_white_new = 0 and race_black_new = 0 and race_hispanic_new = 0 and race_other_new = 0 then race_missing_new = 1; /* Unknown/Missing */
	drop white_race black_race hispanic_race other_race unknown_race; /* Drop old race variables */
run;

/* Step 1.6: Remove members with a missing ZIP code and save the final enrollment file. */
data enroll.enrl_racefixed;
	set enrl_racefixed;
	if me017='' then delete; /* ME017 = Member ZIP Code */
run;

/* Final count of children in the denominator population. */
/* Expected count: 448,623 children. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.enrl_racefixed);
quit;


/* PART 2: MEDICAL CLAIMS DATA PREPARATION */
/* Description: Process medical claims to identify diagnoses of interest (ADHD, ODD, CD). */

/* Step 2.1: Apply age restrictions to claims datasets for years 2019-2021. */
proc sql;
	create table clm19_ageres (compress=yes) as
	select mc013_year, mc004, mc005, mc059, mc060, mc036, mc037, mc055, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2019
	where mc013_year between 2002 and 2015;
quit;

proc sql;
	create table clm20_ageres (compress=yes) as
	select mc013_year, mc004, mc005, mc059, mc060, mc036, mc037, mc055, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2020
	where mc013_year between 2003 and 2016;
quit;

proc sql;
	create table clm21_ageres (compress=yes) as
	select mc013_year, mc004, mc005, mc059, mc060, mc036, mc037, mc055, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2021
	where mc013_year between 2004 and 2017;
quit;

/* Step 2.2: Identify diagnoses in each year's claims using ICD-9 and ICD-10 codes. */
data clm19_dx;
	set clm19_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	Do j= 1 to 13;
		If mcn(j) in: ('F900','F901','F902','F908','F909') and MC915A=0 then adhd=1; /* ICD-10 ADHD */
		If mcn(j) in: ('31400','31401') and MC915A=9 then adhd=1; /* ICD-9 ADHD */
		If mcn(j) in: ('F913') and MC915A=0 then odd=1; /* ICD-10 ODD */
		If mcn(j) in: ('31381') and MC915A=9 then odd=1; /* ICD-9 ODD */
		If mcn(j) in: ('F911','F912','F919') and MC915A=0 then cd=1; /* ICD-10 CD */
		If mcn(j) in: ('31281','31282','31289') and MC915A=9 then cd=1; /* ICD-9 CD */
	end;
	drop j;
run;

data clm20_dx;
	set clm20_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	Do j= 1 to 13;
		If mcn(j) in: ('F900','F901','F902','F908','F909') and MC915A=0 then adhd=1;
		If mcn(j) in: ('31400','31401') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F911','F912','F919') and MC915A=0 then cd=1;
		If mcn(j) in: ('31281','31282','31289') and MC915A=9 then cd=1;
	end;
	drop j;
run;

data clm21_dx;
	set clm21_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	Do j= 1 to 13;
		If mcn(j) in: ('F900','F901','F902','F908','F09') and MC915A=0 then adhd=1;
		If mcn(j) in: ('31400','31401') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F911','F912','F919') and MC915A=0 then cd=1;
		If mcn(j) in: ('31281','31282','31289') and MC915A=9 then cd=1;
	end;
	drop j;
run;

/* Step 2.3: Combine processed claims and keep only records from the 2019 study year. */
data claims.clm19;
	set clm19_dx clm20_dx clm21_dx;
	if svcyear=2019 and (adhd=1 or odd=1 or cd=1) then output;
	proc sort;
		by svcyear;
run;


/* PART 3: MERGING DATA AND CREATING FINAL ANALYTIC FILE */
/* Description: Merge enrollment and claims, apply the final case definition (>=2 claims), and create the patient-level file. */

/* Step 3.1: Left join claims to enrollment data to keep all children in the denominator. */
proc sql;
	create table mrgd19inr as
	select *
	from enroll.enrl_racefixed a
	left join
	claims.clm19 b
	on a.me001 = b.mc001 and a.me107 = b.mc137;
quit;

/* Replace missing diagnosis flags with 0. */
proc stdize data=mrgd19inr out=merged.mrgd19inr reponly missing=0;
	var adhd odd cd;
run;

/* Step 3.2: Collapse data by patient and service date to get unique claim days. */
proc sql;
	create table datecollapsed_19 as
    select
        me998_013, mc059, max(me998) as me998, max(me013) as me013, max(me001) as me001,
        max(me107) as me107, max(mc004) as mc004, max(mc037) as mc037, max(adhd) as adhd,
        max(odd) as odd, max(cd) as cd, max(age) as age, max(male) as male,
        max(female) as female, max(private) as private, max(medicare) as medicare,
        max(medicaid) as medicaid, max(race_white_new) as race_white_new,
        max(race_black_new) as race_black_new, max(race_hispanic_new) as race_hispanic_new,
        max(race_other_new) as race_other_new, max(race_missing_new) as race_missing_new,
        max(year) as year, max(en19) as en19, max(me016) as me016, max(me173a) as me173a,
        max(me017) as me017
    from merged.mrgd19inr
    group by me998_013, mc059;
quit;

/* Step 3.3: Apply the case definition: >=2 service dates with a diagnosis. */
proc sql;
	create table diagnosis_count as
    select me998_013, sum(adhd) as adhd_count, sum(odd) as odd_count, sum(cd) as cd_count
    from datecollapsed_19
    group by me998_013;
quit;

data diagnosis_final;
    set diagnosis_count;
    if adhd_count >= 2 then adhd_final = 1; else adhd_final = 0;
    if odd_count >= 2 then odd_final = 1; else odd_final = 0;
    if cd_count >= 2 then cd_final = 1; else cd_final = 0;
run;

/* Merge final diagnosis flags back to the main dataset. */
proc sql;
	create table bdfinal as
    select a.*, b.adhd_final, b.odd_final, b.cd_final
    from datecollapsed_19 a
    left join diagnosis_final b on a.me998_013 = b.me998_013;
quit;

/* Create a flag for 'any behavioral disorder'. */
data bdfinal;
    set bdfinal;
	if (adhd_final=1 or odd_final=1 or cd_final=1) then anybd=1;
	else anybd=0;
run;

/* Step 3.4: Create the final unique patient-level analytic file. */
proc sql;
	create table merged.finalclm as
    select
        me998_013, max(me998) as me998, max(me013) as me013, max(me001) as me001,
        max(me107) as me107, max(adhd_final) as adhd_final, max(odd_final) as odd_final,
        max(cd_final) as cd_final, max(anybd) as anybd, max(age) as age, max(male) as male,
        max(female) as female, max(private) as private, max(medicare) as medicare,
        max(medicaid) as medicaid, max(race_white_new) as race_white_new,
        max(race_black_new) as race_black_new, max(race_hispanic_new) as race_hispanic_new,
        max(race_other_new) as race_other_new, max(race_missing_new) as race_missing_new,
        max(year) as year, max(en19) as en19, max(me016) as me016, max(me173a) as me173a,
        max(me017) as me017
    from bdfinal
    group by me998_013;
quit;


/* PART 4: CHILD OPPORTUNITY INDEX (COI) DATA PREPARATION */
/* Description: Import and process the 2019 COI data file for Arkansas. */

libname coi 'path/to/your/project/COI_data';

PROC IMPORT OUT=ar_coi
     DATAFILE='path/to/your/project/COI_data/2019.csv'
     DBMS=csv REPLACE;
     GETNAMES=YES; DATAROW=2; guessingrows=max;
RUN;

/* Filter for Arkansas and clean up variables. */
DATA ar_coi;
   	SET ar_coi;
   	WHERE statefips=5;
	DROP c5_ED_nat c5_HE_nat c5_SE_nat c5_COI_nat c5_ED_stt c5_HE_stt c5_SE_stt c5_COI_stt c5_ED_met
	c5_HE_met c5_SE_met c5_COI_met r_ED_met r_HE_met r_SE_met r_COI_met;
RUN;

DATA coi.ar_coi;
	SET ar_coi;
	arzip = PUT(zip , z5.);
	otherrace=aian+api+other2;
	drop zip aian api other2;
RUN;

/* Determine the majority race for each ZIP code for later imputation. */
data coi.ar_coi;
    set coi.ar_coi;
	array races[4] hisp white black otherrace;
	major_race = .;
	do i = 1 to 4;
		if races[i] = max(of races[*]) then do;
			major_race = i;
			leave;
		end;
	end;
	drop i;
run;


/* PART 5: CALCULATING PREVALENCE AND CREATING QUINTILES */
/* Description: Calculate ZIP-level prevalence, rank ZIPs into quintiles, and merge with COI data. */

/* Apply final cleaning and create analysis variables. */
data merged.finalclm;
	set merged.finalclm;
	if (me016 ne "05" or me173a="000") then delete;
	if 4<=age<=11 then agecat=0; else if 12<=age<=17 then agecat=1;
	if male=0 then sex=0; else if male=1 then sex=1;
	if (medicare=1 or medicaid=1) then inscov=0; else if (medicare=0 and medicaid=0) then inscov=1;
	if race_hispanic_new = 1 then race = 1;
	else if race_white_new = 1 then race = 2;
	else if race_black_new = 1 then race = 3;
	else if race_other_new = 1 then race = 4;
	else race = 0;
run;

/* Step 5.1: Calculate disorder counts by ZIP code. */
proc means data=merged.finalclm noprint;
	class me017;
	var adhd_final odd_final cd_final anybd;
	output out=merged.zip_n sum(adhd_final)=adhd_count sum(odd_final)=odd_count sum(cd_final)=cd_count sum(anybd)=anybd_count;
run;

/* Step 5.2: Calculate disorder prevalence by ZIP code. */
proc means data=merged.finalclm nway missing;
	class me017;
	var adhd_final odd_final cd_final anybd;
	output out=merged.zip_pvln mean(adhd_final)=adhd_pvln mean(odd_final)=odd_pvln mean(cd_final)=cd_pvln mean(anybd)=anybd_pvln;
run;

/* Step 5.3: Merge count and prevalence datasets and clean. */
data merged.zip;
	merge merged.zip_n merged.zip_pvln;
	by me017;
	if _N_ = 1 then delete;
	if me017="" then delete;
	drop _TYPE_;
	rename _FREQ_=FREQ me017=arzip;
run;

/* Step 5.4: Rank ZIP codes into quintiles based on prevalence. */
proc rank data=merged.zip (where=(anybd_count > 10)) out=merged.zip_qntl groups=5 ties=mean;
	var anybd_pvln;
	ranks anybd_qntl;
run;

/* Export quintile data to Excel. */
data anybdqntl;
    set merged.zip_qntl(keep=arzip freq anybd_count anybd_pvln anybd_qntl);
run;
proc export data=anybdqntl
    outfile="path/to/your/project/merged_data/anybdqntl.xlsx"
    dbms=xlsx replace;
run;

/* Step 5.5: Join prevalence-ranked data with COI data. */
proc sql;
	create table zip_qntl_coi as
	select *
	from anybdqntl a
	inner join coi.ar_coi b on a.arzip = b.arzip;
quit;

/* Final cleaning of merged ZIP-level file. */
DATA merged.zip_qntl_coi;
    SET zip_qntl_coi;
    if msaid15 = . then msaid15 = 00000;
	if msaname15 = '' then msaname15 = 'Not a Metro/Micro Area';
	anybd_pvln_p = anybd_pvln*100;
RUN;


/* PART 6: GENERATING DESCRIPTIVE STATISTICS */
/* Description: Prepare final data for analysis and generate descriptive statistics across quintiles. */

/* Merge quintile/COI info back to the patient-level file. */
data merged.finalclm; set merged.finalclm; rename me017=arzip; run;
proc sql;
	create table mrgd_zip_clm as
	select *
	from merged.finalclm a
	inner join merged.zip_qntl_coi b on a.arzip = b.arzip;
quit;

/* Impute missing patient race using ZIP-code majority race. */
data mrgd_zip_clm; set mrgd_zip_clm; if race=0 then race=major_race; run;

/* Create final character variables and clean up dataset for analysis. */
data merged.mrgd_zip_clm;
	set mrgd_zip_clm;
	race_cat=put(race, z1.); age_cat=put(agecat, z1.); sex_cat=put(sex, z1.);
	inscov_cat=put(inscov, z1.); anybdqntl=put(anybd_qntl, z1.); anybd_cat=put(anybd, z1.);
	drop male female race_black_new race_hispanic_new race_other_new race_missing_new freq
	     statefips stateusps pop white black hisp otherrace anybd_pvln;
	rename me016=state me173a=county msaid15=metrofips msaname15=metroname anybd_pvln_p=anybd_pvln;
run;

/* Step 6.1: Run descriptive statistics. */
PROC SORT data=merged.mrgd_zip_clm; BY anybdqntl; RUN;

/* Calculate mean and SD for continuous variables by quintile. */
PROC MEANS data=merged.mrgd_zip_clm;
	VAR anybd_pvln r_coi_nat r_ed_nat r_he_nat r_se_nat age;
	BY anybdqntl;
RUN;

/* Test for mean differences between quintiles. */
PROC GLM data=merged.mrgd_zip_clm;
	CLASS anybdqntl;
	MODEL anybd_pvln r_coi_nat r_ed_nat r_he_nat r_se_nat age = anybdqntl;
	LSMEANS anybdqntl / DIFF=CONTROL("4");
RUN;

/* Calculate frequency distributions for categorical variables by quintile. */
proc surveyfreq data=merged.mrgd_zip_clm;
	tables anybdqntl*(age_cat sex_cat inscov_cat race_cat) / cl col chisq;
run;

/* Generate boxplot of prevalence by quintile. */
proc sgplot data=merged.mrgd_zip_clm;
	vbox anybd_pvln / category=anybdqntl;
    xaxis label='Quintile';
    yaxis label='Prevalence of BD, %';
run;


/* PART 7: SDOH DATA PREPARATION & QUALITY CHECKS */
/* Description: Import SDOH data and perform quality checks comparing ZIP codes to ZCTAs. */

libname sdoh 'path/to/your/project/SDOH_data';

/* Import the 2019 SDOH Excel file. */
PROC IMPORT OUT=ar_sdoh
	DATAFILE="path/to/your/project/SDOH_data/SDOH_2019_ZIPCODE_1_0.xlsx"
	DBMS=xlsx REPLACE;
	SHEET="data"; GETNAMES=YES;
RUN;

/* Filter for Arkansas and check for ZIP/ZCTA mismatches. */
DATA sdoh.ar_sdoh; SET ar_sdoh; IF STATEFIPS = '05'; RUN;
DATA mismatched_zipzcta; SET sdoh.ar_sdoh; IF ZIPCODE ~= ZCTA; RUN;

/* Join SDOH data to the main analytic file. */
proc sql;
	create table mrgd_clm_sdoh as
	select *
	from merged.zip_qntl_coi a
	left join sdoh.ar_sdoh b on a.arzip = b.ZIPCODE;
quit;