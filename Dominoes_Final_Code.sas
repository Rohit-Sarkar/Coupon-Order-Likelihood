libname Q 'G:\Dominos';
/*Importing the file and converting into the sas file.*/
proc import datafile='G:\Dominos\transactions-7_1_16-9_30_16.txt' out=Q.transaction3
dbms=dlm replace;
delimiter='|';
run;
proc import datafile='G:\Dominos\transactions-4_1_16-6_30_16.txt' out=Q.transaction2
dbms=dlm replace;
delimiter='|';
run;
proc import datafile='G:\Dominos\transactions-1_1_16-3_31_16.txt' out=Q.transaction1
dbms=dlm replace;
delimiter='|';
run;
proc import datafile='G:\Dominos\Second Stage\us_postal_codes.csv' out=Q.zipcode
dbms=csv replace;
run;
/* Merging the transaction datasets with the zipcode to get the USregions.*/
data Q.transaction0;
merge Q.transaction0(in=A) Q.zipcode;
by zip5;
if A;
drop CRRT OrderType ReturnCode RecordType OrderNumber LocType PaymentCode
		OrderMethodDesc OrderTypeCode per_dis add_order_amt dis_amt County 
		Place_Name State_Abbreviation Latitude Longitude;
if AddressId = "" then delete;
run; 
data Q.transaction1;
merge Q.transaction1(in=A) Q.zipcode;
by zip5;
if A;
drop CRRT OrderType ReturnCode RecordType OrderNumber LocType PaymentCode
		OrderMethodDesc OrderTypeCode per_dis add_order_amt dis_amt County 
		Place_Name State_Abbreviation Latitude Longitude;
if AddressId = "" then delete;
run; 
data Q.transaction2;
merge Q.transaction2(in=A) Q.zipcode;
by zip5;
if A;
drop CRRT OrderType ReturnCode RecordType OrderNumber LocType PaymentCode
		OrderMethodDesc OrderTypeCode per_dis add_order_amt dis_amt County 
		Place_Name State_Abbreviation Latitude Longitude;
if AddressId = "" then delete;
run; 
data Q.transaction3;
merge Q.transaction3(in=A) Q.zipcode;
by zip5;
if A;
drop CRRT OrderType ReturnCode RecordType OrderNumber LocType PaymentCode
		OrderMethodDesc OrderTypeCode per_dis add_order_amt dis_amt County 
		Place_Name State_Abbreviation Latitude Longitude;
if AddressId = "" then delete;
run; 
/*Deleting the missing values in Regions*/
data Q.transaction0;
set Q.transaction0;
if Region = "" then delete;
run;
data Q.transaction1;
set Q.transaction1;
if Region = "" then delete;
run;
data Q.transaction2;
set Q.transaction2;
if Region = "" then delete;
run;
data Q.transaction3;
set Q.transaction3;
if Region = "" then delete;
run;
/*Stratified Sampling at 0.5 probability. on region.*/

proc surveyselect data=Q.transaction0 method=sys rate=.5 seed=9876 out=Q.try_transac0;
strata Region;
run; 
proc freq data=Q.sample_transac0;
tables Region;
run;
proc sort data=Q.transaction1;
by Region;
run;
proc surveyselect data=Q.transaction1 method=sys rate=.5 seed=1230 out=Q.try_transac1;
strata Region;
run; 
proc freq data=Q.sample_transac1;
tables Region;
run;
proc sort data=Q.transaction2;
by Region;
run;
proc surveyselect data=Q.transaction2 method=sys rate=.5 seed=4563 out=Q.try_transac2;
strata Region;
run; 
proc freq data=Q.sample_transac2;
tables Region;
run;
proc sort data=Q.transaction3;
by Region;
run;
proc surveyselect data=Q.transaction3 method=sys rate=.5 seed=7896 out=Q.try_transac3;
strata Region;
run; 
proc freq data=Q.sample_transac3;
tables Region;
run;
PROC SORT DATA=Q.try_TRANSAC0;
BY DESCENDING ADDRESSID;
RUN;
/*Dropping unnecessary variables and creating another dataset with only coupons.*/
data Q.try_transac0;
set Q.try_transac0;
drop allbreadcount artisancbccount ArtisanItSausPepTrioCount ArtisanSpinFetaCount ArtisanTusSalRoastVegCount boneincount
		bonelesscount breadord drinkord outtime;
run; 
data Q.try_coupons0;
set Q.try_transac0;
keep AddressID CouponCode CouponDesc;
run;
data Q.try_transac1;
set Q.try_transac1;
drop allbreadcount artisancbccount ArtisanItSausPepTrioCount ArtisanSpinFetaCount ArtisanTusSalRoastVegCount boneincount
		bonelesscount breadord drinkord outtime;
run; 
data Q.try_coupons1;
set Q.try_transac1;
keep AddressID CouponCode CouponDesc;
run;
data Q.try_transac2;
set Q.try_transac2;
drop allbreadcount artisancbccount ArtisanItSausPepTrioCount ArtisanSpinFetaCount ArtisanTusSalRoastVegCount boneincount
		bonelesscount breadord drinkord outtime;
run; 
data Q.try_coupons2;
set Q.try_transac2;
keep AddressID CouponCode CouponDesc;
run;
data Q.try_transac3;
set Q.try_transac3;
drop allbreadcount artisancbccount ArtisanItSausPepTrioCount ArtisanSpinFetaCount ArtisanTusSalRoastVegCount boneincount
		bonelesscount breadord drinkord outtime;
run; 
data Q.try_coupons3;
set Q.try_transac3;
keep AddressID CouponCode CouponDesc;
run;

data Q.try_transac0(drop=dateordered ordertime); 
set Q.try_transac0;
format DateOfOrder Date9.;
DateOfOrder=DatePart(DateOrdered);
timeoforder=hour(ordertime);run;
data Q.try_transac1(drop=dateordered ordertime); 
set Q.try_transac1;
format DateOfOrder Date9.;
DateOfOrder=DatePart(DateOrdered);
timeoforder=hour(ordertime);run;
data Q.try_transac2(drop=dateordered ordertime); 
set Q.try_transac2;
format DateOfOrder Date9.;
DateOfOrder=DatePart(DateOrdered);
timeoforder=hour(ordertime);run;
data Q.try_transac3(drop=dateordered ordertime); 
set Q.try_transac3;
format DateOfOrder Date9.;
DateOfOrder=DatePart(DateOrdered);
timeoforder=hour(ordertime);run;
/*Calculating the recency*/
/*Creating a dataset with addressid and date*/
data transac_rec0;
set Q.try_transac0;
keep addressid dateoforder;
run;
proc sort data=transac_rec0;
by addressid dateoforder;
run;
data a(drop=dateoforder);
set transac_rec0;
by addressid dateoforder;
retain count;
format orderdate ddmmyy9.;
if first.addressid then do;
	count=0;
	end;
count = count + 1;
if last.addressid then do;
	count = count; 
	orderdate= dateoforder;
	output;
end;run;
data transac_rec1;
set Q.try_transac1;
keep addressid dateoforder;
run;
proc sort data=transac_rec1;
by addressid dateoforder;
run;
data b(drop=dateoforder);
set transac_rec1;
by addressid dateoforder;
retain count;
format orderdate ddmmyy9.;
if first.addressid then do;
	count=0;
	end;
count = count + 1;
if last.addressid then do;
	count = count; 
	orderdate= dateoforder;
	output;
end;run;
data transac_rec2;
set Q.try_transac2;
keep addressid dateoforder;
run;
proc sort data=transac_rec2;
by addressid dateoforder;
run;
data c(drop=dateoforder);
set transac_rec2;
by addressid dateoforder;
retain count;
format orderdate ddmmyy9.;
if first.addressid then do;
	count=0;
	end;
count = count + 1;
if last.addressid then do;
	count = count; 
	orderdate= dateoforder;
	output;
end;run;
data transac_rec3;
set Q.try_transac3;
keep addressid dateoforder;
run;
proc sort data=transac_rec3;
by addressid dateoforder;
run;
data d(drop=dateoforder);
set transac_rec3;
by addressid dateoforder;
retain count;
format orderdate ddmmyy9.;
if first.addressid then do;
	count=0;
	end;
count = count + 1;
if last.addressid then do;
	count = count; 
	orderdate= dateoforder;
	output;
end;run;
data recency_freq;
set a b c d;
run;
proc sort data=recency_freq;
by addressid orderdate;
run;
/*Calculating the frequency*/
data rec_freq(drop=orderdate fq count);
set recency_freq;
by addressid orderdate;
retain fq;
format dateoforder ddmmyy9.;
if first.addressid then do;
	fq = 0;
	end;
fq = fq + count;
if last.addressid then do;
	freq = fq; 
	dateoforder=orderdate;
	output;
end;run;
data rec_freq1;
set rec_freq;
Recency = intck('day',DateOfOrder,'01oct2016'd);
run;
proc freq data=rec_freq1;
tables freq;
run;
proc univariate data=rec_freq1;
var freq recency;
run;
data Q.rec_freq;
set rec_freq1;
run;
proc contents data=Q.sampledata out=abc;
run;
proc freq data=Q.sampledata;
tables LgPieCount
MedPieCount
PanPieCount
PastaCount
PieCt
RectangulatorCount
SandwichCount
SmallPieCount
ThinPieCount
ToppingCount
TwistyOrd
WingOrd
XlgPieCount;
run;
/*Rolling up the data on addressid*/
proc sql;
create table Q.sample_addressid as
select addressid,
sum( orderamount) as add_order_amt,avg( piect) as avg_picnt,
avg( orderamount) as avg_order_amt,avg( discountamount) as avg_dis_amt,
avg( sideord) as avg_sideord,sum( case when couponcode="" then 0 else 1 end) as coupon_cnt,
sum( discountamount) as dis_amt,sum(custamount) as custamt,
avg(toppingcount) as topping_cnt,
count( case when mealperiod=3 then 1 else 0 end ) as late_dinner_meal, 
count( case when mealperiod=2 then 1 else 0 end ) as dinner_meal,
count( case when mealperiod=1 then 1 else 0 end ) as lunch_meal,
sum( case when wkday=1 then 1 else 0 end ) as wk_mon,
sum( case when wkday=2 then 1 else 0 end ) as wk_tues,
sum( case when wkday=3 then 1 else 0 end ) as wk_wed,
sum( case when wkday=4 then 1 else 0 end ) as wk_thur,
sum( case when wkday=5 then 1 else 0 end ) as wk_fri,
sum( case when wkday=6 then 1 else 0 end ) as wk_sat,
sum( case when wkday=7 then 1 else 0 end ) as wk_sun,
avg(chickenord) as avg_chicken_ord, avg(ChsBreadOrd) as avg_cheesebread_ord,
avg(CinnaStixOrd) as avg_cinnastix, avg(DessertCount) as avg_dessertcount,
avg(Drink12ozcount) as avg_12oz, avg(Drink20ozcount) as avg_20oz,
avg(Drink2ltrCount) as avg_2ltr, avg(HandPieCount) as avg_handpie,
avg(LgPieCount) as avg_lgpie, avg(MedPieCount) as avg_medpie,
avg(smallpiecount) as avg_smallpie, avg(PanPieCount) as avg_panpie,
avg(PastaCount) as avg_pasta, avg(SandwichCount) as avg_sandwichcount,
avg(ThinPieCount) as avg_thinpie, avg(twistyord) as avg_twistyord,
avg(WingOrd) as avg_wingord
from Q.sampledata
where addressid <> .
group by 1;
quit;
data Q.sample_id_region(keep= addressid region);
set Q.sampledata;
run;
proc sort data=Q.sampledata;
by addressid;
run;
/*Taking out the latest orderdate at rolled up data*/
data q(keep=USregion addressid zip);
set Q.sampledata;
by addressid;
if last.addressid then do;
USregion = region;
zip = zip5;
output;
end;
run;
data sampledata;
merge Q.sample_addressid(in=A) q(in=B);
by addressid;
if A and B;
run;
data Q.sample_add_region;
set sampledata;
run;
data Q.sample_add_region(drop= dateoforder);
merge sampledata(in=A) Q.rec_freq(in=B);
by addressid;
if A and B;
run;
proc sort data=Q.sample_add_region;
by recency;
run;
/*Ranking on the basis of recency score*/
proc rank data=Q.sample_add_region out=sxd groups=5  descending;
var recency;
ranks recencyscore;
run;
/*Ranking on the basis of frequency score*/
proc rank data=Q.sample_add_region out=rxs groups=5;
var freq;
ranks freqscore;
run;
/*Ranking on the basis of monetary amount*/
proc rank data=Q.sample_add_region out=Q.axp groups=5;
var add_order_amt;
ranks monetaryscore;
run;
proc copy in=work out=Q;
run;
proc sort data=Q.rxs;
by addressid;
run;
proc sort data=Q.axp;
by addressid;
run;
proc sort data=Q.sxd;
by addressid;
run;
data axk;
merge Q.rxs(in=A) Q.axp(in=B);
by addressid;
if A and B;
run;
data zxb;
merge Q.sxd(in=A) axk(in=B);
by addressid;
if A and B;
run;
proc contents data=zxb;
run;
/*Converting the score into character format*/
data abc;
set zxb;
freq_score = put(freqscore , 6.);
monetary_score = put(monetaryscore , 6.);
recency_score = put(recencyscore , 6.);
run;
/*Concatenating the recency score , frequency score and monetary score*/
data abc;
set abc;
RFM = cats(recency_score,freq_score,monetary_score);
run;
data Q.sample_rfm;
set abc;
drop freqscore monetaryscore recencyscore;
run;
proc sql;
create table sam as 
select addressid,
sum( case when mealperiod=3 then 1 else 0 end ) as late_dinner_meal, 
sum( case when mealperiod=2 then 1 else 0 end ) as dinner_meal,
sum( case when mealperiod=1 then 1 else 0 end ) as lunch_meal
from Q.sampledata
where addressid <> .
group by 1;
quit;
run;
proc sort data=sam;
by addressid;
run;
data q.rfm;
merge q.sample_rfm(in=A) sam(in=B);
by addressid;
if A and B;
run;
/*Converting the score to numeric format*/
data q.rfm;
set q.rfm;
rfm_score = rfm*1;
run;
/*Trying out different clusters*/
proc fastclus data=q.rfm maxclusters=8 out=q.rfm_cluster summary;
var rfm_score;
run;
proc freq data=q.rfm_cluster;
tables cluster;
run;
proc rank data=q.rfm out=abc groups=6 ;
var addressid;
ranks rank_address;
run;
/*data Q.a Q.b Q.c Q.d Q.e Q.f;*/
/*set abc;*/
/*if rank_address=0 then output Q.a;*/
/*if rank_address=1 then output Q.b;*/
/*if rank_address=2 then output Q.c;*/
/*if rank_address=3 then output Q.d;*/
/*if rank_address=4 then output Q.e;*/
/*if rank_address=5 then output Q.f;*/
/*run;*/
/*data abc;*/
/*set Q.try_coupons3;*/
/*where couponcode <> "";*/
/*run;*/
/*proc sort data=abc;*/
/*by couponcode;*/
/*run;*/
/*data Q.coupondesc3;*/
/*set abc;*/
/*by couponcode;*/
/*if last.couponcode then do; coupon_desc = coupondesc; coupon_code = couponcode; output;*/
/*end;*/
/*run;*/
/*proc export data=Q.coupondesc3 outfile='Desktop\coupondesc3.xlsx' dbms=xlsx replace;*/
/*run;*/
proc sort data=Q.rfm_cluster;
by cluster;
run;

proc sql;
create table q.cluster_avg as 
select cluster,
avg(recency) as avg_recency, avg(freq) as avg_freq, avg(add_order_amt) as avg_monetary
from Q.rfm_cluster group by 1;
quit;
run; 
ods rtf file='G:\Dominos\cluster_avg.rtf';
proc univariate data=Q.rfm_cluster;
var recency freq add_order_amt;
run;
ods rtf close;
proc fastclus data=q.rfm maxclusters=4 out=q.rfm_cluster_try summary;
var rfm_score;
run;
proc univariate data=Q.rfm_cluster_try;
var recency freq add_order_amt;
run;
proc sql;
create table q.cluster_avg_try as 
select cluster,
avg(recency) as avg_recency, avg(freq) as avg_freq, avg(add_order_amt) as avg_monetary
from Q.rfm_cluster_try group by 1;
quit;
run; 
proc freq data=Q.rfm_cluster;
tables cluster;
run;
proc contents data=Q.rfm_cluster;
run;
/*data Q.rfm_cluster_6;*/
/*set Q.rfm_cluster;*/
/*if cluster = 6 then cluster_name = 6 ;*/
/*if cluster = 7 then cluster_name = 6 ;*/
/*if cluster = 3 then cluster_name = 3 ;*/
/*if cluster = 8 then cluster_name = 3 ;*/
/*if cluster = 5 then cluster_name = 5;*/
/*if cluster = 2 then cluster_name = 2;*/
/*if cluster = 1 then cluster_name = 1;*/
/*if cluster = 4 then cluster_name = 4;*/
/*run;*/
/*proc freq data=Q.rfm_cluster_6;*/
/*tables cluster_name;*/
/*run;*/
proc sql;
create table q.cluster_avg_6 as 
select cluster_name,
avg(recency) as avg_recency, avg(freq) as avg_freq, avg(add_order_amt) as avg_monetary
from Q.rfm_cluster_6 group by 1;
quit;
run; 
/*data Q.rfm_cluster_4;*/
/*set Q.rfm_cluster_6;*/
/*if cluster_name = 6 then cluster_score = 1 ;*/
/*if cluster_name = 3 then cluster_score = 3;*/
/*if cluster_name = 5 then cluster_score = 4;*/
/*if cluster_name = 2 then cluster_score = 2;*/
/*if cluster_name = 1 then cluster_score = 1;*/
/*if cluster_name = 4 then cluster_score = 1;*/
/*run;*/
/*proc sql;*/
/*create table q.cluster_avg_4 as */
/*select cluster_score,*/
/*avg(recency) as avg_recency, avg(freq) as avg_freq, avg(add_order_amt) as avg_monetary*/
/*from Q.rfm_cluster_4 group by 1;*/
/*quit;*/
/*run; */
/*proc freq data=Q.rfm_cluster_4;*/
/*tables cluster_score;*/
/*run;*/

/*Importing demographics dataset*/
PROC IMPORT OUT= Q.Demographics0
            DATAFILE= "G:\Dominos\Demographics\AddressDemoInfotxt_1.txt" 
            DBMS=DLM REPLACE;
DELIMITER='|';
GETNAMES=YES;
     DATAROW=2; 
RUN;
proc sort data=Q.Demographics0;
by addressid;
run;
proc fastclus data=q.rfm maxclusters=8 out=q.rfm_cluster_5 noprint;
var rfm_score;
run;
proc contents data=q.rfm_cluster out=a;
run;
/*Decided on 6 clusters based on the standard deviation between the clusters*/
proc fastclus data=q.rfm maxclusters=6 out=q.rfm_clus_6 summary maxiter=5;
var rfm_score;
run;
proc sql;
create table q.cluster_avg_6 as 
select cluster,
avg(recency) as avg_recency, avg(freq) as avg_freq, avg(add_order_amt) as avg_monetary
from Q.rfm_clus_6 group by 1;
quit;
run; 
proc freq data=q.rfm_clus_6;
tables cluster;
run;
proc contents data=q.rfm_clus_6 out=a;
run;
/*Calculating the means of different variables based on the clusters*/
proc means data=q.rfm_clus_6;
class cluster;
var avg_12oz
avg_20oz
avg_2ltr
avg_cheesebread_ord
avg_chicken_ord
avg_cinnastix
avg_dessertcount
avg_handpie
avg_lgpie
avg_medpie
avg_panpie
avg_pasta
avg_picnt
avg_sandwichcount
avg_sideord
avg_smallpie
avg_thinpie
avg_twistyord
avg_wingord;
output out=q.avg;
run;
proc sort data=q.rfm_clus_6;
by cluster;
run;
proc means data=q.rfm_clus_6;
by cluster;
var wk_fri
wk_mon
wk_sat
wk_sun
wk_thur
wk_tues
wk_wed;
output out=q.b;
run;
proc means data=q.rfm_clus_6;
by cluster;
var dinner_meal
dis_amt
late_dinner_meal
lunch_meal
topping_cnt
coupon_cnt;
output out=q.c;
run;
proc sort data=q.sampledata;
by couponcode;
run;
proc freq data=q.try_coupons0 ;
tables couponcode;
output 
run;
/*Importing the mailed datasets*/
libname R 'G:\Dominos\merged';
data Q.mail1;
set R.merged1;
keep addressid mailed datemailed;
run;
data Q.mail2;
set R.merged2;
keep addressid mailed datemailed;
run;
data Q.mail3;
set R.merged3;
keep addressid mailed datemailed;
run;
data Q.mail4;
set R.merged4d;
keep addressid mailed datemailed;
run;
/*proc print data=Q.Rank0;*/
/*where addressid = 226 ;*/
/*run;*/
/*proc freq data=Q.rank0;*/
/*tables age4_7;*/
/*run;*/
/*proc contents data=Q.rank0 out=z;*/
/*run;*/

/*Converting the demographic variables into dummy variales.*/
data Q.rank0;
set Q.rank0;
if books_music_interest = "Y" then Books_music_int = 1; else Books_music_int = 0; 
if childrens_products_interest = "Y" then child_prod_int = 1; else child_prod_int = 0;
if gourmet_food_wine_interest = "Y" then food_wine_int = 1; else food_wine_int = 0;
if health_fitness_interest = "Y" then fitness_int = 1; else fitness_int = 0;
if high_ticket_mail_order_buyer = "Y" then high_ticket = 1; else high_ticket = 0;
if outdoor_enthusiast = "Y" then outdoor_enthu = 1; else outdoor_enthu = 0;
if pet_owner = "Y" then pet_owner = 1; else pet_owner = 0;
if photography_enthusiast = "Y" then photo_enthu = 1; else photo_enthu = 0;
if travel_entertainment_interest = "Y" then travel_entertain_int = 1; else travel_entertain_int = 0;
if fishing = "Y" then fish = 1; else fish = 0;
if boatingsailing = "Y" then boat_sailing = 1 ; else boat_sailing = 0;
if foreigntravel = "Y" then frn_travel = 1; else frn_travel = 0;
if gambling = "Y" then gamble = 1 ; else gamble = 0 ;
if dietingweightloss = "Y" then diet_wt_loss = 1; else diet_wt_loss = 0;
if cooking = "Y" then cook = 1 ; else cook = 0;
if camping = "Y" then camp = 1 ; else camp = 0;
drop boatingsailing
books_music_interest
camping
childrens_products_interest
cooking
creditcard_holder
dietingweightloss
femaleoccupation
fishing
foreigntravel
gambling
gourmet_food_wine_interest
health_fitness_interest
heavy_internet_user
high_ticket_mail_order_buyer
outdoor_enthusiast
photography_enthusiast
travel_entertainment_interest;
run;
/*Rolling up the demographic dataset on addressid*/
proc sql;
create table Q.demo0 as 
select addressid,
sum(numberofadults) as total_adults, sum(numberofchildren) as total_child,
sum(householdmembercount) as total_member_count, avg(phhchild) as per_under_18,
avg(phhw65p) as per_more_65, avg(pop05) as per_0_5 ,
avg(medag18p) as median_more_18, avg(phhsz3p) as per_3_more,
avg(phhmarrd) as per_married_couple, avg(phhspchld) as per_adult_1child_0spouse,
avg(phhd25km) as per_less_25k, avg(phhd150k) as per_more_200k,
avg(medhhd) as med_household_income, avg(ppoverty) as per_blw_poverty,
avg(p2auto) as per_with2vehicles, avg(pcolgrad) as per_graduates ,
avg(pwhitcol) as per_white, avg(pbluecol) as per_blue,
avg(punemply) as per_unemploy, avg(phhblack) as per_black, avg(phhasian) as per_asian, avg(phhspnsh) as per_spanish,
sum(books_music_int) as books_music, sum(child_prod_int) as child_prod_ind, sum(food_wine_int) as food_wine_ind,
sum(fitness_int) as fitness, sum(high_ticket) as high_tkt, sum(outdoor_enthu) as outdoor_activity,
sum(photo_enthu) as photography, sum(travel_entertain_int) as travel_entertain, sum(fish) as fishing,
sum(boat_sailing) as boating_sailing, sum(frn_travel) as travelling, sum(gamble) as gambling,
sum(diet_wt_loss) as dieting, sum(cook) as cooking, sum(camp) as camping
from Q.Rank0
group by 1;
quit;
data Q.rank1;
set Q.rank1;
if books_music_interest = "Y" then Books_music_int = 1; else Books_music_int = 0; 
if childrens_products_interest = "Y" then child_prod_int = 1; else child_prod_int = 0;
if gourmet_food_wine_interest = "Y" then food_wine_int = 1; else food_wine_int = 0;
if health_fitness_interest = "Y" then fitness_int = 1; else fitness_int = 0;
if high_ticket_mail_order_buyer = "Y" then high_ticket = 1; else high_ticket = 0;
if outdoor_enthusiast = "Y" then outdoor_enthu = 1; else outdoor_enthu = 0;
if pet_owner = "Y" then pet_owner = 1; else pet_owner = 0;
if photography_enthusiast = "Y" then photo_enthu = 1; else photo_enthu = 0;
if travel_entertainment_interest = "Y" then travel_entertain_int = 1; else travel_entertain_int = 0;
if fishing = "Y" then fish = 1; else fish = 0;
if boatingsailing = "Y" then boat_sailing = 1 ; else boat_sailing = 0;
if foreigntravel = "Y" then frn_travel = 1; else frn_travel = 0;
if gambling = "Y" then gamble = 1 ; else gamble = 0 ;
if dietingweightloss = "Y" then diet_wt_loss = 1; else diet_wt_loss = 0;
if cooking = "Y" then cook = 1 ; else cook = 0;
if camping = "Y" then camp = 1 ; else camp = 0;
drop boatingsailing
books_music_interest
camping
childrens_products_interest
cooking
creditcard_holder
dietingweightloss
femaleoccupation
fishing
foreigntravel
gambling
gourmet_food_wine_interest
health_fitness_interest
heavy_internet_user
high_ticket_mail_order_buyer
outdoor_enthusiast
photography_enthusiast
travel_entertainment_interest;
run;
proc sql;
create table Q.demo1 as 
select addressid,
sum(numberofadults) as total_adults, sum(numberofchildren) as total_child,
sum(householdmembercount) as total_member_count, avg(phhchild) as per_under_18,
avg(phhw65p) as per_more_65, avg(pop05) as per_0_5 ,
avg(medag18p) as median_more_18, avg(phhsz3p) as per_3_more,
avg(phhmarrd) as per_married_couple, avg(phhspchld) as per_adult_1child_0spouse,
avg(phhd25km) as per_less_25k, avg(phhd150k) as per_more_200k,
avg(medhhd) as med_household_income, avg(ppoverty) as per_blw_poverty,
avg(p2auto) as per_with2vehicles, avg(pcolgrad) as per_graduates ,
avg(pwhitcol) as per_white, avg(pbluecol) as per_blue,
avg(punemply) as per_unemploy, avg(phhblack) as per_black, avg(phhasian) as per_asian, avg(phhspnsh) as per_spanish,
sum(books_music_int) as books_music, sum(child_prod_int) as child_prod_ind, sum(food_wine_int) as food_wine_ind,
sum(fitness_int) as fitness, sum(high_ticket) as high_tkt, sum(outdoor_enthu) as outdoor_activity,
sum(photo_enthu) as photography, sum(travel_entertain_int) as travel_entertain, sum(fish) as fishing,
sum(boat_sailing) as boating_sailing, sum(frn_travel) as travelling, sum(gamble) as gambling,
sum(diet_wt_loss) as dieting, sum(cook) as cooking, sum(camp) as camping
from Q.Rank1
group by 1;
quit;
proc sql;
create table Q.demo0 as 
select addressid,
sum(numberofadults) as total_adults, sum(numberofchildren) as total_child,
sum(householdmembercount) as total_member_count, avg(phhchild) as per_under_18,
avg(phhw65p) as per_more_65, avg(pop05) as per_0_5 ,
avg(medag18p) as median_more_18, avg(phhsz3p) as per_3_more,
avg(phhmarrd) as per_married_couple, avg(phhspchld) as per_adult_1child_0spouse,
avg(phhd25km) as per_less_25k, avg(phhd150k) as per_more_200k,
avg(medhhd) as med_household_income, avg(ppoverty) as per_blw_poverty,
avg(p2auto) as per_with2vehicles, avg(pcolgrad) as per_graduates ,
avg(pwhitcol) as per_white, avg(pbluecol) as per_blue,
avg(punemply) as per_unemploy, avg(phhblack) as per_black, avg(phhasian) as per_asian, avg(phhspnsh) as per_spanish,
sum(books_music_int) as books_music, sum(child_prod_int) as child_prod_ind, sum(food_wine_int) as food_wine_ind,
sum(fitness_int) as fitness, sum(high_ticket) as high_tkt, sum(outdoor_enthu) as outdoor_activity,
sum(photo_enthu) as photography, sum(travel_entertain_int) as travel_entertain, sum(fish) as fishing,
sum(boat_sailing) as boating_sailing, sum(frn_travel) as travelling, sum(gamble) as gambling,
sum(diet_wt_loss) as dieting, sum(cook) as cooking, sum(camp) as camping
from Q.Rank0
group by 1;
quit;
data Q.rank2;
set Q.rank2;
if books_music_interest = "Y" then Books_music_int = 1; else Books_music_int = 0; 
if childrens_products_interest = "Y" then child_prod_int = 1; else child_prod_int = 0;
if gourmet_food_wine_interest = "Y" then food_wine_int = 1; else food_wine_int = 0;
if health_fitness_interest = "Y" then fitness_int = 1; else fitness_int = 0;
if high_ticket_mail_order_buyer = "Y" then high_ticket = 1; else high_ticket = 0;
if outdoor_enthusiast = "Y" then outdoor_enthu = 1; else outdoor_enthu = 0;
if pet_owner = "Y" then pet_owner = 1; else pet_owner = 0;
if photography_enthusiast = "Y" then photo_enthu = 1; else photo_enthu = 0;
if travel_entertainment_interest = "Y" then travel_entertain_int = 1; else travel_entertain_int = 0;
if fishing = "Y" then fish = 1; else fish = 0;
if boatingsailing = "Y" then boat_sailing = 1 ; else boat_sailing = 0;
if foreigntravel = "Y" then frn_travel = 1; else frn_travel = 0;
if gambling = "Y" then gamble = 1 ; else gamble = 0 ;
if dietingweightloss = "Y" then diet_wt_loss = 1; else diet_wt_loss = 0;
if cooking = "Y" then cook = 1 ; else cook = 0;
if camping = "Y" then camp = 1 ; else camp = 0;
drop boatingsailing
books_music_interest
camping
childrens_products_interest
cooking
creditcard_holder
dietingweightloss
femaleoccupation
fishing
foreigntravel
gambling
gourmet_food_wine_interest
health_fitness_interest
heavy_internet_user
high_ticket_mail_order_buyer
outdoor_enthusiast
photography_enthusiast
travel_entertainment_interest;
run;
proc sql;
create table Q.demo2 as 
select addressid,
sum(numberofadults) as total_adults, sum(numberofchildren) as total_child,
sum(householdmembercount) as total_member_count, avg(phhchild) as per_under_18,
avg(phhw65p) as per_more_65, avg(pop05) as per_0_5 ,
avg(medag18p) as median_more_18, avg(phhsz3p) as per_3_more,
avg(phhmarrd) as per_married_couple, avg(phhspchld) as per_adult_1child_0spouse,
avg(phhd25km) as per_less_25k, avg(phhd150k) as per_more_200k,
avg(medhhd) as med_household_income, avg(ppoverty) as per_blw_poverty,
avg(p2auto) as per_with2vehicles, avg(pcolgrad) as per_graduates ,
avg(pwhitcol) as per_white, avg(pbluecol) as per_blue,
avg(punemply) as per_unemploy, avg(phhblack) as per_black, avg(phhasian) as per_asian, avg(phhspnsh) as per_spanish,
sum(books_music_int) as books_music, sum(child_prod_int) as child_prod_ind, sum(food_wine_int) as food_wine_ind,
sum(fitness_int) as fitness, sum(high_ticket) as high_tkt, sum(outdoor_enthu) as outdoor_activity,
sum(photo_enthu) as photography, sum(travel_entertain_int) as travel_entertain, sum(fish) as fishing,
sum(boat_sailing) as boating_sailing, sum(frn_travel) as travelling, sum(gamble) as gambling,
sum(diet_wt_loss) as dieting, sum(cook) as cooking, sum(camp) as camping
from Q.Rank2
group by 1;
quit;
data Q.rank3;
set Q.rank3;
if books_music_interest = "Y" then Books_music_int = 1; else Books_music_int = 0; 
if childrens_products_interest = "Y" then child_prod_int = 1; else child_prod_int = 0;
if gourmet_food_wine_interest = "Y" then food_wine_int = 1; else food_wine_int = 0;
if health_fitness_interest = "Y" then fitness_int = 1; else fitness_int = 0;
if high_ticket_mail_order_buyer = "Y" then high_ticket = 1; else high_ticket = 0;
if outdoor_enthusiast = "Y" then outdoor_enthu = 1; else outdoor_enthu = 0;
if pet_owner = "Y" then pet_owner = 1; else pet_owner = 0;
if photography_enthusiast = "Y" then photo_enthu = 1; else photo_enthu = 0;
if travel_entertainment_interest = "Y" then travel_entertain_int = 1; else travel_entertain_int = 0;
if fishing = "Y" then fish = 1; else fish = 0;
if boatingsailing = "Y" then boat_sailing = 1 ; else boat_sailing = 0;
if foreigntravel = "Y" then frn_travel = 1; else frn_travel = 0;
if gambling = "Y" then gamble = 1 ; else gamble = 0 ;
if dietingweightloss = "Y" then diet_wt_loss = 1; else diet_wt_loss = 0;
if cooking = "Y" then cook = 1 ; else cook = 0;
if camping = "Y" then camp = 1 ; else camp = 0;
drop boatingsailing
books_music_interest
camping
childrens_products_interest
cooking
creditcard_holder
dietingweightloss
femaleoccupation
fishing
foreigntravel
gambling
gourmet_food_wine_interest
health_fitness_interest
heavy_internet_user
high_ticket_mail_order_buyer
outdoor_enthusiast
photography_enthusiast
travel_entertainment_interest;
run;
proc sql;
create table Q.demo3 as 
select addressid,
sum(numberofadults) as total_adults, sum(numberofchildren) as total_child,
sum(householdmembercount) as total_member_count, avg(phhchild) as per_under_18,
avg(phhw65p) as per_more_65, avg(pop05) as per_0_5 ,
avg(medag18p) as median_more_18, avg(phhsz3p) as per_3_more,
avg(phhmarrd) as per_married_couple, avg(phhspchld) as per_adult_1child_0spouse,
avg(phhd25km) as per_less_25k, avg(phhd150k) as per_more_200k,
avg(medhhd) as med_household_income, avg(ppoverty) as per_blw_poverty,
avg(p2auto) as per_with2vehicles, avg(pcolgrad) as per_graduates ,
avg(pwhitcol) as per_white, avg(pbluecol) as per_blue,
avg(punemply) as per_unemploy, avg(phhblack) as per_black, avg(phhasian) as per_asian, avg(phhspnsh) as per_spanish,
sum(books_music_int) as books_music, sum(child_prod_int) as child_prod_ind, sum(food_wine_int) as food_wine_ind,
sum(fitness_int) as fitness, sum(high_ticket) as high_tkt, sum(outdoor_enthu) as outdoor_activity,
sum(photo_enthu) as photography, sum(travel_entertain_int) as travel_entertain, sum(fish) as fishing,
sum(boat_sailing) as boating_sailing, sum(frn_travel) as travelling, sum(gamble) as gambling,
sum(diet_wt_loss) as dieting, sum(cook) as cooking, sum(camp) as camping
from Q.Rank3
group by 1;
quit;
/*Appending the different demographics file.*/
data demo;
set q.demo0 q.demo1 q.demo2 q.demo3;
run;
proc sort data=demo;
by addressid;
run;
proc sort data=q.rfm_clus_6;
by addressid;
run;
/*Merging the demographics and transactional datasets.*/
data Q.rfmclus_demo;
merge Q.rfm_clus_6(in=A) demo(in=B);
by addressid;
if A and B;
run;
/*data Q.rfmclus_demo1;*/
/*set Q.rfmclus_demo;*/
/*by addressid;*/
/*if first.addressid then do;*/
/*output;*/
/*end;*/
/*run;*/
proc freq data=q.rfmclus_demo;
tables cluster;
run;
proc contents data=q.rfmclus_demo out=abc;
run;
data q.rfmclus_demo1;
set q.rfmclus_demo;
keep AddressId
CLUSTER
USregion
boating_sailing
books_music
camping
child_prod_ind
cooking
dieting
fishing
fitness
food_wine_ind
gambling
high_tkt
med_household_income
median_more_18
outdoor_activity
per_0_5
per_3_more
per_adult_1child_0spouse
per_asian
per_black
per_blue
per_blw_poverty
per_graduates
per_less_25k
per_married_couple
per_more_65
per_more_200k
per_spanish
per_under_18
per_unemploy
per_white
per_with2vehicles
photography
total_adults
total_child
total_member_count
travel_entertain
travelling;
run;
proc contents data=q.rfmclus_demo out=xyz;
run;
/* Trying out the decision tree on the demographics.*/
/*But we are not able to find significant results hence we used enterprise miner 
for demographic profiling of clusters.*/
proc hpsplit data=q.rfmclus_demo_s;
target cluster;
input Books_music_int
USregion
boat_sailing
camp
child_prod_int
cook
diet_wt_loss
fish
fitness_int
food_wine_int
frn_travel
gamble
high_ticket
householdmembercount
medag18p
medhhd
numberofadults
numberofchildren
outdoor_enthu
p2auto
pbluecol
pcolgrad
pet_owner
phhasian
phhblack
phhchild
phhd150k
phhd25km
phhmarrd
phhspchld
phhspnsh
phhsz3p
phhw65p
photo_enthu
pop05
ppoverty
punemply
purchasingpowerincome
pwhitcol
travel_entertain_int;
criterion entropy;
prune misc / N <= 6;
score out=scored2;
run; 
proc freq data=q.rfmclus_demo_s;
tables cluster*USregion;
run;
proc means data=q.rfmclus_demo_s;
class cluster;
var Books_music_int
boat_sailing
camp
child_prod_int
cook
diet_wt_loss
fish
fitness_int
food_wine_int
frn_travel
gamble
high_ticket
householdmembercount
medag18p
medhhd
numberofadults
numberofchildren
outdoor_enthu
p2auto
pbluecol
pcolgrad
phhasian
phhblack
phhchild
phhd150k
phhd25km
phhmarrd
phhspchld
phhspnsh
phhsz3p
phhw65p
photo_enthu
pop05
ppoverty
punemply
pwhitcol
travel_entertain_int;
output out=try mean=m;
run;
data q.mail_merge;
set Q.mail1 q.mail2 q.mail3 q.mail4;
run;
proc sort data=q.mail_merge;
by addressid;
run;
proc sort data=q.mail_merge;
by datemailed;
run;
proc sort data=Q.sampledata;
by addressid;
run;
/*Making a different file for the coupons based on addressid and cluster*/
data q.coupons(keep=addressid coupondesc couponcode);
merge q.rfm(in=A) q.sampledata(in=B);
by addressid;
if A and B;
run;
proc sort data=coupons;
by couponcode;
run;
ods rtf file='G:\Dominos\coupons_sample.rtf';
proc freq data=coupons;
tables couponcode;
run;
ods rtf close;
proc freq data=coupons;
tables couponcode;
output out=q.coupons_freq;
run;
/*Coupons Categorization.*/
data q.coupon_newdec(keep=couponcode couponcodenew coupondesc addressid);
set q.coupons;
format couponcodenew $20.;
if find(coupondesc,'MIX 2 OR MORE','i') and find(coupondesc,'MEDIUM','i') then DO; couponcodenew = 'combo';end;
if find(coupondesc,'LG','i') or find(coupondesc,'large','i') then DO; couponcodenew = 'Large';end;
if find(coupondesc,'50% OFF','i') then DO; couponcodenew = '50off';end;
if find(coupondesc,'Franchise','i') and couponcodenew ne '50off' then DO; couponcodenew = 'Franchise';end;
if find(coupondesc,'CARRY OUT','i') or find(coupondesc,'carryout','i') then DO; couponcodenew = 'carryout';end;
if find(coupondesc,'St. Jude Donation','i') or find(coupondesc,'St. Jude Donations','i') or find(coupondesc,'ST.JUDE THANKS','i') 
then DO; couponcodenew = 'donation'; end;
if find(coupondesc,'Medium 1 Topping Pizza','i') then DO; couponcodenew = 'Med1TopPizza'; end;
if find(coupondesc,'Free Side Item','i') then DO; couponcodenew = 'sideitem'; end;
if find(coupondesc,'free','i') and find(coupondesc,'pizza','i') then DO; couponcodenew = 'freepizza'; end;
if find(coupondesc,'free drink','i') then DO; couponcodenew = 'freedrink'; end;
if find(coupondesc,'2 liter','i') or find(coupondesc,'2-Liter','i') or 
find(coupondesc,'2L','i') then DO; couponcodenew = 'drinkoffer'; end;
if find(coupondesc,'UPSELL','i') then DO; couponcodenew = 'UPSELL'; end;
if find(coupondesc,'MEDIUM PAN PIZZA WITH TWO TOPPINGS','i') or find(coupondesc,'2 MEDIUM 2 TOPPING PIZZA ','i') or
find(coupondesc,'MEDIUM 2-TOPPING PAN PIZZA','i') then DO; couponcodenew = 'MED PAN WITH 2 TOP'; end;
if find(coupondesc,'MEDIUM SPECIALTY PIZZA','i') then DO; couponcodenew = 'MED SPECIALTY PIZZA'; end;
/*not sure how fixed price is a coupon*/
if find(coupondesc,'FIXED PRICE','i') then DO; couponcodenew = 'FIXED PRICE'; end;
if find(coupondesc,'Free Delivery','i') then DO; couponcodenew = 'FREE DELIVERY'; end;
if couponcode = '9013' then DO; couponcodenew = 'Med3Topping'; end;
else if couponcodenew = '' then do; couponcodenew = 'No Coupon' ; end; 
RUN;
proc freq data=q.coupon_newdec;
tables couponcodenew;
run;
proc sort data=q.coupon_newdec tagsort;
by addressid;
run;
data q.coupon_newdec_clus(keep=addressid coupondesc couponcode couponcodenew cluster);
merge q.coupon_newdec(in=A) q.rfm_clus_6;
by addressid;
if A;
run;
ods rtf file='G:\Dominos\freq_cluster_coupon.rtf';
proc freq data=q.coupon_newdec_clus;
tables couponcodenew*cluster;
run;
ods rtf close;
/*Creating a coupon indicator*/
data q.coupon_newdec_clus;
set q.coupon_newdec_clus;
if couponcodenew = 'No_Coupon' then coupon_ind = 0; else coupon_ind = 1;
run;
/*Trying out to create a dataset for the association rule mining*/
data a;
set q.assoc1;
where coupon1 = '8469' or coupon2 = '8469' or coupon3 = '8469';
run;
data q.assoc1_wo8469;
set q.assoc1;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;
data q.assoc2_wo8469;
set q.assoc2;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;
data q.assoc3_wo8469;
set q.assoc3;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;
data q.assoc4_wo8469;
set q.assoc4;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;
data q.assoc5_wo8469;
set q.assoc5;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;
data q.assoc6_wo8469;
set q.assoc6;
if coupon1 = '8469' then coupon1 = '';
if coupon2 = '8469' then coupon2 = '';
if coupon3 = '8469' then coupon3 = '';
couponcodenew = catx(',',coupon1,coupon2,coupon3);
run;

data b;
set q.assoc2;
where coupon1 = '8469' or coupon2 = '8469' or coupon3 = '8469';
run;
proc contents data=q.assoc1;
run;
data a;
set q.assoc1_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc1_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Making in the form for the association rule for cluster 2*/
data a;
set q.assoc2_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc2_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Making in the form for the association rule for cluster 3*/
data a;
set q.assoc3_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc3_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Making in the form for the association rule for cluster 4*/
data a;
set q.assoc4_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc4_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Making in the form for the association rule for cluster 5*/
data a;
set q.assoc5_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc5_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Making in the form for the association rule for cluster 6*/
data a;
set q.assoc6_wo8469;
by addressid;
if first.addressid then output;
drop cluster couponcodenew;
run;
data a;
set a;
if coupon3 = "" then coupon3 = 'NA';
if coupon2 = "" then coupon2 = 'NA';
run;

proc transpose data=a out=b;
by addressid;
var coupon1 coupon2 coupon3;
run;
data q.assoc6_1;
set b;
if COL1 = 'NA' then delete;
if COL1 = '' then delete;
run;
/*Removing the Franchise Coupons and Multiple Coupons named as FRN and MC2 respectively.*/
data q.assoc1_1_1;
set q.assoc1_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
data q.assoc2_1_1;
set q.assoc2_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
data q.assoc3_1_1;
set q.assoc3_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
data q.assoc4_1_1;
set q.assoc4_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
data q.assoc5_1_1;
set q.assoc5_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
data q.assoc6_1_1;
set q.assoc6_1;
if COL1 = 'FRN' or COL1 = 'MC2' then delete;
run;
/*Selecting only target clusters as 2,4,6*/
data dom;
set q.coupon_newdec_clus;
where cluster in(2,4,6);
run;
data dom_2 dom_4 dom_6;
set dom;
if cluster = 2 then output dom_2;
if cluster = 4 then output dom_4;
if cluster = 6 then output dom_6;
run;
proc freq data=dom_2;
tables couponcodenew;
run;
/*Calculating high frequency coupons in major coupon description.*/
data dome_2_1;
set dom_2;
where couponcodenew in ('Large','carryout','combo');
run;
proc freq data=dom_4;
tables couponcodenew;
run;
data dome_4_1;
set dom_4;
where couponcodenew in ('Large','MED PAN WITH 2 TOP');
run;
data dome_4_2;
set dom_4;
where couponcodenew in ('drinkoffer');
run;
proc freq data=dome_4_2 order=freq;
tables couponcode;
run; 
data dome_4_3;
set dom_4;
where couponcodenew in ('combo');
run;
proc freq data=dome_4_3 order=freq;
tables couponcode;
run;
proc freq data=dom_6 order=freq;
tables couponcodenew;
run;
data dome_6_1;
set dom_6;
where couponcodenew in ('50off');
run;
proc freq data=dome_6_1 order=freq;
tables couponcode;
run; 
proc contents data=q.sample_rfm_clus out=contents;
run;
data q.sample_rfm_clus;
set q.sample_rfm_clus;
keep AddressId
BottleAmount
CLUSTER
ChickenOrd
ChsBreadOrd
CinnaStixOrd
DateOfOrder
DessertCount
DiscountAmount
DominatorCount
Drink12ozCount
Drink20ozCount
Drink2ltrCount
GarChsBreadOrd
GlutenFreeCrustCount
HandPieCount
KickersOrd
LavaCakeCount
LegendsCount
LgPieCount
MealPeriod
MedPieCount
OrderAmount
PanPieCount
PastaCount
PieCt
RectangulatorCount
Region
SandwichCount
SideOrd
SmallPieCount
ThinPieCount
ToppingCount
TwistyOrd
USregion
WingOrd
WkDay
XlgPieCount
add_order_amt
avg_12oz
avg_20oz
avg_2ltr
avg_cheesebread_ord
avg_chicken_ord
avg_cinnastix
avg_dessertcount
avg_dis_amt
avg_handpie
avg_lgpie
avg_medpie
avg_order_amt
avg_panpie
avg_pasta
avg_picnt
avg_sandwichcount
avg_sideord
avg_smallpie
avg_thinpie
avg_twistyord
avg_wingord
coupon_cnt
dinner_meal
dis_amt
late_dinner_meal
lunch_meal
topping_cnt
wk_fri
wk_mon
wk_sat
wk_sun
wk_thur
wk_tues
wk_wed;
run;
/*data a;*/
/*set q.sampledata;*/
/*where couponcode = 'MC2,LTY001,94' ;*/
/*run;*/
proc freq data=q.mergedmail_reg;
tables cluster*response;
run; 
data c;
set q.mergedmail_reg;
where cluster in (2,4,5,6);
run;
data b;
set c;
drop storenum domcustid SelectionProb SamplingWeight ;
if couponcode = 'No_Coupon' then coupon_ind = 0 ; else coupon_ind = 1;
run;
proc freq data=b;
tables response*coupon_ind;
run;
data q.mergedmail_logreg;
set b;
run;
proc contents data=q.mergedmail_logreg out=d;
run;
/*Modelling the logistic regression*/
proc logistic data=q.mergedmail_logreg desc;
class region cluster wkday mealperiod couponcodenew;
model response = BottleAmount
region
CLUSTER
ChickenOrd
ChsBreadOrd
CinnaStixOrd
DessertCount
DiscountAmount

Drink12ozCount
Drink20ozCount
Drink2ltrCount
GlutenFreeCrustCount
HandPieCount
IdealFoodCost
LgPieCount
MealPeriod
MedPieCount
MenuAmount
OrderAmount
PanPieCount
PastaCount
PieCt
couponcodenew
RectangulatorCount
SandwichCount
SideOrd
SmallPieCount
SurchargeAmount
ThinPieCount
ToppingCount
TwistyOrd
WingOrd
WkDay
XlgPieCount/ rsq lackfit ctable;
output out=try p=predicted;
run;
/*Calculating the contengency table taking apriori as 14%*/
data try1;
set try;
if predicted < 0.14 then pred=0;
else pred=1;
run;

proc freq data=try1;
table response*pred;
run;
/*proc freq data=try;*/
/*table response*_level_;*/
/*run;*/
proc freq data=q.mergedmail_logreg;
tables response;
run;
data q.mergedmail_logreg;set q.mergedmail_logreg;
format couponcodenew $20.;
if find(coupondesc,'MIX 2 OR MORE','i') and find(coupondesc,'MEDIUM','i') then DO; couponcodenew = 'combo';end;
if find(coupondesc,'LG','i') or find(coupondesc,'large','i') then DO; couponcodenew = 'Large';end;
if find(coupondesc,'50% OFF','i') then DO; couponcodenew = '50off';end;
if find(coupondesc,'Franchise','i') and couponcodenew ne '50off' then DO; couponcodenew = 'Franchise';end;
if find(coupondesc,'CARRY OUT','i') or find(coupondesc,'carryout','i') then DO; couponcodenew = 'carryout';end;
/*if find(coupondesc,'St. Jude Donation','i') or find(coupondesc,'St. Jude Donations','i') or find(coupondesc,'ST.JUDE THANKS','i') */
/*then DO; couponcodenew = 'donation'; end;*/
if find(coupondesc,'Medium 1 Topping Pizza','i') then DO; couponcodenew = 'Med1TopPizza'; end;
if find(coupondesc,'Free Side Item','i') then DO; couponcodenew = 'sideitem'; end;
if find(coupondesc,'free','i') and find(coupondesc,'pizza','i') then DO; couponcodenew = 'freepizza'; end;
if find(coupondesc,'free drink','i') then DO; couponcodenew = 'freedrink'; end;
if find(coupondesc,'2 liter','i') or find(coupondesc,'2-Liter','i') or 
find(coupondesc,'2L','i') then DO; couponcodenew = 'drinkoffer'; end;
if find(coupondesc,'UPSELL','i') then DO; couponcodenew = 'UPSELL'; end;
if find(coupondesc,'MEDIUM PAN PIZZA WITH TWO TOPPINGS','i') or find(coupondesc,'2 MEDIUM 2 TOPPING PIZZA ','i') or
find(coupondesc,'MEDIUM 2-TOPPING PAN PIZZA','i') then DO; couponcodenew = 'MED PAN WITH 2 TOP'; end;
if find(coupondesc,'MEDIUM SPECIALTY PIZZA','i') then DO; couponcodenew = 'MED SPECIALTY PIZZA'; end;
/*not sure how fixed price is a coupon*/
if find(coupondesc,'FIXED PRICE','i') then DO; couponcodenew = 'FIXED PRICE'; end;
if find(coupondesc,'Free Delivery','i') then DO; couponcodenew = 'FREE DELIVERY'; end;
if couponcode = '9013' then DO; couponcodenew = 'Med3Topping'; end;
else if coupondesc = '' then do; couponcodenew = 'No Coupon' ; end; 
RUN;
proc freq data=q.mergedmail_logreg;tables state*response;run;
data q.mergedmail_logreg;
set q.mergedmail_logreg;
drop KickersOrd
LavaCakeCount
LegendsCount
DominatorCount
GarChsBreadOrd;
run;
/*Creating the interaction variables.*/
data q.log;
set q.mergedmail_logreg;
drop couponcode coupondesc  discountamount F9amount
SurchargeAmount CustAmount IdealFoodCost RectangulatorCount dateoforder timeoforder x datemailed
cnt_of_visits;
Pasta_Sandwich = PastaCount * SandwichCount;
Largepie_drink = LgPieCount * Drink2ltrCount;
Medpie_drink = MedPieCount * Drink2ltrCount;
Largepie_Pasta = LgPieCount * PastaCount;
Medpie_Pasta = MedPieCount * PastaCount;
Lgpie_side = SideOrd * LgPieCount;
Medpie_side = SideOrd * MedPieCount;
Lgpie_wing = LgPieCount * Wingord;
Medpie_wing = MedpieCount * Wingord;
chicken_drink = ChickenOrd * Drink2ltrCount;
if wkday = '6' then sat = 1 ; else sat = 0;
if wkday = '5' then fri = 1 ; else fri = 0;
if wkday = '4' then thrus = 1; else thurs = 0;
if wkday = '7' then sun = 1 ; else sun = 0;
if mealperiod = '1' then lunch = 1; else lunch = 0;
if mealperiod = '2' then dinner = 1; else dinner = 0;
if mealperiod = '3' then late_dinner = 1; else late_dinner = 0;
sat_LgPieCount = sat * LgPieCount;
thurs_LgPieCount = thurs * LgPieCount;
sun_LgPieCount = sun * LgPieCount;
fri_LgPieCount = fri * LgPieCount;
sat_Med = sat * MedPieCount;
sun_Med = sun * MedPieCount;
fri_Med = fri * MedPieCount;
lunch_lgpiecount = lunch * LgPieCount;
lunch_panpiecount = lunch * Panpiecount;
lunch_sandwich = lunch * sandwichcount;
lunch_sideorder = lunch * sideord;
lunch_pasta = lunch * pastacount;
dinner_lgpiecount = dinner * LgPieCount;
dinner_panpiecount = dinner * Panpiecount;
dinner_sandwich = dinner * sandwichcount;
dinner_sideorder = dinner * sideord;
dinner_pasta = dinner * pastacount;
late_dinner_lgpiecount = late_dinner * LgPieCount;
late_dinner_panpiecount = late_dinner * Panpiecount;
late_dinner_sandwich = late_dinner * sandwichcount;
late_dinner_sideorder = late_dinner * sideord;
late_dinner_pasta = late_dinner * pastacount;
run;
proc contents data=q.log out=f;
run;
proc freq data=q.log;
tables state;
run;
/*Running the logistic regression */
proc logistic data=q.log desc;
class region cluster(ref='5' param=ref) wkday(ref='2' param=ref) 
mealperiod(ref='3' param=ref) couponcodenew(ref='No Coupon' param=ref) state(ref='Maine' param=ref); 
model response = CLUSTER
state
ChickenOrd
ChsBreadOrd
Drink2ltrCount
MealPeriod
MedPieCount
MenuAmount
PanPieCount
PastaCount
PieCt
SideOrd
SmallPieCount
ToppingCount
WingOrd
WkDay
coupon_ind
couponcodenew 
Largepie_Pasta
Medpie_Pasta 
late_dinner_lgpiecount 
 / rsq lackfit ctable outroc=roc;
output out=logistic p=pred;
run;
proc freq data= logistic;
tables response;
run;
/*Creating a contigency table considering apriori as 15.28%*/
data q.try1;
set logistic;
if pred < 0.1528 then predicted=0;
else predicted=1;
run;

proc freq data=q.try1;
table response*predicted;
run;
/*proc freq data=try;*/
/*table response*_level_;*/
/*run;*/

/*Calculating the lift.*/
proc rank data=logistic descending groups=10 out=logistic;
var pred;
ranks pred_rank;
run;

proc means data=logistic nway;
var response;
class pred_rank;
output out=lift sum=sums;
run;
/*Calculating the incremental sales */
/*Forming the control and treatment group based on the same addressid who are mailed but sometimes they used coupons and 
sometimes they did not use the coupons.*/
data q.response_incre_sales1;
set q.response_incre_sales;
where cluster in (4,5,6);
if couponcode = 'No_Coupon' then couponind = 0; else couponind = 1;
run;
data nc;
set q.response_incre_sales1;
by addressid;
where couponind = 0;
retain ctr;
if first.addressid then do; ctr = 0;end;
ctr = ctr + 1;
if last.addressid then do; ctr = ctr;end;
run;
data nc_unique;
set nc;
if ctr >1 then delete;
run;
data nc_id(keep=addressid);
set nc_unique;
run;
data c;
set q.response_incre_sales1;
by addressid;
where couponind = 1;
retain ctr;
if first.addressid then do; ctr = 0;end;
ctr = ctr + 1;
if last.addressid then do; ctr = ctr;end;
run;
data c_unique;
set c;
if ctr >1 then delete;
run;
data c_id(keep=addressid);
set c_unique;
run;
data incre_id;
merge nc_id(in=A) c_id(in=B);
by addressid;
if A and B;
run;
data q.nc_uniq;
merge nc_unique(in=A) incre_id(in=B);
by addressid;
run;
data q.c_uniq;
merge c_unique(in=A) incre_id(in=B);
by addressid;
run;
data q.incre_sales;
set q.nc_uniq q.c_uniq;
run;
proc sort data=q.incre_sales;
by addressid;
run;
data incre_sales1(keep=addressid);
set q.incre_sales;
by addressid;
retain ctr;
if first.addressid then do;ctr = 0 ; counter = ctr;end;
ctr = ctr + 1;
if last.addressid then do;ctr=ctr ; counter2 = ctr;end;
if counter <> counter2 then delete;
run;
data q.incre_sales1;
merge q.incre_sales(in=A) incre_sales1(in=B);
by addressid;
if A and B;
run;
proc contents data=q.incre_sales1 out=try4;
run;
/*Created the final file for calculating the incremental sales.*/
data q.incre_calc;
set q.incre_sales1;
keep addressid
CLUSTER
CustAmount
DiscountAmount
F9Amount
Profit
MenuAmount
OrderAmount
IdealFoodCost
cnt_of_visits
couponind
ctr
response
Profit1;
Profit = OrderAmount - IdealFoodCost;
Profit1 = CustAmount - IdealFoodCost;
run;
proc export data=q.incre_calc 
outfile='G:\Dominos\Second Stage\incre_cal.xls' dbms=xlsx replace;
run;











