-- AFSM ETL
-- Version 7 -- 5th Jan 2024
-- Created by and Copyright Adrian Power 2021-2024
-- There are three key sections to this file: initial load, transform, international load
	-- these need to be executed in order

-- NOTE: Update transform section with each version
-- search string: UPDATE EACH REPORT

/*
###############################################################################
INITIAL LOAD
###############################################################################
*/

/*

	Initial MYSQL Config

	1.
	Set up mysql connection in workbench to ensure can load from local file: https://bugs.mysql.com/bug.php?id=91872 
	Edit the connection, on the Connection tab, go to the 'Advanced' sub-tab, and in the 'Others:' box add the line 'OPT_LOCAL_INFILE=1'. 
	-- This should allow a client using the Workbench to run LOAD DATA INFILE as usual.

	2.
	SET GLOBAL local_infile = 'ON';
	-- https://www.mysqltutorial.org/import-csv-file-mysql-table/ 
	-- SHOW GLOBAL VARIABLES LIKE 'local_infile';
	-- SHOW GLOBAL VARIABLES LIKE 'local_infile';

	3.
	Disable safe mode in preferences to allow updating records.
	
	4.
	Older Microsoft Visual C++ Redistributables required to run MySQL on Windows 10

*/

SET GLOBAL local_infile = 'ON';

-- create database fungi;

use fungi;

drop table if exists obs;

-- 19 fields
CREATE TABLE obs (
-- basic
id VARCHAR(100) NOT NULL, 
observed_on DATE NULL, 
user_id VARCHAR(510) NULL, 
user_login VARCHAR(510) NULL, 
created_at DATE NULL, 				
quality_grade VARCHAR(510) NULL, 
-- geo
latitude DECIMAL(14, 10), 
longitude DECIMAL(14, 10), 
place_state_name VARCHAR(510) NULL, 
-- taxon
species_guess VARCHAR(510) NULL,
scientific_name VARCHAR(510) NULL,
common_name VARCHAR(510) NULL,
taxon_id VARCHAR(510) NULL,
-- extras
taxon_kingdom_name VARCHAR(510) NULL,
taxon_phylum_name VARCHAR(510) NULL,
taxon_class_name VARCHAR(510) NULL,
taxon_order_name VARCHAR(510) NULL,
taxon_genus_name VARCHAR(510) NULL,
taxon_species_name VARCHAR(510) NULL,
-- created
loaded timestamp(6) default current_timestamp(6) NOT NULL, -- enough precision to generate unique delta primary keys
PRIMARY KEY (id, loaded) -- composite key ensures that deltas get loaded and not ignored due to duplicate primary key of id only
);

-- fungi (date range to 2021-01-01)
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_fungi.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- deltas 2021 fungi (date range from 2021-01-01 - 2022-01-01)
-- always load deltas last for larger difference in time stamp
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_fungi_2021.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- deltas 2022 fungi (date range from 2022-01-01 - 2023-01-01)
-- always load deltas last for larger difference in time stamp
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_fungi_2022.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- deltas 2023 fungi (date range from 2023-01-01 - 2024-01-01)
-- always load deltas last for larger difference in time stamp
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_fungi_2023.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- slime
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_slime.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- red triangle slugs
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/obs_for_db_rts.csv' 
INTO TABLE obs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- resolve missing state name data for territories

update obs
set place_state_name = 'Northern Territory'
where place_state_name = '' and latitude > -28;

update obs
set place_state_name = 'Australian Capital Territory'
where place_state_name = '' and latitude < -28;

-- add identification tables

drop table if exists ids;

CREATE TABLE ids (
ids_date DATE NULL, 
taxon_kingdom_name VARCHAR(10) NOT NULL, 
`Rank` integer NULL, 				
User VARCHAR(510) NOT NULL, 
Identifications integer NULL,
loaded timestamp(6) default current_timestamp(6) NOT NULL,
PRIMARY KEY (User, taxon_kingdom_name) 
);

-- note, to prepare the identifcations summary stats:
-- copy from https://www.inaturalist.org/observations?iconic_taxa=Fungi&place_id=6744&view=identifiers
-- in excel, format CSV date yyyy-mm-dd, remove spaces from names and make numbers 'general' to remove commas

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/id_fungi.csv' 
INTO TABLE ids 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/id_protozoa.csv' 
INTO TABLE ids 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- load fungimap species

/*
Fungimap 1 List
*/

drop table if exists fungi.fungimap_1;

CREATE TABLE fungi.fungimap_1 (
scientific_name VARCHAR(255) NOT NULL,
PRIMARY KEY (scientific_name) 
);

insert into fungi.fungimap_1

values 
('Agaricus xanthodermus'),
('Amanita austroviridis'),
('Amanita muscaria'),
('Amanita phalloides'),
('Amanita xanthocephala'),
('Anthracophyllum archeri'),
('Armillaria luteobubalina'),
('Asterophora mirabilis'),
('Bolbitius titubans'),
('Coprinus comatus'),
('Cortinarius austroalbidus'),
('Cortinarius metallicus'),
('Cortinarius roseolilacinus'),
('Cortinarius rotundisporus'),
('Cortinarius sublargus'),
('Cortinarius symeae'),
('Cyptotrama asprata'),
('Cortinarius austrovenetus'),
('Cortinarius persplendidus'),
('Entoloma virescens'),
('Gymnopilus junonius'),
('Hebeloma aminophilum'),
('Hygrocybe cheelii'),
('Gliophorus graminicolor'),
('Porpolomopsis lewelliniae'),
('Lepista nuda'),
('Leucopaxillus lilacinus'),
('Marasmius elegans'),
('Marasmius oreades'),
('Roridomyces austrororidus'),
('Mycena interrupta'),
('Mycena leaiana australis'),
('Mycena nargan'),
('Cruentomycena viscidocruenta'),
('Neolentinus dactyloides'),
('Lichenomphalia chromacea'),
('Omphalotus nidiformis'),
('Oudemansiella radicata'),
('Panus fasciatus'),
('Pleurotus australis'),
('Schizophyllum commune'),
('Tubaria rufofulva'),
('Volvopluteus gloiocephalus'),
('Craterellus cornucopioides'),
('Podoserpula pusio'),
('Boletellus obscurecoccineus'),
('Sanguinoderma rude'),
('Panellus pusillus'),
('Fistulina hepatica'),
('Flabellophora superposita'),
('Gloeophyllum concentricum'),
('Laccocephalum hartmannii'),
('Laccocephalum mylittae'),
('Microporus affinis'),
('Microporus xanthopus'),
('Neolentiporus maculatissimus'),
('Piptoporus australiensis'),
('Beenakia dacostae'),
('Hericium coralloides'),
('Phlebia subceracea'),
('Cymatoderma elegans'),
('Stereum hirsutum'),
('Stereum ostrea'),
('Macrotyphula juncea'),
('Mucronella pendula'),
('Pseudohydnum gelatinosum'),
('Tremella fuciformis'),
('Tremella mesenterica'),
('Uromyces politus'),
('Astraeus hygrometricus'),
('Geastrum fornicatum'),
('Battarrea phalloides'),
('Calostoma fuhreri'),
('Calostoma fuscum'),
('Calostoma rodwayi'),
('Podaxis pistillaris'),
('Schizostoma laceratum'),
('Clathrus archeri'),
('Aseroe rubra'),
('Claustula fischeri'),
('Colus pusillus'),
('Ileodictyon cibarium'),
('Ileodictyon gracile'),
('Phallus indusiatus'),
('Cyttaria gunnii'),
('Drechmeria gunnii'),
('Cordyceps hawkesii'),
('Geomorium beatonii'),
('Cookeina tricholoma'),
('Helvella fibrosa'),
('Urnula campylospora'),
('Hypocreopsis amplectens'),
('Ascocoryne sarcoides'),
('Banksiamyces macrocarpus'),
('Poronia erici'),
('Morchella australiana'),
('Morchella esculenta'),
('Chlorovibrissea bicolor'),
('Leotia lubrica'),
('Vibrissea dura')
;

/*
Fungimap 2 List
*/

drop table if exists fungi.fungimap_2;

CREATE TABLE fungi.fungimap_2 (
scientific_name VARCHAR(255) NOT NULL,
taxon_kingdom_name VARCHAR(255) NOT NULL,
PRIMARY KEY (scientific_name) 
);

insert into fungi.fungimap_2

values 
('Amanita hemibapha', 'Fungi'),
('Amanita arenaria', 'Fungi'),
('Amanita armeniaca', 'Fungi'),
('Amanita flavella', 'Fungi'),
('Austropaxillus infundibuliformis', 'Fungi'),
('Cantharellus concinnus', 'Fungi'),
('Chlorophyllum brunneum', 'Fungi'),
('Chlorophyllum molybdites', 'Fungi'),
('Collybia eucalyptorum', 'Fungi'),
('Coprinellus micaceus', 'Fungi'),
('Cortinarius archeri', 'Fungi'),
('Cortinarius canarius', 'Fungi'),
('Cortinarius phalarus', 'Fungi'),
('Cortinarius sinapicolor', 'Fungi'),
('Cribbea gloriosa', 'Fungi'),
('Cribbea reticulata', 'Fungi'),
('Cyclocybe parasitica', 'Fungi'),
('Cystoderma muscicola', 'Fungi'),
('Descolea recedens', 'Fungi'),
('Entoloma albidosimulans', 'Fungi'),
('Entoloma austroroseum', 'Fungi'),
('Entoloma aromaticum', 'Fungi'),
('Entoloma carminicolor', 'Fungi'),
('Entoloma matthinae', 'Fungi'),
('Entoloma panniculus', 'Fungi'),
('Entoloma splendidum', 'Fungi'),
('Entoloma viridomarginatum', 'Fungi'),
('Galerina patagonica', 'Fungi'),
('Gymnopilus allantopus', 'Fungi'),
('Gyrophragmium inquinans', 'Fungi'),
('Hebeloma victoriense', 'Fungi'),
('Humidicutis viridimagentea', 'Fungi'),
('Hygrocybe astatogala', 'Fungi'),
('Hypholoma brunneum', 'Fungi'),
('Inocybe violaceocaulis', 'Fungi'),
('Laccaria sp. A (not broken down to sp.)', 'Fungi'),
('Lactarius eucalypti', 'Fungi'),
('Lentinellus tasmanicus', 'Fungi'),
('Lentinula lateritia', 'Fungi'),
('Lentinus sajor-caju', 'Fungi'),
('Lepista sublilacina', 'Fungi'),
('Leratiomyces ceres', 'Fungi'),
('Leucoagaricus leucothites', 'Fungi'), 
('Leucocoprinus birnbaumii', 'Fungi'),
('Limacella pitereka', 'Fungi'),
('Macrolepiota clelandii', 'Fungi'),
('Macrolepiota dolichaula', 'Fungi'),
('Marasmiellus affixus', 'Fungi'),
('Marasmius alveolaris', 'Fungi'),
('Melanophyllum haematospermum', 'Fungi'),
('Montagnea arenaria', 'Fungi'),
('Multifurca stenophylla', 'Fungi'), 
('Mycena cystidiosa', 'Fungi'),
('Mycena lazulina', 'Fungi'),
('Mycena toyerlaricola', 'Fungi'),
('Oudemansiella australis', 'Fungi'),
('Oudemansiella turbinispora', 'Fungi'), 
('Pholiota aurivella', 'Fungi'),
('Pleurotus tuber-regium', 'Fungi'), 
('Pluteus atromarginatus', 'Fungi'),
('Psathyrella echinata', 'Fungi'),
('Deconica horizontalis', 'Fungi'),
('Psilocybe subaeruginosa', 'Fungi'),
('Russula persanguinea', 'Fungi'),
('Scytinotus longinquus', 'Fungi'),
('Simocybe phlebophora', 'Fungi'),
('Xeromphalina leonina', 'Fungi'),
('Nidula emodensis', 'Fungi'),
('Sphaerobolus stellatus', 'Fungi'),
('Australopilus palumanus', 'Fungi'),
('Austroboletus lacunosus', 'Fungi'),
('Austroboletus subvirens', 'Fungi'),
('Boletus edulis', 'Fungi'),
('Phlebopus marginatus', 'Fungi'),
('Sutorius australiensis', 'Fungi'),
('Favolaschia claudopus', 'Fungi'),
('Filoboletus manipularis', 'Fungi'),
('Cordyceps meneristitis', 'Fungi'),
('Xylaria hypoxylon', 'Fungi'),
('Clavaria amoena', 'Fungi'),
('Clavaria zollingeri', 'Fungi'),
('Ramaria abietina', 'Fungi'),
('Ramaria botrytoides', 'Fungi'),
('Ramaria capitata', 'Fungi'),
('Ramaria fennica', 'Fungi'),
('Septobasidium clelandii', 'Fungi'),
('Annulohypoxylon bovei', 'Fungi'),
('Daldinia concentrica', 'Fungi'),
('Trichoderma gelatinosum', 'Fungi'),
('Hypoxylon howeanum', 'Fungi'),
('Neobarya agaricicola', 'Fungi'),
('Aleuria aurantia', 'Fungi'),
('Aleurina ferruginea', 'Fungi'),
('Chlorociboria aeruginascens', 'Fungi'),
('Cookeina insititia', 'Fungi'),
('Gyromitra tasmanica', 'Fungi'),
('Hymenotorrendiella eucalypti', 'Fungi'),
('Lachnum virgineum', 'Fungi'),
('Phaeohelotium baileyanum', 'Fungi'),
('Phillipsia subpurpurea', 'Fungi'),
('Scutellinia scutellata', 'Fungi'),
('Sowerbyella rhenana', 'Fungi'),
('Trichaleurina javanica', 'Fungi'), 
('Geastrum pectinatum', 'Fungi'),
('Cortinarius globuliformis', 'Fungi'),
('Gymnogaster boletoides', 'Fungi'),
('Descolea tenuipes', 'Fungi'),
('Heterotextus peziziformis', 'Fungi'),
('Badimiella pteridophila', 'Fungi'),
('Chrysothrix candelaris', 'Fungi'),
('Cladia muelleri', 'Fungi'),
('Nephroma australe', 'Fungi'),
('Psora decipiens', 'Fungi'),
('Teloschistes chrysophthalmus', 'Fungi'),
('Thamnolia vermicularis', 'Fungi'),
('Xanthoparmelia semiviridis', 'Fungi'),
('Aurantiporus pulcherrimus', 'Fungi'),
('Coltriciella dependens', 'Fungi'),
('Hexagonia vesparia', 'Fungi'),
('Laetiporus portentosus', 'Fungi'),
('Porodisculus pendulus', 'Fungi'),

('Ceratiomyxa fruticulosa', 'Protozoa'),
('Elaeomyxa cerifera', 'Protozoa'), -- updated from Eleomyxa cerifera
('Fuligo septica', 'Protozoa'),
('Hemitrichia serpula', 'Protozoa'),
('Leocarpus fragilis', 'Protozoa'),
('Lycogala epidendrum', 'Protozoa'),
('Physarum viride', 'Protozoa'),

('Auricularia delicata', 'Fungi'),
('Byssomerulius corium', 'Fungi'),
('Calyptella longipes', 'Fungi'),
('Cotylidia undulata', 'Fungi'),
('Henningsomyces candidus', 'Fungi'),
('Podoscypha petalodes', 'Fungi'),
('Stalked puffballs (not broken down to sp.)', 'Fungi'),
('Tulostoma pulchellum', 'Fungi'),
('Itajahya hornseyi', 'Fungi'),
('Phallus rubicundus', 'Fungi'),
('Auriscalpium barbatum', 'Fungi'),
('Gyrodontium sacchari', 'Fungi'),
('Hydnum repandum', 'Fungi')

;



/*

###############################################################################
TRANSFORM

-- IMPORTANT AT EACH LOAD: To produce neat charts
-- 		UPDATE the OPF Date RANGE in where clause (and update groups in 2023)
-- 		UPDATE the obs_temp where clause date ranges (created and observed) BELOW with each report
###############################################################################

*/

/*
creates a temporary table from the obs import to apply date functions for the obs_final table
*/
drop table if exists fungi.obs_temp;
CREATE TABLE IF NOT EXISTS fungi.obs_temp AS
(

-- flag duplicates for removal in where clause
WITH obs_no_dups AS(
   SELECT *,
       ROW_NUMBER()OVER(PARTITION BY id ORDER BY id) as ranked_id
   FROM fungi.obs
)

SELECT 
id, 
observed_on, 
user_id,
user_login,
created_at, 
quality_grade, 
latitude, 
longitude, 
case when length(place_state_name) = 0	or place_state_name is null	then 'unknown' else place_state_name end as place_state_name, 
case when length(species_guess) = 0 	or species_guess is null 	then 'unknown' else species_guess end as 	species_guess,
case when length(scientific_name) = 0 	or scientific_name is null 	then 'unknown' else scientific_name end as 	scientific_name,
case when length(common_name) = 0 		or common_name is null		then 'unknown' else common_name end as 		common_name,
taxon_id,
case when length(taxon_kingdom_name) = 0	or taxon_kingdom_name is null then 'unknown' else taxon_kingdom_name end as	taxon_kingdom_name,
case when length(taxon_phylum_name) = 0  	or taxon_phylum_name is null then 'unknown' else taxon_phylum_name end as 	taxon_phylum_name,
case when length(taxon_class_name) = 0 		or taxon_class_name is null then 'unknown' else taxon_class_name end as 	taxon_class_name,
case when length(taxon_order_name) = 0		or taxon_order_name is null then 'unknown' else taxon_order_name end as 	taxon_order_name,
case when length(taxon_genus_name) = 0		or taxon_genus_name is null then 'unknown' else taxon_genus_name end as 	taxon_genus_name,
case 
	when length(taxon_species_name) = 0 then 'unknown' 
	when length(taxon_species_name) = 1 then 'unknown' 
	when taxon_species_name is null then 'unknown' 
	else taxon_species_name end as taxon_species_name,
loaded,
week(observed_on) as observed_week, -- created here as transformed in next code chunk (obs final)
month(observed_on) as observed_month, -- created here as transformed in next code chunk (obs final)
datediff(created_at,observed_on) as created_lag, -- created here as transformed in next code chunk (obs final)
rank() over (partition by id order by loaded desc) as delta -- rank to return only updated records in next code chunk (obs final)
FROM obs_no_dups -- fungi.obs
where ranked_id = 1
	and quality_grade <> 'casual' 
	and observed_on <> '0000-00-00' 
	and observed_on <> '0001-01-01' 
	and observed_on is not null 
	and latitude > -44 -- excluding macquarie island obs so maps are neater
	-- UPDATE EACH REPORT	
	and observed_on <= '2023-12-31' 
	and created_at 	<= '2023-12-31'
);


/*
creates the obs_final table - final transformations / data cleansing
*/
drop table if exists fungi.obs_final;
CREATE TABLE IF NOT EXISTS fungi.obs_final AS
(
select
id, 
observed_on, 
user_id,
user_login, 
created_at, 
quality_grade, 
latitude, 
longitude, 
place_state_name, 
species_guess,
a.scientific_name,
common_name,
taxon_id,
a.taxon_kingdom_name,
taxon_phylum_name,
taxon_class_name,
taxon_order_name,
taxon_genus_name,
taxon_species_name,
case 
	when observed_week = 0 then 1 
    when observed_week = 53 then 52 
    else observed_week end as observed_week,
observed_month,
case 
	when observed_month = 12 then '4. Summer' 
	when observed_month = 1 then '4. Summer' 
    when observed_month = 2 then '4. Summer'
    when observed_month = 3 then '1. Autumn'
    when observed_month = 4 then '1. Autumn'
    when observed_month = 5 then '1. Autumn'
    when observed_month = 6 then '2. Winter'
    when observed_month = 7 then '2. Winter'
    when observed_month = 8 then '2. Winter'
    when observed_month = 9 then '3. Spring'
    when observed_month = 10 then '3. Spring'
    when observed_month = 11 then '3. Spring'
	else '4. Summer'
    end as observed_season,
case 
	when place_state_name = 'New South Wales' then 'NSW' 
    when place_state_name = 'Victoria' then 'VIC' 
    when place_state_name = 'Queensland' then 'QLD' 
    when place_state_name = 'Australian Capital Territory' then 'ACT' 
    when place_state_name = 'Northern Territory' then 'NT' 
    when place_state_name = 'Western Australia' then 'WA' 
    when place_state_name = 'South Australia' then 'SA' 
	when place_state_name = 'Tasmania' then 'TAS' 
	else 'unknown'
    end as state,
LAST_DAY(observed_on) as observed_on_month,
LAST_DAY(created_at) as created_month,
created_lag,
case 
	when created_lag < 0 then '1. Minus 1-2 days'
	when created_lag = 0 then '2. Same day'
	when created_lag > 0 and created_lag <= 2 then '3. 1-2 days'
	when created_lag > 2 and created_lag <= 14 then '4. 2 days to 2 weeks'
	when created_lag > 14 and created_lag <= 60 then '5. 2 weeks to 2 months'
	when created_lag > 60 and created_lag <= 720 then '6. 2 months to 2 years'
	else '7. More than 2 years'
	end as created_lag_cat,	
round(latitude,1) as latitude_reduced,
round(longitude,1) as longitude_reduced,
concat(taxon_class_name, ' > ', taxon_order_name, ' > ', taxon_genus_name) as cog_tree,
case
	when taxon_phylum_name = 'unknown' and taxon_class_name = 'unknown' and taxon_order_name = 'unknown' and taxon_genus_name = 'unknown' and taxon_species_name = 'unknown' then '1. Kingdom'
	when taxon_phylum_name <> 'unknown' and taxon_class_name = 'unknown' and taxon_order_name = 'unknown' and taxon_genus_name = 'unknown' and taxon_species_name = 'unknown' then '2. Phylum'
	when taxon_phylum_name <> 'unknown' and taxon_class_name <> 'unknown' and taxon_order_name = 'unknown' and taxon_genus_name = 'unknown' and taxon_species_name = 'unknown' then '3. Class'
	when taxon_phylum_name <> 'unknown' and taxon_class_name <> 'unknown' and taxon_order_name <> 'unknown' and taxon_genus_name = 'unknown' and taxon_species_name = 'unknown' then '4. Order'
	when taxon_phylum_name <> 'unknown' and taxon_class_name <> 'unknown' and taxon_order_name <> 'unknown' and taxon_genus_name <> 'unknown' and taxon_species_name = 'unknown' then '5. Genus'
	when taxon_phylum_name <> 'unknown' and taxon_class_name <> 'unknown' and taxon_order_name <> 'unknown' and taxon_genus_name <> 'unknown' and taxon_species_name <> 'unknown' then '6. Species'
	else '1. Kingdom' end as loc, -- level of completeness

case when fm1.scientific_name is not null then 1 else 0 end as FM_1,
case when fm2.scientific_name is not null then 1 else 0 end as FM_2,
case when fm1.scientific_name is not null then 'Y' else '.' end as FM_1_DESC,
case when fm2.scientific_name is not null then 'Y' else '.' end as FM_2_DESC,

case when quality_grade = 'research' then 1 else 0 end as RG,
1 as OBS

from fungi.obs_temp a
left join fungimap_1 fm1 on a.taxon_species_name = fm1.scientific_name -- note the join on species name is to avoid the obs from Complex https://www.inaturalist.org/taxa/1155105 which causes a discrepancy when joined on scientific_name 
left join fungimap_2 fm2 on a.taxon_species_name = fm2.scientific_name and a.taxon_kingdom_name = fm2.taxon_kingdom_name
where delta = 1 
);


drop table if exists fungi.obs_temp;

	/* URL formatting: attempts at getting a URL in a table value to work in R markdown - no success 
	-- concat("https://www.inaturalist.org/people/",user_login) as user_url,
	-- concat("[",user_login,"](https://www.inaturalist.org/people/",user_login,")") as user_url_markdown,
	-- concat("<a href=""https://www.inaturalist.org/people/",user_login,""">",user_login,"</a>") as user_url_html,
	-- concat("text_spec(""",user_login,""", link = ""https://www.inaturalist.org/people/",user_login,""")") as user_url_kable,
	-- concat("https://www.inaturalist.org/taxa/",taxon_id) as taxon_url,
	*/

	/* Data quality check for LOC 
	-- Replace the else flag per below:
	-- else 'flag' end as loc # level of completeness 
	-- select * from obs_final final where loc = 'flag'
	-- there are 15 or so records with unknown in the middle taxons - requested all missing to be flagged for curation on 20220729
	*/
	
/*
DATA Dates 
*/

drop table if exists fungi.data_dates;
CREATE TABLE IF NOT EXISTS fungi.data_dates AS
(

select
'Observations' as data_source,
taxon_kingdom_name,
min(observed_on) as data_from,
max(observed_on) as data_to
from obs_final
group by taxon_kingdom_name

union all

select
'Identifications' as data_source,
taxon_kingdom_name,
min(ids_date) as data_from,
max(ids_date) as data_to
from ids
group by taxon_kingdom_name

);


/*
NULL COUNTER - to understand the population of fields that are not always 100% populated.
*/

drop table if exists fungi.null_counter;
CREATE TABLE IF NOT EXISTS fungi.null_counter AS
(

with nulls_counter as (

select
-- null and length checks are done in obs_temp
taxon_kingdom_name,
case WHEN species_guess 	= 'unknown' then 1 else 0 end as nulls_species_guess, 
case WHEN scientific_name 	= 'unknown' then 1 else 0 end as nulls_scientific_name, 
case WHEN common_name		= 'unknown' then 1 else 0 end as nulls_common_name, 
case WHEN taxon_phylum_name	= 'unknown' then 1 else 0 end as nulls_taxon_phylum_name, 
case WHEN taxon_class_name	= 'unknown' then 1 else 0 end as nulls_taxon_class_name, 
case WHEN taxon_order_name 	= 'unknown' then 1 else 0 end as nulls_taxon_order_name, 
case WHEN taxon_genus_name 	= 'unknown' then 1 else 0 end as nulls_taxon_genus_name, 
case WHEN taxon_species_name = 'unknown' then 1 else 0 end as nulls_taxon_species_name,
1 as total_obs
from fungi.obs_final
where taxon_kingdom_name <> 'Animalia'
)

select
taxon_kingdom_name,
round(cast(sum(nulls_species_guess) as float)       / cast(sum(total_obs) as float),2) as species_guess, 
round(cast(sum(nulls_scientific_name) as float)     / cast(sum(total_obs) as float),2) as scientific_name, 
round(cast(sum(nulls_common_name) as float)        	/ cast(sum(total_obs) as float),2) as common_name, 
round(cast(sum(nulls_taxon_phylum_name) as float)   / cast(sum(total_obs) as float),2) as taxon_phylum_name, 
round(cast(sum(nulls_taxon_class_name) as float)    / cast(sum(total_obs) as float),2) as taxon_class_name, 
round(cast(sum(nulls_taxon_order_name) as float)    / cast(sum(total_obs) as float),2) as taxon_order_name, 
round(cast(sum(nulls_taxon_genus_name) as float)    / cast(sum(total_obs) as float),2) as taxon_genus_name, 
round(cast(sum(nulls_taxon_species_name) as float)  / cast(sum(total_obs) as float),2) as taxon_species_name
from nulls_counter
group by taxon_kingdom_name

)
;


/*
FIRST FIND - aggregates SPECIES counts. First OBS aggregates dates.
*/

drop table if exists fungi.first_find;
CREATE TABLE IF NOT EXISTS fungi.first_find AS
(

with first_observed as 
(
select
taxon_kingdom_name,
id,
scientific_name,
user_login,
observed_on,
rank() over (partition by taxon_kingdom_name, user_login order by observed_on, id) fo_rank
from fungi.obs_final
where taxon_kingdom_name <> 'Animalia'
),

counter as (
select
taxon_kingdom_name,
scientific_name,
count(scientific_name) as total
from first_observed
where fo_rank = 1
group by taxon_kingdom_name, scientific_name
order by 1,3 desc
)

select
taxon_kingdom_name,
scientific_name,
total,
row_number() over (partition by taxon_kingdom_name order by total desc) fo_row
from counter
order by 1,3 desc

);


/*
FIRST OBS - dates of first obs to understand engagement. First Find aggregates species.
*/

drop table if exists fungi.first_obs;
CREATE TABLE IF NOT EXISTS fungi.first_obs AS
(

with first_obs as (

SELECT 
id,
user_login,
taxon_kingdom_name,
min(created_month) as created_month
FROM fungi.obs_final
where taxon_kingdom_name <> 'Animalia'
group by taxon_kingdom_name, user_login

)

select
taxon_kingdom_name,
created_month,
count(distinct id) as Obs
from first_obs
group by taxon_kingdom_name, created_month

);


/*
OBS COUNT - obs per month.
*/

drop table if exists fungi.obs_count;
CREATE TABLE IF NOT EXISTS fungi.obs_count AS
(

SELECT 
taxon_kingdom_name,
observed_on_month,
count(distinct id) as Obs
FROM fungi.obs_final
where taxon_kingdom_name <> 'Animalia'
group by taxon_kingdom_name, observed_on_month

);




/*
#################################################################################
AUS RANKER 2
#################################################################################
*/


drop table if exists fungi.aus_ranker;
CREATE TABLE IF NOT EXISTS fungi.aus_ranker AS

with 


/*

OBS

*/

aus_obs as (
select
taxon_kingdom_name,
user_login,
count(id) as observations
from fungi.obs_final
group by taxon_kingdom_name, user_login
),

aus_obs_ranked as (
select
taxon_kingdom_name,
user_login,
observations,
rank() over (partition by taxon_kingdom_name order by observations desc) as observations_rank
from aus_obs
),

/*

SPECIES

*/

aus_species as (
select
taxon_kingdom_name,
user_login,
count(distinct scientific_name) as species
from fungi.obs_final
group by taxon_kingdom_name, user_login
),

aus_species_ranked as (
select
taxon_kingdom_name,
user_login,
species,
rank() over (partition by taxon_kingdom_name order by species desc) as species_rank
from aus_species
),


/*
research ratio
*/


all_id_count as (
select
taxon_kingdom_name,
user_login,
count(distinct id) as all_id_count
from fungi.obs_final
group by  taxon_kingdom_name, user_login
),

needs_id_count as (
select
taxon_kingdom_name,
user_login,
count(distinct id) as needs_id_count
from fungi.obs_final
where quality_grade = 'needs_id'
group by  taxon_kingdom_name, user_login
),

research_count as (
select
taxon_kingdom_name,
user_login,
count(distinct id) as research_count
from fungi.obs_final
where quality_grade = 'research'
group by taxon_kingdom_name, user_login
),


research_prop as (

select
distinct a.user_login,
a.taxon_kingdom_name,
case when b.needs_id_count is null then 0 else b.needs_id_count end as needs_id_count,
case when c.research_count is null then 0 else c.research_count end as research_count,
d.all_id_count as all_id_count,
case when c.research_count is null then cast(0 as float) else
	cast(c.research_count as float) / cast(d.all_id_count as float)	end as research_ratio

from fungi.obs_final as a
left join needs_id_count as b on a.user_login = b.user_login and a.taxon_kingdom_name = b.taxon_kingdom_name
left join research_count as c on a.user_login = c.user_login and a.taxon_kingdom_name = c.taxon_kingdom_name
left join all_id_count as d on a.user_login = d.user_login and a.taxon_kingdom_name = d.taxon_kingdom_name

order by 1 


),

research_ratio as (
select
user_login,
taxon_kingdom_name,
needs_id_count,
research_count,
all_id_count,
research_ratio,
rank() over (partition by taxon_kingdom_name order by research_ratio desc) research_ratio_rank
from research_prop
),

/*
species ratio
*/

species_ratio_generator as (
select
taxon_kingdom_name,
user_login,
count(distinct id) as total_observations,
count(distinct scientific_name) as species_count,
cast(count(distinct scientific_name) as float) / cast(count(distinct id) as float) as species_ratio
from fungi.obs_final
group by  taxon_kingdom_name, user_login
),

species_ratio as (
select
user_login,
taxon_kingdom_name,
total_observations,
species_count,
species_ratio,
rank() over (partition by taxon_kingdom_name order by species_ratio desc) species_ratio_rank
from species_ratio_generator
order by 5 desc
),

obs_total as (

select
taxon_kingdom_name,
count(distinct id) as observations_total
from fungi.obs_final
group by taxon_kingdom_name
),

species_total as (

select
taxon_kingdom_name,
count(distinct scientific_name) as species_total
from fungi.obs_final
group by taxon_kingdom_name
),

users_total as (

SELECT
taxon_kingdom_name,
count(distinct user_login) as users_total
from fungi.obs_final
group by taxon_kingdom_name
),

protozoa_totals as (

select
o.taxon_kingdom_name,

o.user_login,
ut.users_total,
cast(1 as float) / cast(ut.users_total as float) as user_prop,

o.observations as obs,
o.observations_rank as obs_rank,
ot.observations_total as obs_total,
cast(o.observations as float) / cast(ot.observations_total as float) as obs_prop,
case when o.observations_rank <= 50 then 'Top 50' else 'Others' end as obs_rank_cat,

s.species,
s.species_rank,
st.species_total,
cast(s.species as float) / cast(st.species_total as float) as species_prop,
case when s.species_rank <= 50 then 'Top 50' else 'Others' end as species_rank_cat,

r.needs_id_count,
r.research_count,
r.research_ratio,
r.research_ratio_rank as research_ratio_rank,

sr.species_ratio,
sr.species_ratio_rank,

i.Identifications as IDs,
i.Rank as ID_rank

from aus_obs_ranked as o
left join aus_species_ranked as s on o.user_login = s.user_login and o.taxon_kingdom_name = s.taxon_kingdom_name 
left join research_ratio as r on o.user_login = r.user_login and o.taxon_kingdom_name = r.taxon_kingdom_name 
left join species_ratio as sr on o.user_login = sr.user_login and o.taxon_kingdom_name = sr.taxon_kingdom_name 

left join obs_total as ot on o.taxon_kingdom_name = ot.taxon_kingdom_name
left join species_total as st on o.taxon_kingdom_name = st.taxon_kingdom_name
left join users_total as ut on o.taxon_kingdom_name = ut.taxon_kingdom_name

left join ids as i on i.user = o.user_login and i.taxon_kingdom_name = o.taxon_kingdom_name

where o.taxon_kingdom_name = 'Protozoa'

),

fungi_totals as (

select
o.taxon_kingdom_name,

o.user_login,
ut.users_total,
cast(1 as float) / cast(ut.users_total as float) as user_prop,

o.observations as obs,
o.observations_rank as obs_rank,
ot.observations_total as obs_total,
cast(o.observations as float) / cast(ot.observations_total as float) as obs_prop,
case when o.observations_rank <= 50 then 'Top 50' else 'Others' end as obs_rank_cat,

s.species,
s.species_rank,
st.species_total,
cast(s.species as float) / cast(st.species_total as float) as species_prop,
case when s.species_rank <= 50 then 'Top 50' else 'Others' end as species_rank_cat,

r.needs_id_count,
r.research_count,
r.research_ratio,
r.research_ratio_rank as research_ratio_rank,

sr.species_ratio,
sr.species_ratio_rank,

i.Identifications as IDs,
i.Rank as ID_rank

from aus_obs_ranked as o
left join aus_species_ranked as s on o.user_login = s.user_login and o.taxon_kingdom_name = s.taxon_kingdom_name 
left join research_ratio as r on o.user_login = r.user_login and o.taxon_kingdom_name = r.taxon_kingdom_name 
left join species_ratio as sr on o.user_login = sr.user_login and o.taxon_kingdom_name = sr.taxon_kingdom_name 

left join obs_total as ot on o.taxon_kingdom_name = ot.taxon_kingdom_name
left join species_total as st on o.taxon_kingdom_name = st.taxon_kingdom_name
left join users_total as ut on o.taxon_kingdom_name = ut.taxon_kingdom_name

left join ids as i on i.user = o.user_login and i.taxon_kingdom_name = o.taxon_kingdom_name

where o.taxon_kingdom_name = 'Fungi'

)

select 
*
from protozoa_totals

union all 

select 
*
from fungi_totals;




/*
RTS Ghost - Red Triangle Slug and Ghost Fungi distribution.
*/

drop table if exists fungi.rts_ghost;
CREATE TABLE IF NOT EXISTS fungi.rts_ghost AS

SELECT *,
case when scientific_name = 'Triboniophorus graeffei' then 'RTS' else 'Ghost' end as rts_ghost_flag
FROM fungi.obs_final
where scientific_name = 'Triboniophorus graeffei' or scientific_name = 'Omphalotus nidiformis' 

;


/*
RTS Ghost - OVERLAP
*/
drop table if exists fungi.rts_ghost_overlap;
CREATE TABLE IF NOT EXISTS fungi.rts_ghost_overlap AS
(

with every_point as (

SELECT
rts_ghost_flag,
round(latitude,4) as latitude_reduced_4,
round(longitude,4) as longitude_reduced_4,
concat('lat',round(latitude,4),'lon',round(longitude,4)) as lat_lon_4_combined
from fungi.rts_ghost

),

every_point_unique as (

SELECT
distinct lat_lon_4_combined,
latitude_reduced_4,
longitude_reduced_4
from every_point

),

every_point_unique_RTS as (

SELECT
distinct lat_lon_4_combined,
latitude_reduced_4,
longitude_reduced_4,
1 as RTS
from every_point
where rts_ghost_flag = 'RTS'
),

every_point_unique_Ghost as (

SELECT
distinct lat_lon_4_combined,
latitude_reduced_4,
longitude_reduced_4,
1 as Ghost
from every_point
where rts_ghost_flag = 'Ghost'
),

combiner as (

select
distinct a.lat_lon_4_combined,
a.latitude_reduced_4,
a.longitude_reduced_4,
COALESCE(b.RTS,0) as RTS,
COALESCE(c.GHOST,0) as Ghost
from every_point_unique a
left join every_point_unique_RTS b on a.lat_lon_4_combined = b.lat_lon_4_combined
left join every_point_unique_Ghost c on a.lat_lon_4_combined = c.lat_lon_4_combined
)

select
lat_lon_4_combined,
latitude_reduced_4,
longitude_reduced_4,
RTS,
Ghost,
case when RTS = 1 and Ghost = 0 then 'RTS Only'
	 when RTS = 0 and Ghost = 1 then 'Ghost Only'
	 when RTS = 1 and Ghost = 1 then 'Both'
	 else 'Error' end as Overlap
from combiner

);


/*
RG Species Rank - RESEARCH GRADE Species rank - to get aggregated RG list.
*/

drop table if exists fungi.rg_species_rank;
CREATE TABLE IF NOT EXISTS fungi.rg_species_rank AS
(

with spec_rg_counts as 
(
select
id,
taxon_kingdom_name,
scientific_name,
quality_grade,
rg,
obs,
FM_1_DESC,
FM_2_DESC
from fungi.obs_final
where taxon_kingdom_name <> 'Animalia' and loc = '6. Species'

)

select
taxon_kingdom_name,
scientific_name,
sum(rg) as rg_count,
sum(obs) as obs_count,
round(cast(sum(rg) as float) / cast(sum(obs) as float),3) as spec_rg_prop,
FM_1_DESC as FM_1,
FM_2_DESC as FM_2
from spec_rg_counts
group by taxon_kingdom_name, scientific_name

);


/*
RESEARCH GRADE at Kingdom level
*/

drop table if exists fungi.rg_kingdom;
CREATE TABLE IF NOT EXISTS fungi.rg_kingdom AS
(

with rg_counts as 
(
select
id,
taxon_kingdom_name,
scientific_name,
quality_grade,
rg
from fungi.obs_final
where taxon_kingdom_name <> 'Animalia'

)

select
taxon_kingdom_name,
sum(rg) as rg_count,
count(distinct id) as obs_count,
round(cast(sum(rg) as float) / cast(count(distinct id) as float),3) as rg_prop
from rg_counts
group by taxon_kingdom_name

);

/*
RESEARCH GRADE Kingdom Species - count of species LOC at RG
*/

drop table if exists fungi.rg_kingdom_spec;
CREATE TABLE IF NOT EXISTS fungi.rg_kingdom_spec AS
(

with kindom_count as (
select
taxon_kingdom_name,
rg_count,
obs_count,
rg_prop
from rg_kingdom
group by taxon_kingdom_name
),

spec_rg_counts_loc as 
(
select
id,
taxon_kingdom_name,
rg
from fungi.obs_final
where taxon_kingdom_name <> 'Animalia' and loc = '6. Species'
),

final_loc as (
select
taxon_kingdom_name,
sum(rg) as rg_count,
count(distinct ID) as obs_count,
round(cast(sum(rg) as float) / cast(count(distinct ID) as float),3) as spec_rg_prop
from spec_rg_counts_loc
group by taxon_kingdom_name
)

SELECT
a.taxon_kingdom_name,
a.rg_count,
a.obs_count,
a.rg_prop,
b.rg_count as rg_count_spec,
b.obs_count as obs_count_spec,
b.spec_rg_prop as rg_prop_spec
from kindom_count a
left join final_loc b on a.taxon_kingdom_name = b.taxon_kingdom_name

);

/*
LOC Kingdom
*/

drop table if exists fungi.loc_kingdom;
CREATE TABLE IF NOT EXISTS fungi.loc_kingdom AS
(

with loc_p as (

select
LOC,
count(distinct id) as 'Protozoa',
(select count(loc) from obs_final where taxon_kingdom_name = 'Protozoa') as p_denom
from obs_final
where taxon_kingdom_name = 'Protozoa'
group by loc
order by 1
),

loc_f as (
select
LOC,
count(distinct id) as 'Fungi',
(select count(loc) from obs_final where taxon_kingdom_name = 'Fungi') as f_denom
from obs_final
where taxon_kingdom_name = 'Fungi'
group by loc
order by 1
)

select
a.LOC,
a.Fungi,
cast(a.Fungi as float) / cast(a.f_denom as float) as LOC_Prop_Fungi,
b.Protozoa,
cast(b.Protozoa as float) / cast(b.p_denom as float) as LOC_Prop_Protozoa
from loc_f a
left join loc_p b on a.loc = b.loc
order by 1

);


/*
ID stats
*/

drop table if exists fungi.id_summary_stats;
CREATE TABLE IF NOT EXISTS fungi.id_summary_stats AS
(

select 
taxon_kingdom_name,
count(distinct user) as Total_Users,
sum(Identifications) as Total_IDs,
round(avg(Identifications),0) as Average_IDs,
max(Identifications) as Higest_User_IDs,
round(cast(max(Identifications) as float) / cast(sum(Identifications) as float),4) as Highest_User_Prop_Total
from
ids
group by taxon_kingdom_name

)
;


/*
ID top 50 stats
*/

drop table if exists fungi.obs_and_id_top_50;
CREATE TABLE IF NOT EXISTS fungi.obs_and_id_top_50 AS
(

with 

top_50_ids_fungi as (

select 
*,
'Top 50 IDs' as ids_category
from
ids
where `Rank` < 51
and taxon_kingdom_name = 'Fungi'
order by `rank`, user
limit 50
),

top_50_ids_protozoa as (

select 
*,
'Top 50 IDs' as ids_category
from
ids
where `Rank` < 51
and taxon_kingdom_name = 'Protozoa'
order by `rank`, user
limit 50
),

top_50_obs_fungi as (

select 
*
from
aus_ranker
where obs_rank < 51
and taxon_kingdom_name = 'Fungi'
order by obs_rank, user_login
limit 50
),

top_50_obs_protozoa as (

select 
*
from
aus_ranker
where obs_rank < 51
and taxon_kingdom_name = 'Protozoa'
order by obs_rank, user_login
limit 50
),

fungi_obs_id_combined as (

select
a.user_login,
a.obs_rank,
a.obs,
b.rank as id_rank,
b.Identifications as ids
from top_50_obs_protozoa a
left join top_50_ids_fungi b on a.user_login = b.user

),

protozoa_obs_id_combined as (

select
a.user_login,
a.obs_rank,
a.obs,
b.rank as id_rank,
b.Identifications as ids
from top_50_obs_protozoa a
left join top_50_ids_protozoa b on a.user_login = b.user

),

unioner as (

select 
*,
case when id_rank is null then 'No' else 'Yes' end as obs_and_id_top_50,
'Fungi' as taxon_kingdom_name 
from fungi_obs_id_combined

union all

select 
*,
case when id_rank is null then 'No' else 'Yes' end as obs_and_id_top_50,
'Protozoa' as taxon_kingdom_name 
from protozoa_obs_id_combined
),

grouper as (

select 
taxon_kingdom_name,
obs_and_id_top_50,
count(obs_and_id_top_50) as Number
from unioner
group by taxon_kingdom_name, obs_and_id_top_50
order by taxon_kingdom_name asc, obs_and_id_top_50 desc
)

select
a.taxon_kingdom_name,
round(cast(a.number as float) / cast(50 as float),2) as Prop_Top_50_Ids_in_Top_50_Obs
from
grouper as a
where a.obs_and_id_top_50 = 'Yes' 

)
;


/*
State stats
*/

drop table if exists fungi.state_stats;
CREATE TABLE IF NOT EXISTS fungi.state_stats AS
(

SELECT 
taxon_kingdom_name,
place_state_name,
count(distinct user_login) as Users,
count(distinct id) as Obs
FROM fungi.obs_final
where taxon_kingdom_name <> 'Animalia'
group by taxon_kingdom_name, place_state_name
order by 1,3 desc

)
;



/*
OPF Cumulative Total
*/

drop table if exists fungi.opf_cumulative_total;
CREATE TABLE IF NOT EXISTS fungi.opf_cumulative_total AS

with opf as (
SELECT 
id,
observed_on_month
FROM fungi.obs_final
where taxon_genus_name = 'Favolaschia' and taxon_species_name <> 'Favolaschia manipularis' and taxon_species_name <> 'Favolaschia pustulosa'
),

opf_month_total as (

select 
observed_on_month,
count(distinct id) as obs
from opf
group by observed_on_month
order by 1 
)

select 
t1.observed_on_month,
t1.obs as opf_obs,
sum(t2.obs) as Cumulative_Total
from
(
	select 
	observed_on_month,
	count(distinct id) as obs
	from opf
	group by observed_on_month
	order by 1 
) as t1
inner join -- self join
(
	select 
	observed_on_month,
	count(distinct id) as obs
	from opf
	group by observed_on_month
	order by 1 
) as t2
on t1.observed_on_month >= t2.observed_on_month
group by t1.observed_on_month
order by t1.observed_on_month
;



/*
OPF Map
*/


drop table if exists fungi.opf_map;
CREATE TABLE IF NOT EXISTS fungi.opf_map AS
(


with opf as (
SELECT 
id,
observed_on,
observed_on_month,
user_login,
latitude,
longitude,
latitude_reduced,
longitude_reduced
FROM fungi.obs_final
where taxon_genus_name = 'Favolaschia' and taxon_species_name <> 'Favolaschia manipularis' and taxon_species_name <> 'Favolaschia pustulosa'
),

opf_range_1 as (

select 
id,
latitude_reduced,
longitude_reduced,
'1. To end 2014' as Date_Range
from opf
where observed_on_month <= '2014-12-31'
 
),

opf_range_2 as (

select 
id,
latitude_reduced,
longitude_reduced,
'2. To end 2018' as Date_Range
from opf
where observed_on_month <= '2018-12-31'
 
),

opf_range_3 as (

select 
id,
latitude_reduced,
longitude_reduced,
'3. To end 2022' as Date_Range
from opf
where observed_on_month <= '2022-12-31'
 
),


/*
UPDATE OPF 4 FOR NEXT TIME
UPDATE EACH REPORT
OPF RANGE

*/

opf_range_4 as (

select 
id,
latitude_reduced,
longitude_reduced,
'4. To end 2023' as Date_Range
from opf
where observed_on_month < '2023-12-31'
 
)

select * from opf_range_1
union all
select * from opf_range_2
union all
select * from opf_range_3
union all
select * from opf_range_4


)
;




/*
Myxos tree
*/


drop table if exists fungi.myxo_tree;
CREATE TABLE IF NOT EXISTS fungi.myxo_tree AS
(

with myxo_tree as (

SELECT 
cog_tree as Mycetozoa_Tree,
count(cog_tree) as Branch_Obs
FROM fungi.obs_final
where taxon_phylum_name = 'Mycetozoa' 
group by cog_tree
),

myxo_tree_rg as (
select
cog_tree as cog_tree_rg,
count(cog_tree) as myxo_tree_rg
FROM fungi.obs_final
where taxon_phylum_name = 'Mycetozoa' and quality_grade = 'research'
group by cog_tree
),

myxo_tree_spec as (
select
cog_tree as cog_tree_spec,
count(distinct taxon_species_name) as Species
FROM fungi.obs_final
where taxon_phylum_name = 'Mycetozoa' and taxon_species_name <> 'unknown'
group by cog_tree
),

mixer as (
select 
a.Mycetozoa_Tree,
a.Branch_Obs,
c.Species,
b.myxo_tree_rg as Branch_RG,
round(cast(b.myxo_tree_rg as float) / cast(a.Branch_Obs as float),3) as RG_Prop
from myxo_tree a
left join myxo_tree_spec c on a.Mycetozoa_Tree = c.cog_tree_spec
left join myxo_tree_rg b on a.Mycetozoa_Tree = b.cog_tree_rg
order by a.Mycetozoa_Tree
)

select
Mycetozoa_Tree,
Branch_Obs,
case when Species is null then 0 else Species end as Species,
case when Branch_RG is null then 0 else Branch_RG end as Branch_RG,
case when RG_Prop is null then 0 else RG_Prop end as RG_Prop
from mixer
order by Mycetozoa_Tree

)
;



/*
lat_lon POINT heat maps reduced 
*/

drop table if exists fungi.point_heat_fungi;
CREATE TABLE IF NOT EXISTS fungi.point_heat_fungi AS
(
with latlon_fungi as (

SELECT
id,
taxon_kingdom_name,
latitude_reduced,
longitude_reduced,
concat('lat',latitude_reduced,'lon',longitude_reduced) as lat_lon_combined
FROM fungi.obs_final
where taxon_kingdom_name = 'Fungi'

),

counter as (
select
lat_lon_combined,
latitude_reduced,
longitude_reduced,
count(id) as Obs
from latlon_fungi
group by lat_lon_combined
)

select 
latitude_reduced,
longitude_reduced,
Obs,
null as Obs_Cat
from counter
)
;

drop table if exists fungi.point_heat_protozoa;
CREATE TABLE IF NOT EXISTS fungi.point_heat_protozoa AS
(
with latlon_protozoa as (

SELECT
id,
taxon_kingdom_name,
latitude_reduced,
longitude_reduced,
concat('lat',latitude_reduced,'lon',longitude_reduced) as lat_lon_combined
FROM fungi.obs_final
where taxon_kingdom_name = 'Protozoa'

),

counter as (
select
lat_lon_combined,
latitude_reduced,
longitude_reduced,
count(id) as Obs
from latlon_protozoa
group by lat_lon_combined
)

select 
latitude_reduced,
longitude_reduced,
Obs,
null as Obs_Cat
from counter
)
;




/*
Fungimap 1 Analysis
*/

drop table if exists fungi.fungimap_1_analysis;
CREATE TABLE IF NOT EXISTS fungi.fungimap_1_analysis AS
(

select
coalesce(b.cog_tree,'x No obs x') as cog_tree,
a.scientific_name,
coalesce(sum(b.rg),0) as RG,
coalesce(sum(b.obs),0) as Obs,
round(cast(coalesce(sum(b.rg),0) as float) / cast(coalesce(sum(b.obs),0) as float),3) as RG_Prop,
count(distinct b.user_id) as Users,
count(distinct b.state) as 'States / Territories'
from
fungi.fungimap_1 a
left join fungi.obs_final b on a.scientific_name = b.scientific_name
group by b.cog_tree, a.scientific_name
order by 1,2

)
;


/*
Fungimap 1 proportion of total obs
*/

drop table if exists fungi.fungimap_1_prop;
CREATE TABLE IF NOT EXISTS fungi.fungimap_1_prop AS

select
sum(fm_1) as FM1,
sum(obs) as Total,
round(cast(sum(fm_1) as float) / cast(sum(obs) as float),3) as FM1_Prop
from obs_final
where taxon_kingdom_name = 'Fungi'
;


/*
Fungimap 1 proportion of total obs - SPECIES LEVEL
*/

drop table if exists fungi.fungimap_1_prop_spec;
CREATE TABLE IF NOT EXISTS fungi.fungimap_1_prop_spec AS

select
sum(fm_1) as FM1,
sum(obs) as Total,
round(cast(sum(fm_1) as float) / cast(sum(obs) as float),3) as FM1_Prop
from obs_final
where taxon_kingdom_name = 'Fungi' and loc = '6. Species'
;



/*
Fungimap 2 Analysis - FUNGI
*/

drop table if exists fungi.fungimap_2_analysis;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_analysis AS
(

select
coalesce(b.cog_tree,'x No obs x') as cog_tree,
a.scientific_name,
coalesce(sum(b.rg),0) as RG,
coalesce(sum(b.obs),0) as Obs,
round(cast(coalesce(sum(b.rg),0) as float) / cast(coalesce(sum(b.obs),0) as float),3) as RG_Prop,
count(distinct b.user_id) as Users,
count(distinct b.state) as 'States / Territories'
from
fungi.fungimap_2 a
left join fungi.obs_final b on a.scientific_name = b.scientific_name 
where a.taxon_kingdom_name = 'Fungi'
group by b.cog_tree, a.scientific_name
order by 1,2
)
;


/*
Fungimap 2 proportion of total obs - FUNGI
*/

drop table if exists fungi.fungimap_2_prop;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_prop AS

select
sum(fm_2) as FM2,
sum(obs) as Total,
round(cast(sum(fm_2) as float) / cast(sum(obs) as float),3) as FM2_Prop
from obs_final
where taxon_kingdom_name = 'Fungi'

;

/*
Fungimap 2 proportion of total obs - SPECIES LEVEL - FUNGI
*/

drop table if exists fungi.fungimap_2_prop_spec;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_prop_spec AS

select
sum(fm_2) as FM2,
sum(obs) as Total,
round(cast(sum(fm_2) as float) / cast(sum(obs) as float),3) as FM2_Prop
from obs_final
where taxon_kingdom_name = 'Fungi' and loc = '6. Species'
;

/*
Fungimap 2 Analysis - PROTOZOA
*/

drop table if exists fungi.fungimap_2_analysis_protozoa;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_analysis_protozoa AS
(

select
coalesce(b.cog_tree,'x No obs x') as cog_tree,
a.scientific_name,
coalesce(sum(b.rg),0) as RG,
coalesce(sum(b.obs),0) as Obs,
round(cast(coalesce(sum(b.rg),0) as float) / cast(coalesce(sum(b.obs),0) as float),3) as RG_Prop,
count(distinct b.user_id) as Users,
count(distinct b.state) as 'States / Territories'
from
fungi.fungimap_2 a
left join fungi.obs_final b on a.scientific_name = b.scientific_name
where a.taxon_kingdom_name = 'Protozoa' 
group by b.cog_tree, a.scientific_name
order by 1,2
)
;


/*
Fungimap 2 proportion of total obs - FUNGI
*/

drop table if exists fungi.fungimap_2_prop_protozoa;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_prop_protozoa AS

select
sum(fm_2) as FM2,
sum(obs) as Total,
round(cast(sum(fm_2) as float) / cast(sum(obs) as float),3) as FM2_Prop
from obs_final
where taxon_kingdom_name = 'Protozoa'

;

/*
Fungimap 2 proportion of total obs - SPECIES LEVEL - FUNGI
*/

drop table if exists fungi.fungimap_2_prop_spec_protozoa;
CREATE TABLE IF NOT EXISTS fungi.fungimap_2_prop_spec_protozoa AS

select
sum(fm_2) as FM2,
sum(obs) as Total,
round(cast(sum(fm_2) as float) / cast(sum(obs) as float),3) as FM2_Prop
from obs_final
where taxon_kingdom_name = 'Protozoa' and loc = '6. Species'
;


/*
Protozoa and Fungi Observers Combination

- Proportions of protozoa observers who also observe fungi
- are they predominately the same people and are protozoa obs therefore mostly dependent on fungi observers

*/

drop table if exists fungi.protozoa_fungi_users;
CREATE TABLE IF NOT EXISTS fungi.protozoa_fungi_users AS
(


with fungi_users as (

select
distinct user_login,
1 as F
from fungi.obs_final
where taxon_kingdom_name = 'Fungi'
),

protozoa_users as (

select
distinct user_login,
1 as P
from fungi.obs_final
where taxon_kingdom_name = 'Protozoa'
),

combiner as (
select
P,
coalesce(b.F,0) as F
from protozoa_users a
left join fungi_users b on a.user_login = b.user_login
)

select
sum(P) as Protozoa_Observers,
sum(F) as Also_Fungi_Observers,
round(cast(sum(F) as float) / cast(sum(P) as float),3) as Prop_AFO
from combiner

)
;



/*
Users Cumulative Total
*/

drop table if exists fungi.users_cumulative_total;
CREATE TABLE IF NOT EXISTS fungi.users_cumulative_total AS


with users_cum as (
SELECT 
taxon_kingdom_name,
user_login,
min(created_month) as aggr_month
FROM fungi.obs_final
where  taxon_kingdom_name <> 'Animalia'
group by taxon_kingdom_name, user_login
),

user_month_total as (

select 
taxon_kingdom_name,
aggr_month,
count(user_login) as aggr_users
from users_cum
group by taxon_kingdom_name, aggr_month
order by 1,2 
),

user_cum_total as (

select 
	t1.taxon_kingdom_name,
	t1.aggr_month,
	'Users' as Total_Cat,
	t1.users as Count,
	sum(t2.users) as Cumulative_Total
from
(
	select 
	taxon_kingdom_name,
	aggr_month,
	sum(aggr_users) as users
	from user_month_total
	group by taxon_kingdom_name, aggr_month
	order by 1 
) as t1
inner join -- this is self join
(
	select 
	taxon_kingdom_name,
	aggr_month,
	sum(aggr_users) as users
	from user_month_total
	group by taxon_kingdom_name, aggr_month
	order by 1 
) as t2
on t1.aggr_month >= t2.aggr_month and t1.taxon_kingdom_name = t2.taxon_kingdom_name
-- this should also work 
-- on t1.OrderID >= t2.OrderID
group by t1.taxon_kingdom_name, t1.aggr_month, 'Users'
order by t1.taxon_kingdom_name, t1.aggr_month, 'Users'

)

select * from user_cum_total

;


/*
OBS Cumulative Total
*/

drop table if exists fungi.obs_cumulative_total;
CREATE TABLE IF NOT EXISTS fungi.obs_cumulative_total AS

with obs_cum as (
SELECT 
taxon_kingdom_name,
observed_on_month as aggr_month,
count(id) as aggr_obs
FROM fungi.obs_final
where  taxon_kingdom_name <> 'Animalia'
group by taxon_kingdom_name, observed_on_month
order by 1,2
),

obs_cum_total as (

select 
	t1.taxon_kingdom_name,
	t1.aggr_month,
	'Obs' as Total_Cat,
	t1.obs as Count,
	sum(t2.obs) as Cumulative_Total
from
(
	select 
	taxon_kingdom_name,
	aggr_month,
	sum(aggr_obs) as obs
	from obs_cum
	group by taxon_kingdom_name, aggr_month
	order by 1 
) as t1
inner join -- self join
(
	select 
	taxon_kingdom_name,
	aggr_month,
	sum(aggr_obs) as obs
	from obs_cum
	group by taxon_kingdom_name, aggr_month
	order by 1 
) as t2
on t1.aggr_month >= t2.aggr_month and t1.taxon_kingdom_name = t2.taxon_kingdom_name
group by t1.taxon_kingdom_name, t1.aggr_month, 'Obs'
order by t1.taxon_kingdom_name, t1.aggr_month, 'Obs'

)

select * from obs_cum_total
;


/*

###############################################################################
INTERNATIONAL LOAD
###############################################################################

/*

Loads and transforms the international observation data for comparison.

Notes:

In future versions to support machine learning ADD the following fields, and remove genus derived:
	taxon_kingdom_name VARCHAR(510) NULL,
	taxon_phylum_name VARCHAR(510) NULL,
	taxon_class_name VARCHAR(510) NULL,
	taxon_order_name VARCHAR(510) NULL,
	taxon_genus_name VARCHAR(510) NULL,
	taxon_species_name VARCHAR(510) NULL,

*/

SET GLOBAL local_infile = 'ON';

use fungi;

-- ####################################
-- Import International Observations by Region
-- ####################################

-- East Asia (region name in iNaturalist)

drop table if exists fungi.fungi_east_asia;

CREATE TABLE fungi.fungi_east_asia (
-- basic
id VARCHAR(100) NOT NULL, 
observed_on DATE NULL, 
user_id VARCHAR(510) NULL, 
user_login VARCHAR(510) NULL, 
created_at DATE NULL, 				
quality_grade VARCHAR(510) NULL, 
-- geo
latitude DECIMAL(14, 10), 
longitude DECIMAL(14, 10), 
place_state_name VARCHAR(510) NULL, 
place_country_name VARCHAR(510) NULL, 
-- taxon
scientific_name VARCHAR(510) NULL,
common_name VARCHAR(510) NULL,
taxon_id VARCHAR(510) NULL,
-- created
loaded timestamp(6) default current_timestamp(6) NOT NULL, -- enough precision to generate unique delta primary keys
PRIMARY KEY (id, loaded) -- composite key ensures that deltas get loaded and not ignored due to duplicate primary key of id only
);

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fungi_east_asia.csv' 
INTO TABLE fungi_east_asia 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE fungi.fungi_east_asia
ADD COLUMN region VARCHAR(100) AFTER id;

UPDATE fungi.fungi_east_asia
SET region = 'East Asia';


-- Northern Europe (region name in iNaturalist)

drop table if exists fungi.fungi_northern_europe;

CREATE TABLE fungi.fungi_northern_europe (
-- basic
id VARCHAR(100) NOT NULL, 
observed_on DATE NULL, 
user_id VARCHAR(510) NULL, 
user_login VARCHAR(510) NULL, 
created_at DATE NULL, 				
quality_grade VARCHAR(510) NULL, 
-- geo
latitude DECIMAL(14, 10), 
longitude DECIMAL(14, 10), 
place_state_name VARCHAR(510) NULL, 
place_country_name VARCHAR(510) NULL, 
-- taxon
scientific_name VARCHAR(510) NULL,
common_name VARCHAR(510) NULL,
taxon_id VARCHAR(510) NULL,
-- created
loaded timestamp(6) default current_timestamp(6) NOT NULL, -- enough precision to generate unique delta primary keys
PRIMARY KEY (id, loaded) -- composite key ensures that deltas get loaded and not ignored due to duplicate primary key of id only
);

-- Obs < 2020-12-01
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fungi_northern_europe_1.csv' 
INTO TABLE fungi_northern_europe
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Obs > 2020-12-02 -- no need to handle dups
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fungi_northern_europe_2.csv' 
INTO TABLE fungi_northern_europe 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE fungi.fungi_northern_europe
ADD COLUMN region VARCHAR(100) AFTER id;

UPDATE fungi.fungi_northern_europe
SET region = 'Northern Europe';


-- Canada (region name in iNaturalist)

drop table if exists fungi.fungi_canada;

CREATE TABLE fungi.fungi_canada (
-- basic
id VARCHAR(100) NOT NULL, 
observed_on DATE NULL, 
user_id VARCHAR(510) NULL, 
user_login VARCHAR(510) NULL, 
created_at DATE NULL, 				
quality_grade VARCHAR(510) NULL, 
-- geo
latitude DECIMAL(14, 10), 
longitude DECIMAL(14, 10), 
place_state_name VARCHAR(510) NULL, 
place_country_name VARCHAR(510) NULL, 
-- taxon
scientific_name VARCHAR(510) NULL,
common_name VARCHAR(510) NULL,
taxon_id VARCHAR(510) NULL,
-- created
loaded timestamp(6) default current_timestamp(6) NOT NULL, -- enough precision to generate unique delta primary keys
PRIMARY KEY (id, loaded) -- composite key ensures that deltas get loaded and not ignored due to duplicate primary key of id only
);

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fungi_canada.csv' 
INTO TABLE fungi_canada 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE fungi.fungi_canada
ADD COLUMN region VARCHAR(100) AFTER id;

UPDATE fungi.fungi_canada
SET region = 'Canada';


-- New Zealand  (region name in iNaturalist)

drop table if exists fungi.fungi_new_zealand;

CREATE TABLE fungi.fungi_new_zealand (
-- basic
id VARCHAR(100) NOT NULL, 
observed_on DATE NULL, 
user_id VARCHAR(510) NULL, 
user_login VARCHAR(510) NULL, 
created_at DATE NULL, 				
quality_grade VARCHAR(510) NULL, 
-- geo
latitude DECIMAL(14, 10), 
longitude DECIMAL(14, 10), 
place_state_name VARCHAR(510) NULL, 
place_country_name VARCHAR(510) NULL, 
-- taxon
scientific_name VARCHAR(510) NULL,
common_name VARCHAR(510) NULL,
taxon_id VARCHAR(510) NULL,
-- created
loaded timestamp(6) default current_timestamp(6) NOT NULL, -- enough precision to generate unique delta primary keys
PRIMARY KEY (id, loaded) -- composite key ensures that deltas get loaded and not ignored due to duplicate primary key of id only
);

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fungi_new_zealand.csv' 
INTO TABLE fungi_new_zealand 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE fungi.fungi_new_zealand
ADD COLUMN region VARCHAR(100) AFTER id;

UPDATE fungi.fungi_new_zealand
SET region = 'New Zealand';


-- ####################################
-- Combined Overseas (OS) obs - Transforms and unions
-- update Hemisphere (season mapping and variable)
-- ####################################


drop table if exists fungi.os_obs;
CREATE TABLE IF NOT EXISTS fungi.os_obs AS
(

SELECT *,
case 
	when month(observed_on) = 6 then '4. Summer' 
	when month(observed_on) = 7 then '4. Summer' 
    when month(observed_on) = 8 then '4. Summer'
    when month(observed_on) = 9 then '1. Autumn'
    when month(observed_on) = 10 then '1. Autumn'
    when month(observed_on) = 11 then '1. Autumn'
    when month(observed_on) = 12 then '2. Winter'
    when month(observed_on) = 1 then '2. Winter'
    when month(observed_on) = 2 then '2. Winter'
    when month(observed_on) = 3 then '3. Spring'
    when month(observed_on) = 4 then '3. Spring'
    when month(observed_on) = 5 then '3. Spring'
	else '4. Summer'
    end as observed_season, -- Northern Hemisphere
'Northern' as hemisphere,
round(latitude,0) as latitude_reduced,
round(longitude,0) as longitude_reduced,
SUBSTRING_INDEX(scientific_name,' ',1) as genus_derived
FROM fungi.fungi_northern_europe
where observed_on <> '0000-00-00' and observed_on <> '0001-01-01' and observed_on is not null and scientific_name is not null

union ALL

SELECT *,
case 
	when month(observed_on) = 6 then '4. Summer' 
	when month(observed_on) = 7 then '4. Summer' 
    when month(observed_on) = 8 then '4. Summer'
    when month(observed_on) = 9 then '1. Autumn'
    when month(observed_on) = 10 then '1. Autumn'
    when month(observed_on) = 11 then '1. Autumn'
    when month(observed_on) = 12 then '2. Winter'
    when month(observed_on) = 1 then '2. Winter'
    when month(observed_on) = 2 then '2. Winter'
    when month(observed_on) = 3 then '3. Spring'
    when month(observed_on) = 4 then '3. Spring'
    when month(observed_on) = 5 then '3. Spring'
	else '4. Summer'
    end as observed_season, -- Northern Hemisphere
'Northern' as hemisphere,
round(latitude,0) as latitude_reduced,
round(longitude,0) as longitude_reduced,
SUBSTRING_INDEX(scientific_name,' ',1) as genus_derived
FROM fungi.fungi_east_asia
where observed_on <> '0000-00-00' and observed_on <> '0001-01-01' and observed_on is not null and scientific_name is not null

union ALL

SELECT *,
case 
	when month(observed_on) = 6 then '4. Summer' 
	when month(observed_on) = 7 then '4. Summer' 
    when month(observed_on) = 8 then '4. Summer'
    when month(observed_on) = 9 then '1. Autumn'
    when month(observed_on) = 10 then '1. Autumn'
    when month(observed_on) = 11 then '1. Autumn'
    when month(observed_on) = 12 then '2. Winter'
    when month(observed_on) = 1 then '2. Winter'
    when month(observed_on) = 2 then '2. Winter'
    when month(observed_on) = 3 then '3. Spring'
    when month(observed_on) = 4 then '3. Spring'
    when month(observed_on) = 5 then '3. Spring'
	else '4. Summer'
    end as observed_season, -- Northern Hemisphere
'Northern' as hemisphere,
round(latitude,0) as latitude_reduced,
round(longitude,0) as longitude_reduced,
SUBSTRING_INDEX(scientific_name,' ',1) as genus_derived
FROM fungi.fungi_canada
where observed_on <> '0000-00-00' and observed_on <> '0001-01-01' and observed_on is not null and scientific_name is not null

union ALL

SELECT *,
case 
	when month(observed_on) = 12 then '4. Summer' 
	when month(observed_on) = 1 then '4. Summer' 
    when month(observed_on) = 2 then '4. Summer'
    when month(observed_on) = 3 then '1. Autumn'
    when month(observed_on) = 4 then '1. Autumn'
    when month(observed_on) = 5 then '1. Autumn'
    when month(observed_on) = 6 then '2. Winter'
    when month(observed_on) = 7 then '2. Winter'
    when month(observed_on) = 8 then '2. Winter'
    when month(observed_on) = 9 then '3. Spring'
    when month(observed_on) = 10 then '3. Spring'
    when month(observed_on) = 11 then '3. Spring'
	else '4. Summer'
    end as observed_season, -- Southern Hemisphere
'Southern' as hemisphere,
round(latitude,0) as latitude_reduced,
round(longitude,0) as longitude_reduced,
SUBSTRING_INDEX(scientific_name,' ',1) as genus_derived
FROM fungi.fungi_new_zealand
where observed_on <> '0000-00-00' and observed_on <> '0001-01-01' and observed_on is not null and scientific_name is not null


-- ADD AUSTRALIA -- as different source, be sure to specify all fields

union ALL

SELECT

-- basic
id, 
'Australia' as region,
observed_on, 
user_id, 
user_login, 
created_at,			
quality_grade,
-- geo
latitude,
longitude,
place_state_name,
'Australia' as place_country_name,
-- taxon
scientific_name,
common_name,
taxon_id,
-- created
null as loaded,
case 
	when month(observed_on) = 12 then '4. Summer' 
	when month(observed_on) = 1 then '4. Summer' 
    when month(observed_on) = 2 then '4. Summer'
    when month(observed_on) = 3 then '1. Autumn'
    when month(observed_on) = 4 then '1. Autumn'
    when month(observed_on) = 5 then '1. Autumn'
    when month(observed_on) = 6 then '2. Winter'
    when month(observed_on) = 7 then '2. Winter'
    when month(observed_on) = 8 then '2. Winter'
    when month(observed_on) = 9 then '3. Spring'
    when month(observed_on) = 10 then '3. Spring'
    when month(observed_on) = 11 then '3. Spring'
	else '4. Summer'
    end as observed_season, -- Southern Hemisphere
'Southern' as hemisphere,
round(latitude,0) as latitude_reduced,
round(longitude,0) as longitude_reduced,
SUBSTRING_INDEX(scientific_name,' ',1) as genus_derived
FROM fungi.obs_final
where taxon_kingdom_name = 'Fungi' and taxon_species_name <> 'unknown' and observed_on <> '0000-00-00' and observed_on <> '0001-01-01' and observed_on is not null and scientific_name is not null

);


-- ####################################
-- Known Introduced species
-- from https://www.inaturalist.org/observations?introduced&place_id=6744&view=species&iconic_taxa=Fungi
-- ####################################


drop table if exists fungi.os_species_known;

CREATE TABLE fungi.os_species_known (
scientific_name VARCHAR(255) NOT NULL,
source VARCHAR(255) NOT NULL,
PRIMARY KEY (scientific_name) 
);

insert into fungi.os_species_known

values 
('Amanita muscaria','iNat flag'),
('Favolaschia claudopus','iNat flag'),
('Clathrus ruber','iNat flag'),
('Lactarius deliciosus','iNat flag'),
('Agaricus xanthodermus','iNat flag'),
('Suillus luteus','iNat flag'),
('Amanita phalloides','iNat flag'),
('Boletus edulis','iNat flag'),
('Puccinia myrsiphylli','iNat flag'),
('Amanita rubescens','iNat flag'),
('Usnea rubicunda','iNat flag'),
('Pleurotus ostreatus','iNat flag'),
('Thelephora terrestris','iNat flag')
;



-- ####################################
-- Overall summary stats
-- ####################################


drop table if exists fungi.os_species_summary;

CREATE TABLE fungi.os_species_summary as 

with 

#######################################################################

# distinct species by region

distinct_aus as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'Australia'
),

distinct_aus_rg as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'Australia' and quality_grade = 'Research'
),

-- can't get fully non-rg species with quality_grade = 'needs_id' only, so use different source
distinct_aus_non_rg as (
select
distinct scientific_name,
'Australia' as region
from fungi.rg_species_rank a
where rg_count = 0
),

distinct_canada as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'Canada' 
),

distinct_new_zealand as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'New Zealand' 
),

distinct_east_asia as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'East Asia' 
),

distinct_northern_europe as (
select
distinct scientific_name,
region
from fungi.os_obs a
where region = 'Northern Europe' 
),

#######################################################################

# in common RG

in_common_canada_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_rg a
inner join distinct_canada b on a.scientific_name = b.scientific_name
),

in_common_new_zealand_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_rg a
inner join distinct_new_zealand b on a.scientific_name = b.scientific_name
),

in_common_east_asia_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_rg a
inner join distinct_east_asia b on a.scientific_name = b.scientific_name
),

in_common_northern_europe_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_rg a
inner join distinct_northern_europe b on a.scientific_name = b.scientific_name
),

combiner_in_common_rg as (

select region, count(in_common) as in_common from in_common_canada_rg group by region
union all
select region, count(in_common) as in_common from in_common_new_zealand_rg group by region
union all
select region, count(in_common) as in_common from in_common_east_asia_rg group by region
union all
select region, count(in_common) as in_common from in_common_northern_europe_rg group by region

),

###################################################

# in common non_rg

in_common_canada_non_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_non_rg a
inner join distinct_canada b on a.scientific_name = b.scientific_name
),

in_common_new_zealand_non_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_non_rg a
inner join distinct_new_zealand b on a.scientific_name = b.scientific_name
),

in_common_east_asia_non_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_non_rg a
inner join distinct_east_asia b on a.scientific_name = b.scientific_name
),

in_common_northern_europe_non_rg as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus_non_rg a
inner join distinct_northern_europe b on a.scientific_name = b.scientific_name
),

combiner_in_common_non_rg as (

select region, count(in_common) as in_common from in_common_canada_non_rg group by region
union all
select region, count(in_common) as in_common from in_common_new_zealand_non_rg group by region
union all
select region, count(in_common) as in_common from in_common_east_asia_non_rg group by region
union all
select region, count(in_common) as in_common from in_common_northern_europe_non_rg group by region

),

###################################################

# in common all

in_common_canada as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus a
inner join distinct_canada b on a.scientific_name = b.scientific_name
),

in_common_new_zealand as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus a
inner join distinct_new_zealand b on a.scientific_name = b.scientific_name
),

in_common_east_asia as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus a
inner join distinct_east_asia b on a.scientific_name = b.scientific_name
),

in_common_northern_europe as (
select
distinct a.scientific_name as in_common,
b.region
from distinct_aus a
inner join distinct_northern_europe b on a.scientific_name = b.scientific_name
),

combiner_in_common as (

select region, count(in_common) as in_common from in_common_canada group by region
union all
select region, count(in_common) as in_common from in_common_new_zealand group by region
union all
select region, count(in_common) as in_common from in_common_east_asia group by region
union all
select region, count(in_common) as in_common from in_common_northern_europe group by region

)

select
a.region as Region,
count(distinct a.scientific_name) as 'Region RG Sp.',
concat(format(d.in_common,0), ' of ', format((select count(*) from distinct_aus),0)) as 'Sp. in common', -- format adds commas to the string for big number formatting
concat(format(b.in_common,0), ' of ', format((select count(*) from distinct_aus_rg),0)) as 'Sp. in common that have any RG obs in Aus',
concat(format(c.in_common,0), ' of ', format((select count(*) from distinct_aus_non_rg),0)) as 'Sp. in common without RG obs in Au'
from fungi.os_obs a
left join combiner_in_common_rg b on a.region = b.region
left join combiner_in_common_non_rg c on a.region = c.region
left join combiner_in_common d on a.region = d.region
where a.quality_grade = 'Research' and a.region <> 'Australia'
group by a.region
;

-- EOF
