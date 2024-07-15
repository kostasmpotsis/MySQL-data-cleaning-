#######remove duplicate ##########
select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

with duplicate_cte  as
(
select *,
row_number() over(
partition by company,
location,industry,total_laid_off,percentage_laid_off,
stage,country,funds_raised_millions,`date`) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;

select *
from layoffs_staging
where company='Oda';


select *
from layoffs_staging
where company='Casper';

with duplicate_cte  as
(
select *,
row_number() over(
partition by company,
location,industry,total_laid_off,percentage_laid_off,
stage,country,funds_raised_millions,`date`) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num>1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select *
from layoffs_staging2;


insert into layoffs_staging2
select *,
row_number() over(
partition by company,
location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
from layoffs_staging;


select *
from layoffs_staging2;

select  *
from layoffs_staging2
where row_num>1;

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_staging2
where row_num>1;

select *
from layoffs_staging2;

################standarizing data##########
###########################################
###########################################
select  company,trim(company)
from layoffs_staging2; 

update layoffs_staging2
set company=trim(company);


select  distinct industry 
from layoffs_staging2
order by 1;
 
 select *
 from layoffs_staging2
 where industry like 'crypto%';
 
 update layoffs_staging2
 set industry='crypto'
 where industry like 'crypto%';
 
 
 select *
 from layoffs_staging2;
 
 
 select distinct location 
 from layoffs_staging2
 order by 1;
 
 
 select distinct country 
 from layoffs_staging2
 order by 1;
 
 select *
 from layoffs_staging2
 where country like 'United States%'
 order by 1;
 
  select distinct country,trim(trailing '.' from country)
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country=trim(trailing '.' from country)
 where country like 'United states%';
 
 select *
 from layoffs_staging2;
 
 #change date from text to date 
 select `date`
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date`=str_to_date(`date`,'%m/%d/%Y');
 
 #now we can do the change 
 Alter table layoffs_staging2
 modify column `date` date;
 
 #drop the nulls 
 select *
 from layoffs_staging2
 where total_laid_off is NULL
 and percentage_laid_off is NULL ;
 
 select *
 from layoffs_staging2
 where industry is null 
 or  industry= '';
 
 select *
 from layoffs_staging2
 where company='Airbnb';
 
 select t1.industry,t2.industry
 from layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company=t2.company
 where (t1.industry is NULL  or t1.industry= '') 
 and t2.industry is not null;
 


update layoffs_staging2
set industry=NULL
where  industry='';

#set the t1.indrusty to t2.intrusty,where in the first we have 
#the informatioon and the second no so to be the same we set it 
#base on the same location 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry=t2.industry 
where t1.industry is NULL 
and t2.industry is not null;

##deleting the nulls 
select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

#remove duplicates 
#standardize data 
#null values or blank valies
#remove any columns or rows 