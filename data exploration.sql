select *
from layoffs_staging2;

select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc;

select company,sum(total_laid_off)
from layoffs_staging2
group by company 
order by  2 desc;

select min(`date`),max(`date`)
from layoffs_staging2;

select industry,sum(total_laid_off)
from layoffs_staging2
group by industry
order by  2 desc;


select country,sum(total_laid_off)
from layoffs_staging2
group by country
order by  2 desc;


select YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by  2 desc;

select stage,sum(total_laid_off)
from layoffs_staging2
group by stage
order by  2 desc;


select company,sum(percentage_laid_off)
from layoffs_staging2
group by company
order by  2 desc;

select substring(`date`,6,2) as `month`,sum(total_laid_off)
from layoffs_staging2
group by `month`;

select substring(`date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by  1 asc ;

with rolling_total as
(
select substring(`date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by  1 asc 
)
select `month`,sum(total_off) over(order by `month`) as rolling_total
from rolling_total;


select company,YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 2 desc;

select company,YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 3 desc;

with company_year(company,years,total_laid_off) as 
(
select company,YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
),company_year_rank as 
(
select *,
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
order by ranking asc
)
select * 
from company_year_rank
where ranking>=5;



