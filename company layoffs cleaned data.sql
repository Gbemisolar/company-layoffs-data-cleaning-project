-- data cleaning

-- 1. Remove duplicates
-- 2. standardize it
-- 3. null values or blank values
-- 4. remove unneeded columns from the table copy, not the original


-- creating a copy of the table
create table layoffs_staging
like layoffs;
-- inserting the data in the table into the copy
insert layoffs_staging
select *
from layoffs;
select *
from layoffs_staging

-- checking for duplicates in all the columns
select *,
row_number () over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

with duplicate_cte as (
select *,
row_number () over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging);
select * from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Casper';

with duplicate_cte as
(
select *,
row_number () over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date'
, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)

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
row_number () over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date'
, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

SET sql_safe_updates = 0;
delete 
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

-- standardizing data
select company, (trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
where country like 'united states%'
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';


select `date`,
str_to_date (`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null )
and t2.industry is not null;

set sql_safe_updates = 0;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

-- deleting irrelevant data
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

