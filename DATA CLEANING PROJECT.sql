-- DATA CLEANING

SELECT * FROM layoffs;
# We are going to work in stages and that i why we always make a copy of dataset and then work on it so that the 
# raw data is kept as it is, Therefore we are creating another table named as layoffs_staging
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

#Inserting values into layoffs_staging
INSERT layoffs_staging
SELECT * FROM 
layoffs;

#Assigning unique id to every row for identification purposes
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging;

#Selecting for duplicate values
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,
 `date`, stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num >=2;

#Re-Checking whether the duplicates are real or not
SELECT * 
FROM layoffs_staging
WHERE company in ('Casper','Cazoo','Hibob','Wildlife Studios','Yahoo')
ORDER BY company;

#We cannot directly delete the duplicate in mysql there for we will be creating a new table named layoffs_staging2
#from the layoffs_stating
#We are using the following way to create the table because we want to add new column row_num in the table
#which is not present in the dataset
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

#Inserting the values in the tables with row numbers from the previous duplicate_cte
INSERT layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,
 `date`, stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE  
FROM layoffs_staging2
WHERE row_num>1;

SELECT * 
FROM layoffs_staging2 WHERE row_num>1;

SELECT * 
FROM layoffs_staging2;

#Standardize the DATA 

#Checking whitespaces 
#After checking for leading and trailing whitespaces we have checked for any whitespaces in the column 
SELECT 
CASE 
 WHEN country LIKE '% %' THEN country 
 END as whitespace,
 CASE 
 WHEN location LIKE '% %' THEN location
 END as whitespace_loc,
 CASE 
 WHEN industry LIKE '% %' THEN industry
 END as whitespace_ind,
 CASE 
 WHEN stage LIKE '% %' THEN stage
 END as whitespace_stg
FROM layoffs_staging2;

#Checking for issues in industry column
Select DISTINCT industry 
from layoffs_staging2
ORDER BY 1;

#We found that three industry having different name but are same that is Crypto, Cryptocurrency, Crypto currency
Select *
from layoffs_staging2
WHERE industry LIKE 'Crypto%';

#We are giving the industry a common name that is "Crypto"
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

#We need to check each to of the column one by one to check if there is any issue
Select DISTINCT country
from layoffs_staging2
ORDER BY 1;

#We have detected "United States" & "United States." as two seperate countries, so we need to fix this
Select DISTINCT country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2
ORDER BY 1;

#Fixing the issue
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

##Also the date column is of text type, we need to convert it to date type

SELECT `date`,STR_TO_DATE(`date`,'%m/%d/%Y') as new_date
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`,'%m/%d/%Y'); 

##Converted to date type but the data type is still text format, so in order to changeit we will use ALTER command
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off FLOAT8;

#DEALING WITH NULL AND MISSING VALUES

-- Checking for null or blank values in the industry column
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry='';

#We found some blank values in industry column so we will try to populate them with respect to it's company and location
SELECT * 
FROM layoffs_staging2
WHERE company="Airbnb";

SELECT 
    t1.company, t1.location,t1.industry, t2.company, t2.location,t2.industry
FROM
    layoffs_staging2 AS t1
        JOIN
    layoffs_staging2 AS t2 ON t1.company = t2.company
        AND t1.location = t2.location
WHERE
    (t1.industry = '' OR t1.industry IS NULL)
    AND t2.industry IS NOT NULL;

#We found the relation on how to update industry so we are going to update it.

-- We are going to set the Blank values to null first and then change the null values to its corresponding value that we have analyzed
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2 
		ON t1.company = t2.company
        AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE
    t1.industry IS NULL
    AND t2.industry IS NOT NULL;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

#We are done with the industry now we will check for percentage and total, if they both are null so they are of no use for us.
-- We could've populated them if the company's total employee count -: before and after layoff was given by doing calculations
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting the data
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

#Now column row_num is of no use for us, we used for data cleaning purpose therefor it is good to delete uneccessary columns

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


















