#EXPLORATORY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;

#Checking for maximum layoff
SELECT MAX(total_laId_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-- Checking fir comppanies with maximum funding and 100% layoff
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Checking for total layoff by each company
SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Checking for total layoff by each industry
SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

#Checking for yearly trend
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

#Monthly Trend
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
where SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ;

#Checking layoff by country
SELECT country,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

#Now we are going to do the rolling total over months 
#A rolling sum, also known as a moving sum or running total, 
#is a calculation used to sum a specific number of sequential elements in a dataset

WITH Rolling_total AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
FROM layoffs_staging2
where SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC)
SELECT `MONTH`,total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_total;

#Checking the layoff by each company every year

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 1 ASC;

#Now we will look at per year layoff of top 5 companies and industries 

WITH Company_year (company,industry, years,total_laid_off) AS
(
SELECT company,industry,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,industry,YEAR(`date`)
), Company_ranking AS
(SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_year
WHERE years IS NOT NULL)
SELECT *
FROM Company_ranking
WHERE ranking <= 5
;