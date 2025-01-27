-- Data Cleaning Project

-- Setup DB

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Remove Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Yahoo';

CREATE TABLE `layoffs_staging_two` (
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
FROM layoffs_staging_two;

INSERT INTO layoffs_staging_two
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging_two
WHERE row_num > 1;

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging_two;

UPDATE layoffs_staging_two
SET company = TRIM(company);

SELECT *
FROM layoffs_staging_two
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging_two
SET industry = 'Cryptocurrency'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging_two
ORDER BY 1;

SELECT *
FROM layoffs_staging_two
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_two
ORDER BY 1;

UPDATE layoffs_staging_two
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging_two;

UPDATE layoffs_staging_two
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging_two
MODIFY COLUMN `date` DATE;

-- Removing Null Value

SELECT *
FROM layoffs_staging_two
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging_two
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging_two
WHERE company LIKE 'Bally%';

UPDATE layoffs_staging_two
SET industry = NULL
WHERE industry = '';

SELECT tbl_1.industry, tbl_2.industry
FROM layoffs_staging_two AS tbl_1
JOIN layoffs_staging_two AS tbl_2
	ON tbl_1.company = tbl_2.company
WHERE (tbl_1.industry IS NULL OR tbl_1.industry = '')
AND tbl_2.industry IS NOT NULL;

UPDATE layoffs_staging_two AS t1
JOIN layoffs_staging_two AS t2
	ON t1.company = t2.company
SET t1.industry = t2. industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging_two;

DELETE
FROM layoffs_staging_two
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging_two
DROP COLUMN row_num;

-- Exploratory Data Analysis

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging_two;

SELECT *
FROM layoffs_staging_two
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging_two
GROUP BY company
ORDER BY 2 DESC;

SELECT company, MIN(`date`), MAX(`date`), SUM(total_laid_off)
FROM layoffs_staging_two
GROUP BY company
ORDER BY 4 DESC;

SELECT industry, SUM(total_laid_off), SUM(percentage_laid_off)
FROM layoffs_staging_two
GROUP BY industry
ORDER BY 2 DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging_two
GROUP BY company
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_two
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT MONTH(`date`), SUM(total_laid_off)
FROM layoffs_staging_two
WHERE MONTH(`date`) IS NOT NULL
GROUP BY MONTH(`date`)
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
	SELECT SUBSTRING(`date`, 1, 7) AS yearmonth, SUM(total_laid_off) AS total_laid
	FROM layoffs_staging_two
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY yearmonth
	ORDER BY 1 ASC
)
SELECT yearmonth, total_laid, SUM(total_laid) OVER(ORDER BY yearmonth) AS rolling_total
FROM Rolling_Total;

WITH Rolling_Total_company(company, years, total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging_two
	GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Rolling_Total_company
	WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;




