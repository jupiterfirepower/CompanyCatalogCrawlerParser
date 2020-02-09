/****** Script for SelectTopNRows command from SSMS  ******/
SELECT Distinct [SiteUrl],
       [CompanyId]
      ,[Name]
      ,[Address]
      ,[Phone]
      ,[Timework]
      
      ,[IndustryCategoryId]
      ,[EmailProcessed]
      ,[BadUrl]
      ,[EmailFinded]
  FROM [CountryDictionary].[dbo].[tblCompany]
  WHERE [EmailProcessed]=0 AND [BadUrl]=0 AND [EmailFinded]=0 AND [SiteUrl] IS NOT NULL

SELECT Distinct [SiteUrl]
       
  FROM [CountryDictionary].[dbo].[tblCompany]
  WHERE [EmailProcessed]=0 AND [BadUrl]=0 AND [EmailFinded]=0 AND [SiteUrl] IS NOT NULL

SELECT  *
FROM    (SELECT [SiteUrl],
       [CompanyId]
      ,[Name]
      ,[Address]
      ,[Phone]
      ,[Timework]
      
      ,[IndustryCategoryId]
      ,[EmailProcessed]
      ,[BadUrl]
      ,[EmailFinded],
                ROW_NUMBER() OVER (PARTITION BY SiteUrl ORDER BY [CompanyId]) AS RowNumber
         FROM   [CountryDictionary].[dbo].[tblCompany]
         WHERE [EmailProcessed]=0 AND [BadUrl]=0 AND [EmailFinded]=0 AND [SiteUrl] IS NOT NULL) AS a
WHERE   a.RowNumber = 1

DELETE
  FROM [CountryDictionary].[dbo].[tblCompanyEmail]
  WHERE [EmailId] IN 
  (SELECT  [EmailId]
  FROM [CountryDictionary].[dbo].[tblEmail]
  WHERE EMAIL LIKE '%.js' or EMAIL LIKE '%.png' or EMAIL LIKE '%.jpg' or EMAIL LIKE '%.jpeg' or EMAIL LIKE '%.webp' or EMAIL LIKE '%.html'
  or EMAIL LIKE '%subject%' 
  or EMAIL LIKE '%.i8uEU'
  or EMAIL LIKE '%1'
  or EMAIL LIKE '%2'
  or EMAIL LIKE '%3'
  or EMAIL LIKE '%4'
  or EMAIL LIKE '%5'
  or EMAIL LIKE '%6'
  or EMAIL LIKE '%7'
  or EMAIL LIKE '%8'
  or EMAIL LIKE '%9'
  or EMAIL LIKE '%0'
  or LEN(EMAIL) <= 1
  or EMAIL LIKE '@%'
  )


  DELETE
  FROM [CountryDictionary].[dbo].[tblEmail]
  WHERE EMAIL LIKE '%.js' or EMAIL LIKE '%.png' or EMAIL LIKE '%.jpg' or EMAIL LIKE '%.jpeg' or EMAIL LIKE '%.webp' or EMAIL LIKE '%.html'
  or EMAIL LIKE '%subject%' 
  or EMAIL LIKE '%.i8uEU'
  or EMAIL LIKE '%1'
  or EMAIL LIKE '%2'
  or EMAIL LIKE '%3'
  or EMAIL LIKE '%4'
  or EMAIL LIKE '%5'
  or EMAIL LIKE '%6'
  or EMAIL LIKE '%7'
  or EMAIL LIKE '%8'
  or EMAIL LIKE '%9'
  or EMAIL LIKE '%0'
  or LEN(EMAIL) <= 1
  or EMAIL LIKE '@%'

SELECT TOP (1000) [CountryId]
      ,[Name]
      ,[FName]
      ,[Capital]
      ,[Phone]
      ,[Domain]
      ,[Region]
  FROM [CountryDictionary].[dbo].[tblCountry]
  WHERE [Name]='Россия'

SELECT TOP (1000) [CityId]
      ,[Name]
      ,[Subordination]
      ,[Code]
      ,[Region]
  FROM [CountryDictionary].[dbo].[tblCity]
  WHERE [Name] ='Москва'

INSERT INTO tblCountryRegion (CountryId, RegionId)
SELECT 874, [RegionId]
FROM [CountryDictionary].[dbo].[tblRegion]

INSERT INTO tblCityCompany (CityId, CompanyId)
SELECT 865, [CompanyId]
FROM [CountryDictionary].[dbo].tblCompany