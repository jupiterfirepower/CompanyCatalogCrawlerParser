USE [tmp]
GO

/****** Object:  Table [dbo].[tblCountry]    Script Date: 1/17/2020 15:56:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


IF OBJECT_ID(N'dbo.tblCityCompany', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCityCompany];

IF OBJECT_ID(N'dbo.tblCompanyEmail', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCompanyEmail];

IF OBJECT_ID(N'dbo.tblCityCompany', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCityCompany];

IF OBJECT_ID(N'dbo.tblCountryRegion', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCountryRegion];

IF OBJECT_ID(N'dbo.tblRegionCity', N'U') IS NOT NULL
DROP TABLE [dbo].[tblRegionCity];

IF OBJECT_ID(N'dbo.tblCity', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCity];

IF OBJECT_ID(N'dbo.tblCompany', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCompany];

IF OBJECT_ID(N'dbo.tblCountry', N'U') IS NOT NULL
DROP TABLE [dbo].[tblCountry];

IF OBJECT_ID(N'dbo.tblEmail', N'U') IS NOT NULL
DROP TABLE [dbo].[tblEmail];

IF OBJECT_ID(N'dbo.tblIndustryCategory', N'U') IS NOT NULL
DROP TABLE [dbo].[tblIndustryCategory];

IF OBJECT_ID(N'dbo.tblRegion', N'U') IS NOT NULL
DROP TABLE [dbo].[tblRegion];

IF OBJECT_ID(N'dbo.tblEmailSendedReport', N'U') IS NOT NULL
DROP TABLE [dbo].[tblEmailSendedReport];

IF OBJECT_ID(N'dbo.tblCountry', N'U') IS NULL
BEGIN;

CREATE TABLE [dbo].[tblCountry](
	[CountryId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](250) NOT NULL,
	[FName] [nvarchar](250) NOT NULL,
	[Capital] [nvarchar](150) NULL,
	[Phone] [nvarchar](30) NULL,
	[Domain] [nvarchar](10) NULL,
	[Region] [nvarchar](100) NOT NULL,
 CONSTRAINT [PK_tblCountry] PRIMARY KEY CLUSTERED 
(
	[CountryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

END;

IF OBJECT_ID(N'dbo.tblCity', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCity](
	[CityId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](250) NOT NULL,
	[Subordination] [nvarchar](150) NOT NULL,
	[Code] [nvarchar](20) NOT NULL,
	[Region] [nvarchar](250) NOT NULL,
 CONSTRAINT [PK_tblCity] PRIMARY KEY CLUSTERED 
(
	[CityId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

END;

IF OBJECT_ID(N'dbo.tblIndustryCategory', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblIndustryCategory](
	[IndustryCategoryId] [int] IDENTITY(1,1) NOT NULL,
	[ParentIndustryCategoryId] [int] NULL,
	[Name] [nvarchar](150) NOT NULL,
	[Url] [nvarchar](250) NULL,
	[CompanyProcessed] [bit] NOT NULL,
	[CityCode] [nvarchar](20) NOT NULL,
 CONSTRAINT [PK_tblIndustryCategory] PRIMARY KEY CLUSTERED 
(
	[IndustryCategoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblIndustryCategory] ADD  CONSTRAINT [DF_tblIndustryCategory_Processed]  DEFAULT ((0)) FOR [CompanyProcessed]

ALTER TABLE [dbo].[tblIndustryCategory] ADD  CONSTRAINT [DF_tblIndustryCategory_CityCode]  DEFAULT ((77)) FOR [CityCode]

ALTER TABLE [dbo].[tblIndustryCategory]  WITH CHECK ADD  CONSTRAINT [FK_tblIndustryCategory_tblIndustryCategory] FOREIGN KEY([ParentIndustryCategoryId])
REFERENCES [dbo].[tblIndustryCategory] ([IndustryCategoryId])

ALTER TABLE [dbo].[tblIndustryCategory] CHECK CONSTRAINT [FK_tblIndustryCategory_tblIndustryCategory]

END;

IF OBJECT_ID(N'dbo.tblCompany', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCompany](
	[CompanyId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](max) NOT NULL,
	[Address] [nvarchar](max) NULL,
	[Phone] [nvarchar](max) NULL,
	[Timework] [nvarchar](max) NULL,
	[SiteUrl] [nvarchar](max) NULL,
	[IndustryCategoryId] [int] NULL,
	[EmailProcessed] [bit] NOT NULL,
	[BadUrl] [bit] NOT NULL,
	[EmailFinded] [bit] NOT NULL,
 CONSTRAINT [PK_tblCompany] PRIMARY KEY CLUSTERED 
(
	[CompanyId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

ALTER TABLE [dbo].[tblCompany] ADD  CONSTRAINT [DF_tblCompany_EmailProcessed]  DEFAULT ((0)) FOR [EmailProcessed]

ALTER TABLE [dbo].[tblCompany] ADD  CONSTRAINT [DF_tblCompany_BadUrl]  DEFAULT ((0)) FOR [BadUrl]

ALTER TABLE [dbo].[tblCompany] ADD  CONSTRAINT [DF_tblCompany_EmailFinded]  DEFAULT ((0)) FOR [EmailFinded]

ALTER TABLE [dbo].[tblCompany]  WITH CHECK ADD  CONSTRAINT [FK_tblCompany_tblIndustryCategory] FOREIGN KEY([IndustryCategoryId])
REFERENCES [dbo].[tblIndustryCategory] ([IndustryCategoryId])

ALTER TABLE [dbo].[tblCompany] CHECK CONSTRAINT [FK_tblCompany_tblIndustryCategory]

END;


IF OBJECT_ID(N'dbo.tblEmail', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblEmail](
	[EmailId] [int] IDENTITY(1,1) NOT NULL,
	[Email] [nvarchar](150) NOT NULL,
	[BadIgnoreEmail] [bit] NOT NULL,
 CONSTRAINT [PK_tblEmail] PRIMARY KEY CLUSTERED 
(
	[EmailId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblEmail] ADD  CONSTRAINT [DF_tblEmail_BadIgnoreEmail]  DEFAULT ((0)) FOR [BadIgnoreEmail]

END;

IF OBJECT_ID(N'dbo.tblCompanyEmail', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCompanyEmail](
	[CompanyId] [int] NOT NULL,
	[EmailId] [int] NOT NULL,
 CONSTRAINT [PK_tblCompanyEmail] PRIMARY KEY CLUSTERED 
(
	[CompanyId] ASC,
	[EmailId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblCompanyEmail]  WITH CHECK ADD  CONSTRAINT [FK_tblCompanyEmail_tblCompany] FOREIGN KEY([CompanyId])
REFERENCES [dbo].[tblCompany] ([CompanyId])

ALTER TABLE [dbo].[tblCompanyEmail] CHECK CONSTRAINT [FK_tblCompanyEmail_tblCompany]

ALTER TABLE [dbo].[tblCompanyEmail]  WITH CHECK ADD  CONSTRAINT [FK_tblCompanyEmail_tblEmail] FOREIGN KEY([EmailId])
REFERENCES [dbo].[tblEmail] ([EmailId])

ALTER TABLE [dbo].[tblCompanyEmail] CHECK CONSTRAINT [FK_tblCompanyEmail_tblEmail]

END;


IF OBJECT_ID(N'dbo.tblCountry', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCountry](
	[CountryId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](250) NOT NULL,
	[FName] [nvarchar](250) NOT NULL,
	[Capital] [nvarchar](150) NULL,
	[Phone] [nvarchar](30) NULL,
	[Domain] [nvarchar](10) NULL,
	[Region] [nvarchar](100) NOT NULL,
 CONSTRAINT [PK_tblCountry] PRIMARY KEY CLUSTERED 
(
	[CountryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

END;


IF OBJECT_ID(N'dbo.tblRegion', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblRegion](
	[RegionId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](250) NOT NULL,
	[Code] [nvarchar](20) NOT NULL,
 CONSTRAINT [PK_tblRegion] PRIMARY KEY CLUSTERED 
(
	[RegionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

END;

IF OBJECT_ID(N'dbo.tblCompanyEmail', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCompanyEmail](
	[CompanyId] [int] NOT NULL,
	[EmailId] [int] NOT NULL,
 CONSTRAINT [PK_tblCompanyEmail] PRIMARY KEY CLUSTERED 
(
	[CompanyId] ASC,
	[EmailId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblCompanyEmail]  WITH CHECK ADD  CONSTRAINT [FK_tblCompanyEmail_tblCompany] FOREIGN KEY([CompanyId])
REFERENCES [dbo].[tblCompany] ([CompanyId])

ALTER TABLE [dbo].[tblCompanyEmail] CHECK CONSTRAINT [FK_tblCompanyEmail_tblCompany]

ALTER TABLE [dbo].[tblCompanyEmail]  WITH CHECK ADD  CONSTRAINT [FK_tblCompanyEmail_tblEmail] FOREIGN KEY([EmailId])
REFERENCES [dbo].[tblEmail] ([EmailId])

ALTER TABLE [dbo].[tblCompanyEmail] CHECK CONSTRAINT [FK_tblCompanyEmail_tblEmail]

END;

IF OBJECT_ID(N'dbo.tblCountryRegion', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCountryRegion](
	[CountryId] [int] NOT NULL,
	[RegionId] [int] NOT NULL,
 CONSTRAINT [PK_tblCountryRegion] PRIMARY KEY CLUSTERED 
(
	[CountryId] ASC,
	[RegionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblCountryRegion]  WITH CHECK ADD  CONSTRAINT [FK_tblCountryRegion_tblCountry] FOREIGN KEY([CountryId])
REFERENCES [dbo].[tblCountry] ([CountryId])

ALTER TABLE [dbo].[tblCountryRegion] CHECK CONSTRAINT [FK_tblCountryRegion_tblCountry]

ALTER TABLE [dbo].[tblCountryRegion]  WITH CHECK ADD  CONSTRAINT [FK_tblCountryRegion_tblRegion] FOREIGN KEY([RegionId])
REFERENCES [dbo].[tblRegion] ([RegionId])

ALTER TABLE [dbo].[tblCountryRegion] CHECK CONSTRAINT [FK_tblCountryRegion_tblRegion]

END;

IF OBJECT_ID(N'dbo.tblRegionCity', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblRegionCity](
	[RegionId] [int] NOT NULL,
	[CityId] [int] NOT NULL,
 CONSTRAINT [PK_tblRegionCity] PRIMARY KEY CLUSTERED 
(
	[RegionId] ASC,
	[CityId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblRegionCity]  WITH CHECK ADD  CONSTRAINT [FK_tblRegionCity_tblCity] FOREIGN KEY([CityId])
REFERENCES [dbo].[tblCity] ([CityId])

ALTER TABLE [dbo].[tblRegionCity] CHECK CONSTRAINT [FK_tblRegionCity_tblCity]

ALTER TABLE [dbo].[tblRegionCity]  WITH CHECK ADD  CONSTRAINT [FK_tblRegionCity_tblRegion] FOREIGN KEY([RegionId])
REFERENCES [dbo].[tblRegion] ([RegionId])

ALTER TABLE [dbo].[tblRegionCity] CHECK CONSTRAINT [FK_tblRegionCity_tblRegion]

END;

IF OBJECT_ID(N'dbo.tblCityCompany', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblCityCompany](
	[CityCompanyId] [int] IDENTITY(1,1) NOT NULL,
	[CityId] [int] NOT NULL,
	[CompanyId] [int] NOT NULL,
 CONSTRAINT [PK_tblCityCompany] PRIMARY KEY CLUSTERED 
(
	[CityCompanyId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[tblCityCompany]  WITH CHECK ADD  CONSTRAINT [FK_tblCityCompany_tblCity] FOREIGN KEY([CityId])
REFERENCES [dbo].[tblCity] ([CityId])

ALTER TABLE [dbo].[tblCityCompany] CHECK CONSTRAINT [FK_tblCityCompany_tblCity]

ALTER TABLE [dbo].[tblCityCompany]  WITH CHECK ADD  CONSTRAINT [FK_tblCityCompany_tblCompany] FOREIGN KEY([CompanyId])
REFERENCES [dbo].[tblCompany] ([CompanyId])

ALTER TABLE [dbo].[tblCityCompany] CHECK CONSTRAINT [FK_tblCityCompany_tblCompany]

END;

IF OBJECT_ID(N'dbo.tblEmailSendedReport', N'U') IS NULL
 
BEGIN;

CREATE TABLE [dbo].[tblEmailSendedReport](
	[EmailFileName] [nvarchar](260) NOT NULL,
	[LastEmailIdSended] [int] NOT NULL,
 CONSTRAINT [PK_tblEmailSendedReport] PRIMARY KEY CLUSTERED 
(
	[EmailFileName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

END;






