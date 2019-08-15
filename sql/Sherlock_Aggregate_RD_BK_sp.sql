USE [Radar]
GO
/****** Object:  StoredProcedure [dbo].[Sherlock_Aggregate_RemDeletes_sp]    Script Date: 1/4/2019 10:28:30 AM ******/
SET ANSI_NULLS ON
GO
SET ANSI_WARNINGS OFF
GO
SET QUOTED_IDENTIFIER ON
GO
/***********************************************************************************************************************************************
-- Description : Aggregated fields from the following tables:
-- 					* BKFS_Ingestion.dbo.DataFiles
-- 					* RTDomain.dbo.DimJurisdiction
-- 					* RTDomain.dbo.ParcelDomain
-- 					* BKFS_BackStage.dbo.AssessmentBackStageRaw
-- 					* DQuick.dbo.Assessor
-- 				Data is then loaded into the the following tables:
-- 					* Radar.dbo.Sherlock_BKaggregate
-- 					* Radar.dbo.Sherlock_CLaggregate
-- 				for matching by Sherlock
-- Output : Radar.dbo.Sherlock_BKaggregate | Radar.dbo.Sherlock_CLaggregate
-- Author : Richard Senar
-- Tables : BKFS_Ingestion.dbo.DataFiles | RTDomain.dbo.DimJurisdiction | RTDomain.dbo.ParcelDomain
-- 			  BKFS_BackStage.dbo.AssessmentBackStageRaw | DQuick.dbo.Assessor
-- Date        Initials	Description
-- ----------  --------	--------------------------------
-- 08/22/2018	RS			initial create.
*****************************************************************************************************************************************************/
ALTER PROCEDURE [dbo].[Sherlock_Aggregate_RemDeletes_BK_sp]
@FIPS char(5) = NULL
AS 
BEGIN 
	--------------------------------------------------------------------------
	--				 Load Sherlock Black Knight Aggregate tables
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#Sherlock_DFID_BK') IS NOT NULL 
		DROP TABLE #Sherlock_DFID_BK;
	SELECT DataFileID
			,FIPS
			,State
			,County
	INTO #Sherlock_DFID_BK
	FROM BKFS_Ingestion.dbo.DataFiles WITH(NOLOCK)
	WHERE DataFileID IN (
		SELECT MIN(DataFileID)
		FROM BKFS_Ingestion.dbo.DataFiles
		WHERE fips IN (@FIPS)
		AND FileName LIKE '%assessment_refresh%'
		AND Edition IN (
		    SELECT MAX(Edition)
		    FROM BKFS_Ingestion.dbo.DataFiles
		    WHERE FIPS IN (@FIPS)
		    AND FileName LIKE '%assessment_refresh%'
		)
	)
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#Sherlock_JurSCM_BK') IS NOT NULL 
		DROP TABLE #Sherlock_JurSCM_BK;
	SELECT dfid.DataFileID
			,dfid.FIPS
			,dfid.County
			,dfid.State
			,dj.JurisdictionKey
			,dj.ScmId
	INTO #Sherlock_JurSCM_BK
	FROM #Sherlock_DFID_BK AS dfid
		INNER JOIN RTDomain.dbo.DimJurisdiction AS dj
			ON dj.FipsStateCode = CAST((SUBSTRING(dfid.FIPS, 1, 2)) AS INT)
			AND dj.FipsMuniCode = CAST((SUBSTRING(dfid.FIPS, 3, 5)) AS INT)
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#BKaggregate_Stagging_BK') IS NOT NULL 
		DROP TABLE #BKaggregate_Stagging_BK;
	SELECT js.DataFileID
			,js.FIPS
			,js.State
			,js.County
			,js.JurisdictionKey
			,js.ScmId
			,(CASE WHEN NULLIF(abr.DUP_APN,'') IS NOT NULL THEN LEFT(abr.APN, LEN(abr.APN) - (LEN(abr.DUP_APN)+1)) ELSE abr.APN END) AS APN
			,abr.TAX_ACCT_NBR
			,abr.LEGAL_FULL_DSCRP
			,abr.OLD_APN
			,abr.DPID
			,abr.DUP_APN
			,abr.PROP_HOUSE_NBR
			,abr.PROP_STREET_NAME
			,abr.PROP_CITY
			,abr.PROP_STATE
			,abr.PROP_ZIP
			,abr.PROP_UNIT_NBR
			,abr.OWNER_NAME
			,ROW_NUMBER() OVER (PARTITION BY DPID ORDER BY DPID ASC) AS RowNum
	INTO #BKaggregate_Stagging_BK
	FROM #Sherlock_JurSCM_BK AS js
			LEFT JOIN BKFS_BackStage.dbo.AssessmentBackStageRaw AS abr
				ON js.DataFileID = abr.DataFileID
	WHERE	abr.DUP_APN IS NULL
	OR 	abr.DUP_APN = '1'
	--------------------------------------------------------------------------
	INSERT INTO	Radar.dbo.Sherlock_BKaggregate
	SELECT DataFileID
			,FIPS
			,State
			,County
			,JurisdictionKey
			,ScmId
			,APN
			,TAX_ACCT_NBR
			,LEGAL_FULL_DSCRP
			,OLD_APN
			,DPID
			,DUP_APN
			,PROP_HOUSE_NBR
			,PROP_STREET_NAME
			,PROP_CITY
			,PROP_STATE
			,PROP_ZIP
			,PROP_UNIT_NBR
			,OWNER_NAME
	FROM #BKaggregate_Stagging_BK
	WHERE RowNum = 1

END
GO

/*

	Alternate Ingest for DT_708_ForeclosureShell
	INSERT INTO Radar.dbo.Sherlock_BKaggregate
	(
	   DPID
	   ,FIPS
	   ,APN
	   ,OLD_APN
	   ,TAX_ACCT_NBR
	   ,PROP_HOUSE_NBR
	   ,PROP_STREET_NAME
	   ,PROP_CITY
	   ,PROP_STATE
	   ,PROP_UNIT_NBR
	)
	SELECT RePropertyID
	      ,FIPS
	      ,APN
	      ,OLD_APN
	      ,TAX_ACCT_NBR
	      ,PROP_HOUSE_NBR
	      ,PROP_STREET_NAME
	      ,PROP_CITY
	      ,PROP_STATE
	      ,PROP_UNIT_NBR
	FROM Radar.dbo.DT_708_ForeclosureShell
	WHERE FIPS IN (@FIPS)




IF OBJECT_ID('tempdb..#Sherlock_BKaggregateTMP') IS NOT NULL 
	DROP TABLE #Sherlock_BKaggregateTMP;
SELECT [DataFileID]
      ,[FIPS]
      ,[State]
      ,[County]
      ,[JurisdictionKey]
      ,[ScmId]
      ,[APN]
      ,[TAX_ACCT_NBR]
      ,[LEGAL_FULL_DSCRP]
      ,[OLD_APN]
      ,[DPID]
      ,[DUP_APN]
      ,[PROP_HOUSE_NBR]
      ,[PROP_STREET_NAME]
      ,[PROP_CITY]
      ,[PROP_STATE]
      ,[PROP_ZIP]
      ,[PROP_UNIT_NBR]
      ,[OWNER_NAME]
		,ROW_NUMBER() OVER (PARTITION BY DPID ORDER BY DPID ASC) AS RowNum
INTO #Sherlock_BKaggregateTMP
FROM [Radar].[dbo].[Sherlock_BKaggregate]

SELECT [DataFileID]
      ,[FIPS]
      ,[State]
      ,[County]
      ,[JurisdictionKey]
      ,[ScmId]
      ,[APN]
      ,[TAX_ACCT_NBR]
      ,[LEGAL_FULL_DSCRP]
      ,[OLD_APN]
      ,[DPID]
      ,[DUP_APN]
      ,[PROP_HOUSE_NBR]
      ,[PROP_STREET_NAME]
      ,[PROP_CITY]
      ,[PROP_STATE]
      ,[PROP_ZIP]
      ,[PROP_UNIT_NBR]
      ,[OWNER_NAME]
INTO [Radar].[dbo].[Sherlock_BKaggregate2]
FROM #Sherlock_BKaggregateTMP
WHERE RowNum = 1

*/