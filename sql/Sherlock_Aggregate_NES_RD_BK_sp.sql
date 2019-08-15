USE [Radar]
GO
/****** Object:  StoredProcedure [dbo].[Sherlock_Aggregate_RemDeletes_sp]    Script Date: 1/4/2019 10:28:30 AM ******/
SET ANSI_NULLS ON
GO
SET ANSI_WARNINGS OFF;
GO
SET QUOTED_IDENTIFIER ON
GO
/***********************************************************************************************************************************************
-- Description : Aggregated fields from the following tables:
-- Author : Richard Senar
-- Tables : BKFS_Ingestion.dbo.DataFiles | RTDomain.dbo.DimJurisdiction | RTDomain.dbo.ParcelDomain
-- 			BKFS_BackStage.dbo.AssessmentBackStageRaw | DQuick.dbo.Assessor
-- Date        Initials	Description
-- ----------  --------	--------------------------------
-- 08/22/2018	RS			initial create.
*****************************************************************************************************************************************************/
ALTER PROCEDURE [dbo].[Sherlock_Aggregate_NES_RemDeletes_BK_sp]
@FIPS char(5) = NULL
AS 
BEGIN 
	--------------------------------------------------------------------------
	--				 Load Sherlock Black Knight Aggregate tables
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#Sherlock_DFIDEdition') IS NOT NULL 
		DROP TABLE #Sherlock_DFIDEdition;
	SELECT DataFileID
			,FIPS
			,State
			,County
	   	,Edition
	INTO #Sherlock_DFIDEdition
	FROM BKFS_Ingestion.dbo.DataFiles WITH(NOLOCK)
	WHERE Edition IN (
		SELECT MAX(Edition)
		FROM 	BKFS_Ingestion.dbo.DataFiles WITH(NOLOCK)
		WHERE FIPS IN (@FIPS)
	)
	AND FIPS IN (@FIPS)
	AND FileName LIKE '%assessment_refresh%'
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#BKaggregate_Stagging') IS NOT NULL 
		DROP TABLE #BKaggregate_Stagging;
	SELECT	js.DataFileID
			,js.FIPS
			,js.State
			,js.County
			,dj.JurisdictionKey
			,dj.ScmId
			,(CASE WHEN abr.DUP_APN IS NOT NULL THEN SUBSTRING(abr.APN, 1, LEN(abr.APN)-3) ELSE SUBSTRING(abr.APN, 6, LEN(abr.APN)) END) AS APN
			,abr.TAX_ACCT_NBR
			,abr.LEGAL_FULL_DSCRP
			,(CASE WHEN len(abr.OLD_APN) >= 6 THEN SUBSTRING(abr.OLD_APN, 6, LEN(abr.OLD_APN)) ELSE abr.OLD_APN END) AS OLD_APN
			,abr.DPID
			,abr.DUP_APN
			,abr.PROP_HOUSE_NBR
			,abr.PROP_STREET_NAME
			,abr.PROP_CITY
			,abr.PROP_STATE
			,abr.PROP_ZIP
			,abr.PROP_UNIT_NBR
			,abr.OWNER_NAME
			,abr.LEGAL_CITY_TOWN_MUNI
            ,je.RT_MuniName
			,(CASE WHEN je.RT_MuniName IS NOT NULL THEN je.RT_MuniName ELSE abr.LEGAL_CITY_TOWN_MUNI END) AS MUNI
			,ROW_NUMBER() OVER (PARTITION BY DPID ORDER BY DPID ASC) AS RowNum
	INTO 	#BKaggregate_Stagging
	FROM	BKFS_BackStage.dbo.AssessmentBackStageRaw AS abr
			INNER JOIN #Sherlock_DFIDEdition AS js
				ON js.DataFileID = abr.DataFileID
            LEFT JOIN BKFS_Ingestion.dbo.JurisdictionException AS je
                ON abr.LEGAL_CITY_TOWN_MUNI = je.BK_MuniName
			LEFT JOIN RTDomain.dbo.DimJurisdiction AS dj
				ON dj.MuniName = (CASE WHEN je.RT_MuniName IS NOT NULL THEN je.RT_MuniName ELSE abr.LEGAL_CITY_TOWN_MUNI END)
	WHERE 	abr.DataFileID IN (
				SELECT DataFileID FROM #Sherlock_DFIDEdition
			)
	AND		abr.DUP_APN IS NULL OR abr.DUP_APN = '1'
	--------------------------------------------------------------------------
	INSERT INTO	Radar.dbo.Sherlock_BKaggregate
	SELECT	DataFileID
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
	FROM	#BKaggregate_Stagging WITH(NOLOCK)
	WHERE	RowNum = 1

END
GO
