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
ALTER PROCEDURE [dbo].[Sherlock_Aggregate_RemDeletes_CL_sp]
@FIPS char(5) = NULL
AS 
BEGIN 
	--------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#CLaggregate_Stagging') IS NOT NULL 
		DROP TABLE #CLaggregate_Stagging;
    SELECT  dq.SA_PROPERTY_ID
			,@FIPS AS FIPS
			,dj.JurisdictionKey
			,dq.SA_PARCEL_NBR_PRIMARY
			,dq.SA_PARCEL_NBR_PREVIOUS
			,dq.SA_PARCEL_NBR_ALT
			,dq.SA_PARCEL_ACCOUNT_NBR
			,dq.SA_PARCEL_NBR_REFERENCE
			,dq.SA_SITE_HOUSE_NBR
			,dq.SA_SITE_STREET_NAME
			,dq.SA_SITE_CITY_STATE
			,dq.SA_SITE_ZIP
			,dq.SA_SITE_UNIT_VAL
			,dq.SA_OWNER_1
			,dq.SA_LGL_DSCRPTN
			,ROW_NUMBER() OVER (PARTITION BY dq.SA_PROPERTY_ID ORDER BY dq.SA_PROPERTY_ID ASC) AS RowNumSAPID
	INTO	#CLaggregate_Stagging
   FROM	DQuick.dbo.Assessor AS dq 
			LEFT JOIN RTDomain.dbo.DimJurisdiction AS dj 
      		ON dq.MM_FIPS_STATE_CODE = dj.FipsStateCode
				AND dq.MM_FIPS_MUNI_CODE = dj.FipsMuniCode
	WHERE dq.MM_FIPS_STATE_CODE = CAST((SUBSTRING(@FIPS, 1, 2)) AS INT)
	AND 	dq.MM_FIPS_MUNI_CODE = CAST((SUBSTRING(@FIPS, 3, 5)) AS INT)
	--------------------------------------------------------------------------
	INSERT INTO Radar.dbo.Sherlock_CLaggregate
	SELECT	SA_PROPERTY_ID
			,FIPS
			,JurisdictionKey
			,SA_PARCEL_NBR_PRIMARY
			,SA_PARCEL_NBR_PREVIOUS
			,SA_PARCEL_NBR_ALT
			,SA_PARCEL_ACCOUNT_NBR
			,SA_PARCEL_NBR_REFERENCE
			,SA_SITE_HOUSE_NBR
			,SA_SITE_STREET_NAME
			,SA_SITE_CITY_STATE
			,SA_SITE_ZIP
			,SA_SITE_UNIT_VAL
			,SA_OWNER_1
			,SA_LGL_DSCRPTN
	FROM	#CLaggregate_Stagging
	WHERE	RowNumSAPID = 1
	AND		SA_PROPERTY_ID NOT IN (
		SELECT SA_PROPERTY_ID FROM Radar.dbo.Sherlock_DimDeleteAssessor
	)

END
GO
