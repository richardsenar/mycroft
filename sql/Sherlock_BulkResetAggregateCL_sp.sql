USE Radar
GO
/****** Object:  StoredProcedure dbo.Sherlock_Aggregate_RemDeletes_sp    Script Date: 1/4/2019 10:28:30 AM ******/
SET ANSI_NULLS ON
GO
SET ANSI_WARNINGS OFF;
GO
SET QUOTED_IDENTIFIER ON
GO
/********************************************************************************************************************
-- Date        Initials	Description
-- ----------  --------	--------------------------------
-- 08/22/2018	RS		Bulk Reset MatchOutput Table
********************************************************************************************************************/
ALTER PROCEDURE dbo.Sherlock_BulkResetAggregateCL_sp
AS 
BEGIN 
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Sherlock_FipsInputTmpCL','U') IS NOT NULL 
	    DROP TABLE #Sherlock_FipsInputTmpCL;
    SELECT  FIPS
    INTO    #Sherlock_FipsInputTmpCL
    FROM    Radar.dbo.Sherlock_FIPSInput
    WHERE	Loader IS NULL
    OR		Loader = ''
    --------------------------------------------------------------------------
    DECLARE @totalCount int
    SELECT @totalCount = COUNT(*)
    FROM Radar.dbo.Sherlock_CLaggregate
    WHERE FIPS IN (
        SELECT FIPS FROM #Sherlock_FipsInputTmpCL
    )
    --------------------------------------------------------------------------
    -- IF (SELECT @totalCount * 1.0 / (SELECT COUNT(*) FROM Radar.dbo.Sherlock_CLaggregate) * 100) > 30.0
    IF @totalCount > 100000
        BEGIN
            --------------------------------------------------------------------------
    	    IF OBJECT_ID('Radar.dbo.Sherlock_CLaggregate_Tmpdb','U') IS NOT NULL 
    		    DROP TABLE Radar.dbo.Sherlock_CLaggregate_Tmpdb
            --------------------------------------------------------------------------
            SELECT  SA_PROPERTY_ID
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
            INTO Radar.dbo.Sherlock_CLaggregate_Tmpdb
            FROM Radar.dbo.Sherlock_CLaggregate
            WHERE FIPS NOT IN (
                SELECT  FIPS FROM #Sherlock_FipsInputTmpCL
            )
            --------------------------------------------------------------------------
            TRUNCATE TABLE Radar.dbo.Sherlock_CLaggregate
            --------------------------------------------------------------------------
            INSERT INTO Radar.dbo.Sherlock_CLaggregate
            SELECT SA_PROPERTY_ID
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
            FROM Radar.dbo.Sherlock_CLaggregate_Tmpdb
            --------------------------------------------------------------------------
            DROP TABLE Radar.dbo.Sherlock_CLaggregate_Tmpdb
            --------------------------------------------------------------------------            
        END
    ELSE
        BEGIN
            DELETE FROM Radar.dbo.Sherlock_CLaggregate
            WHERE FIPS IN (
                SELECT  FIPS FROM #Sherlock_FipsInputTmpCL
            )
        END
    --------------------------------------------------------------------------
END
GO
