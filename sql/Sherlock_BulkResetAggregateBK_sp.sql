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
ALTER PROCEDURE dbo.Sherlock_BulkResetAggregateBK_sp
AS 
BEGIN 
    --------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#Sherlock_FipsInputTmpBK','U') IS NOT NULL 
		DROP TABLE #Sherlock_FipsInputTmpBK;
    SELECT  FIPS
    INTO    #Sherlock_FipsInputTmpBK
    FROM    Radar.dbo.Sherlock_FIPSInput
    WHERE	Loader IS NULL
    OR		Loader = ''
    --------------------------------------------------------------------------
    -- EXCEPTION FRO BEDFORD COUNTY
    UPDATE  #Sherlock_FipsInputTmpBK
    SET     FIPS = '51019'
    WHERE   FIPS = '51515'
    --------------------------------------------------------------------------
    DECLARE @totalCount int
    SELECT @totalCount = COUNT(*)
    FROM Radar.dbo.Sherlock_BKaggregate
    WHERE FIPS IN (
        SELECT  FIPS FROM #Sherlock_FipsInputTmpBK
    )
    --------------------------------------------------------------------------
    -- IF (SELECT @totalCount * 1.0 / (SELECT COUNT(*) FROM Radar.dbo.Sherlock_BKaggregate) * 100) > 30.0
    IF @totalCount > 100000
        BEGIN
            --------------------------------------------------------------------------
    	    IF OBJECT_ID('Radar.dbo.Sherlock_BKaggregate_Tmpdb','U') IS NOT NULL 
    		    DROP TABLE Radar.dbo.Sherlock_BKaggregate_Tmpdb
            --------------------------------------------------------------------------
            SELECT  DataFileID
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
            INTO Radar.dbo.Sherlock_BKaggregate_Tmpdb
            FROM Radar.dbo.Sherlock_BKaggregate
            WHERE FIPS NOT IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpBK
            )
            --------------------------------------------------------------------------
            TRUNCATE TABLE Radar.dbo.Sherlock_BKaggregate
            --------------------------------------------------------------------------
            INSERT INTO Radar.dbo.Sherlock_BKaggregate
            SELECT  DataFileID
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
            FROM Radar.dbo.Sherlock_BKaggregate_Tmpdb
            --------------------------------------------------------------------------
            DROP TABLE Radar.dbo.Sherlock_BKaggregate_Tmpdb
            --------------------------------------------------------------------------
        END
    ELSE
        BEGIN
            DELETE FROM Radar.dbo.Sherlock_BKaggregate
            WHERE FIPS IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpBK
            )
        END
    --------------------------------------------------------------------------
END
GO
