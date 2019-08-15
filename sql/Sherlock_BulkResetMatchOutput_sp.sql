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
ALTER PROCEDURE dbo.Sherlock_BulkResetMatchOutput_sp
AS 
BEGIN 
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Sherlock_FipsInputTmpMO','U') IS NOT NULL 
	    DROP TABLE #Sherlock_FipsInputTmpMO;
    SELECT  FIPS
    INTO    #Sherlock_FipsInputTmpMO
    FROM    Radar.dbo.Sherlock_FIPSInput
    WHERE	Matcher IS NULL
    OR		Matcher = ''
    --------------------------------------------------------------------------
    DECLARE @totalCount int
    SELECT @totalCount = COUNT(*)
    FROM Radar.dbo.Sherlock_MatchOutput
    WHERE FIPS IN (
        SELECT FIPS FROM #Sherlock_FipsInputTmpMO
    )
    --------------------------------------------------------------------------
    --IF (SELECT @totalCount * 1.0 / (SELECT COUNT(*) FROM Radar.dbo.Sherlock_MatchOutput) * 100) > 30.0
    IF @totalCount > 100000
        BEGIN
            --------------------------------------------------------------------------
    	    IF OBJECT_ID('Radar.dbo.Sherlock_MatchOutput_Tmpdb','U') IS NOT NULL 
    		    DROP TABLE Radar.dbo.Sherlock_MatchOutput_Tmpdb
            --------------------------------------------------------------------------
            SELECT  FIPS
                    ,MatchVal
                    ,BKCol
                    ,DPID
                    ,BKRule
                    ,BKHseNum
                    ,BKStrName
                    ,CLCol
                    ,SAPID
                    ,CLRule
                    ,CLHseNum
                    ,CLStrName
                    ,BKZip
                    ,CLZip
                    ,InputMask
                    ,OutputMask
            INTO Radar.dbo.Sherlock_MatchOutput_Tmpdb
            FROM Radar.dbo.Sherlock_MatchOutput
            WHERE FIPS NOT IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpMO
            )
            --------------------------------------------------------------------------
            TRUNCATE TABLE Radar.dbo.Sherlock_MatchOutput
            --------------------------------------------------------------------------
            INSERT INTO Radar.dbo.Sherlock_MatchOutput
            SELECT  FIPS
                    ,MatchVal
                    ,BKCol
                    ,DPID
                    ,BKRule
                    ,BKHseNum
                    ,BKStrName
                    ,CLCol
                    ,SAPID
                    ,CLRule
                    ,CLHseNum
                    ,CLStrName
                    ,BKZip
                    ,CLZip
                    ,InputMask
                    ,OutputMask
            FROM Radar.dbo.Sherlock_MatchOutput_Tmpdb
            --------------------------------------------------------------------------
            DROP TABLE Radar.dbo.Sherlock_MatchOutput_Tmpdb
            --------------------------------------------------------------------------            
        END
    ELSE
        BEGIN
            DELETE FROM Radar.dbo.Sherlock_MatchOutput
            WHERE FIPS IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpMO
            )
        END
    --------------------------------------------------------------------------
END
GO
