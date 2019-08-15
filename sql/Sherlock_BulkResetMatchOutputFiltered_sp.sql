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
ALTER PROCEDURE dbo.Sherlock_BulkResetMatchOutputFiltered_sp
AS 
BEGIN 
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Sherlock_FipsInputTmpMOF','U') IS NOT NULL 
	    DROP TABLE #Sherlock_FipsInputTmpMOF;
    SELECT  FIPS
    INTO    #Sherlock_FipsInputTmpMOF
    FROM    Radar.dbo.Sherlock_FIPSInput
    WHERE	Filter IS NULL
    OR		Filter = ''
    --------------------------------------------------------------------------
    DECLARE @totalCount int
    SELECT @totalCount = COUNT(*)
    FROM Radar.dbo.Sherlock_MatchOutputFiltered
    WHERE FIPS IN (
        SELECT FIPS FROM #Sherlock_FipsInputTmpMOF
    )
    --------------------------------------------------------------------------
    --IF (SELECT @totalCount * 1.0 / (SELECT COUNT(*) FROM Radar.dbo.Sherlock_MatchOutputFiltered) * 100) > 30.0
    IF @totalCount > 100000
        BEGIN
            --------------------------------------------------------------------------
    	    IF OBJECT_ID('Radar.dbo.Sherlock_MatchOutputFiltered_Tmpdb','U') IS NOT NULL 
    		    DROP TABLE Radar.dbo.Sherlock_MatchOutputFiltered_Tmpdb
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
            INTO Radar.dbo.Sherlock_MatchOutputFiltered_Tmpdb
            FROM Radar.dbo.Sherlock_MatchOutputFiltered
            WHERE FIPS NOT IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpMOF
            )
            --------------------------------------------------------------------------
            TRUNCATE TABLE Radar.dbo.Sherlock_MatchOutputFiltered
            --------------------------------------------------------------------------
            INSERT INTO Radar.dbo.Sherlock_MatchOutputFiltered
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
            FROM Radar.dbo.Sherlock_MatchOutputFiltered_Tmpdb
            --------------------------------------------------------------------------
            DROP TABLE Radar.dbo.Sherlock_MatchOutputFiltered_Tmpdb
            --------------------------------------------------------------------------
        END
    ELSE
        BEGIN
            DELETE FROM Radar.dbo.Sherlock_MatchOutputFiltered
            WHERE FIPS IN (
                SELECT FIPS FROM #Sherlock_FipsInputTmpMOF
            )
        END
    --------------------------------------------------------------------------
END
GO
