USE [Radar]
GO
/****** Object:  StoredProcedure [dbo].[Sherlock_Aggregate_RemDeletes_sp]    Script Date: 1/4/2019 10:28:30 AM ******/
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
ALTER PROCEDURE [dbo].[Sherlock_BulkResetMatchOutputFilteredReport_sp]
AS 
BEGIN 
    DELETE FROM Radar.dbo.Sherlock_MatchOutputFilteredReport
    WHERE FIPS IN (
        SELECT  FIPS
        FROM    Radar.dbo.Sherlock_FIPSInput
        WHERE	Report IS NULL
        OR		Report = ''
    )
END
GO
