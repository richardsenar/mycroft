
RETURN

-- Server
      /*.                    .oooo.     .oooo.     oooooooo 
     .888.                  d8P'`Y8b   d8P'`Y8b   dP""""""" 
    .8"888.       oooooooo 888    888 888    888 d88888b.   
   .8' `888.     d'""7d8P  888    888 888    888     `Y88b  
  .88ooo8888.      .d8P'   888    888 888    888       88  
 .8'     `888.   .d8P'  .P `88b  d88' `88b  d88' o.   .88P  
o88o     o8888o d8888888P   `Y8bd8P'   `Y8bd8P'  `8bd88*/
                             
-------------------------------------------------------
SELECT TOP 1000 DataFileID
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
FROM Radar.dbo.Sherlock_BKaggregate
-------------------------------------------------------
SELECT TOP 1000 SA_PROPERTY_ID
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
FROM Radar.dbo.Sherlock_CLaggregate
-------------------------------------------------------
SELECT TOP 1000 FIPS
      ,Loader
      ,Matcher
      ,Filter
      ,Report
FROM Radar.dbo.Sherlock_FIPSInput
-------------------------------------------------------
SELECT TOP 1000 FIPS
      ,BKCol
      ,CLCol
      ,BKRule
      ,CLRule
FROM Radar.dbo.Sherlock_Exceptions
-------------------------------------------------------
SELECT TOP 1000 FIPS
      ,Src
      ,Col
      ,LeftTrim
      ,RightTrim
      ,MaxLen
FROM Radar.dbo.Sherlock_Truncate
-------------------------------------------------------
SELECT TOP 1000 FIPS
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
FROM Radar.dbo.Sherlock_MatchOutput
-------------------------------------------------------
SELECT TOP 1000 FIPS
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
FROM Radar.dbo.Sherlock_MatchOutputFiltered
-------------------------------------------------------
SELECT TOP 1000 FIPS
      ,Total_BK_Aggregate
      ,Total_CL_Aggregate
      ,Total_MatchOutput
      ,Total_MatchOutputFiltered
      ,Total_Records_Processed
      ,Total_Matches
      ,Total_NoMatches
      ,Total_MatchPct
      ,UpdateDate
FROM Radar.dbo.Sherlock_MatchOutputFilteredReport


/*oooo                                             .   
`888'                                           .o8   
 888  ooo. .oo.    .oooo.o  .ooooo.  oooo d8b .o888oo 
 888  `888P"Y88b  d88(  "8 d88' `88b `888""8P   888   
 888   888   888  `"Y88b.  888ooo888  888       888   
 888   888   888  o.  )88b 888    .o  888       888 . 
o888o o888o o888o 8""888P' `Y8bod8P' d888b      "88*/

RETURN
INSERT INTO Radar.dbo.Sherlock_FIPSInput (FIPS)
VALUES
('13315')


/*ooo     ooo                  .o8                .             
`888'     `8'                 "888              .o8             
 888       8  oo.ooooo.   .oooo888   .oooo.   .o888oo  .ooooo.  
 888       8   888' `88b d88' `888  `P  )88b    888   d88' `88b 
 888       8   888   888 888   888   .oP"888    888   888ooo888 
 `88.    .8'   888   888 888   888  d8(  888    888 . 888    .o 
   `YbodP'     888bod8P' `Y8bod88P" `Y888""8o   "888" `Y8bod8P' 
               888                                              
              o88*/

RETURN
UPDATE Radar.dbo.Sherlock_FIPSInput
SET 	Loader = NULL
      ,Matcher = NULL
    	,Filter = NULL
    	,Report = NULL
WHERE FIPS IN ('21111')

RETURN
UPDATE Radar.dbo.Sherlock_FIPSInput
SET 	Loader = GETDATE()
      ,Matcher = GETDATE()
    	,Filter = GETDATE()
    	,Report = GETDATE()
WHERE FIPS IN ('21111')

-------------------------------------------------------
RETURN
UPDATE Radar.dbo.Sherlock_FIPSInput
SET 	Matcher = 'SKIP'
    	,Filter = 'SKIP'
    	,Report = 'SKIP'
WHERE FIPS IN ('33015 ','44007')

UPDATE Radar.dbo.Sherlock_FIPSInput
SET 	Loader = NULL
      ,Matcher = NULL
    	,Filter = NULL


      /*.                             oooo                        o8o           
     .888.                            `888                        `"'           
    .8"888.     ooo. .oo.    .oooo.    888  oooo    ooo  .oooo.o oooo   .oooo.o 
   .8' `888.    `888P"Y88b  `P  )88b   888   `88.  .8'  d88(  "8 `888  d88(  "8 
  .88ooo8888.    888   888   .oP"888   888    `88..8'   `"Y88b.   888  `"Y88b.  
 .8'     `888.   888   888  d8(  888   888     `888'    o.  )88b  888  o.  )88b 
o88o     o8888o o888o o888o `Y888""8o o888o     .8'     8""888P' o888o 8""888P' 
                                            .o..P'                              
                                            `Y8*/
SELECT
(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Loader IS NOT NULL) AS Load_Done
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Loader IS NULL) AS Load_Pending
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Matcher IS NOT NULL) AS Match_Done
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Matcher IS NULL) AS Match_Pending
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Filter IS NOT NULL) AS Filter_Done
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Filter IS NULL) AS Filter_Pending
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Report IS NOT NULL) AS Report_Done
,(SELECT COUNT(1) FROM Radar.dbo.Sherlock_FIPSInput WHERE Report IS NULL) AS Report_Pending

------------------------------------------------------
RETURN
DECLARE @FIPS char(5) = '13315'
------------------------------------------------------
SELECT mo.BKCol AS BK_ColID
		,mo.CLCol AS CL_ColID
		,mo.BKRule AS BK_Rule
		,mo.CLRule AS CL_RuleID
		,COUNT(*) AS TOTAL
FROM	Radar.dbo.Sherlock_MatchOutputFiltered as mo WITH(NOLOCK)
WHERE	1=1
AND		mo.FIPS = @FIPS
AND		mo.BKRule <> -1 
AND		mo.CLRule <> -1 
-- AND		mo.MatchVal <> 'nomatch' -- MATCHES
-- AND		mo.MatchVal = 'nomatch' -- NO MATCHES
GROUP BY mo.BKCol, mo.CLCol, mo.BKRule, mo.CLRule
ORDER BY mo.CLRule DESC
------------------------------------------------------
SELECT	mo.MatchVal
		,mo.FIPS AS FIPS
		,mo.DPID AS BK_DPID
		,mo.SAPID AS CL_SAPID
		,mo.BKCol AS BKColNum
		,mo.CLCol AS CLColNum
		,mo.BKRule AS BKRuleNum
		,mo.CLRule AS CLRuleNum
		,mo.InputMask AS InputMask 
		,mo.OutputMask AS OutputMask
		,bkagg.APN AS BK_APN
		,bkagg.OLD_APN AS BK_OLDAPN
		,bkagg.TAX_ACCT_NBR AS BK_TaxAcctNum
		,clagg.SA_PARCEL_NBR_PRIMARY AS CL_PN_Prime
		,clagg.SA_PARCEL_NBR_PREVIOUS AS CL_PN_Prev
		,clagg.SA_PARCEL_NBR_ALT AS CL_PN_Alt
		,clagg.SA_PARCEL_ACCOUNT_NBR AS CL_PN_AcctNum
		,clagg.SA_PARCEL_NBR_REFERENCE AS CL_PN_Ref
		,bkagg.PROP_HOUSE_NBR
		,clagg.SA_SITE_HOUSE_NBR
		,bkagg.PROP_STREET_NAME
		,clagg.SA_SITE_STREET_NAME
		,bkagg.PROP_UNIT_NBR
		,clagg.SA_SITE_UNIT_VAL
		,bkagg.PROP_ZIP
		,clagg.SA_SITE_ZIP
		,bkagg.JurisdictionKey AS bkJurKey
		,clagg.JurisdictionKey AS clJurKey
FROM	Radar.dbo.Sherlock_MatchOutputFiltered as mo WITH(NOLOCK)
		LEFT JOIN Radar.dbo.Sherlock_BKaggregate AS bkagg
			ON mo.DPID = bkagg.DPID
		LEFT JOIN Radar.dbo.Sherlock_CLaggregate AS clagg
			ON mo.SAPID = clagg.SA_PROPERTY_ID
WHERE	1=1
AND		bkagg.FIPS = @FIPS
-- AND		mo.MatchVal <> 'nomatch' -- MATCHES
-- AND		mo.MatchVal = 'nomatch' -- NO MATCHES
AND		mo.BKRule <> -1 
AND		mo.CLRule <> -1 
ORDER BY	mo.SAPID



  /*oooooo.                                    .             
 d8P'  `Y8b                                 .o8             
888          oooo d8b  .ooooo.   .oooo.   .o888oo  .ooooo.  
888          `888""8P d88' `88b `P  )88b    888   d88' `88b 
888           888     888ooo888  .oP"888    888   888ooo888 
`88b    ooo   888     888    .o d8(  888    888 . 888    .o 
 `Y8bood8P'  d888b    `Y8bod8P' `Y888""8o   "888" `Y8bod8*/

RETURN
DROP TABLE Radar.dbo.Sherlock_BKaggregate
CREATE TABLE Radar.dbo.Sherlock_BKaggregate(
	ID int IDENTITY (1,1) NOT NULL,
	DataFileID int NULL,
	FIPS char(5) NULL,
	State char(2) NULL,
	County varchar(MAX) NULL,
	JurisdictionKey int NULL,
	ScmId int NULL,
	APN varchar(MAX) NULL,
	TAX_ACCT_NBR varchar(MAX) NULL,
	LEGAL_FULL_DSCRP varchar(MAX) NULL,
	OLD_APN varchar(MAX) NULL,
	DPID varchar(450) NULL,
	DUP_APN varchar(MAX) NULL,
	PROP_HOUSE_NBR varchar(MAX) NULL,
	PROP_STREET_NAME varchar(MAX) NULL,
	PROP_CITY varchar(MAX) NULL,
	PROP_STATE varchar(MAX) NULL,
	PROP_ZIP varchar(MAX) NULL,
	PROP_UNIT_NBR varchar(MAX) NULL,
	OWNER_NAME varchar(MAX) NULL,
	CONSTRAINT PK_Sherlock_BKaggregate_ID PRIMARY KEY CLUSTERED (ID ASC)
)
CREATE INDEX IDX_FIPS ON Radar.dbo.Sherlock_BKaggregate (FIPS ASC)
-------------------------------------------------------
RETURN
DROP TABLE Radar.dbo.Sherlock_CLaggregate
CREATE TABLE Radar.dbo.Sherlock_CLaggregate(
	ID int IDENTITY (1,1) NOT NULL,
	SA_PROPERTY_ID int NULL,
	FIPS char(5) NULL,
	JurisdictionKey int NULL,
	SA_PARCEL_NBR_PRIMARY varchar(MAX) NULL,
	SA_PARCEL_NBR_PREVIOUS varchar(MAX) NULL,
	SA_PARCEL_NBR_ALT varchar(MAX) NULL,
	SA_PARCEL_ACCOUNT_NBR varchar(MAX) NULL,
	SA_PARCEL_NBR_REFERENCE varchar(MAX) NULL,
	SA_SITE_HOUSE_NBR varchar(MAX) NULL,
	SA_SITE_STREET_NAME varchar(MAX) NULL,
	SA_SITE_CITY_STATE varchar(MAX) NULL,
	SA_SITE_ZIP varchar(MAX) NULL,
	SA_SITE_UNIT_VAL varchar(MAX) NULL,
	SA_OWNER_1 varchar(MAX) NULL,
	SA_LGL_DSCRPTN varchar(MAX) NULL,
	CONSTRAINT PK_Sherlock_CLaggregate_ID PRIMARY KEY CLUSTERED (ID ASC)
)
CREATE INDEX IDX_FIPS ON Radar.dbo.Sherlock_CLaggregate (FIPS ASC)
-------------------------------------------------------
RETURN
ALTER TABLE Radar.dbo.Sherlock_MatchOutput DROP CONSTRAINT PK_Sherlock_MatchOutput_ID
DROP TABLE Radar.dbo.Sherlock_MatchOutput
CREATE TABLE Radar.dbo.Sherlock_MatchOutput(
	ID int IDENTITY (1,1) NOT NULL,
	FIPS char(5) NULL,
	MatchVal varchar(MAX) NULL,
	BKCol int NULL,
	DPID varchar(450) NULL,
	BKRule int NULL,
	BKHseNum varchar(MAX) NULL,
	BKStrName varchar(MAX) NULL,
      BKUnitNum varchar(MAX) NULL,
	CLCol int NULL,
	SAPID varchar(450) NULL,
	CLRule int NULL,
	CLHseNum varchar(MAX) NULL,
	CLStrName varchar(MAX) NULL,
      CLUnitNum varchar(MAX) NULL,
	BKZip varchar(MAX) NULL,
	CLZip varchar(MAX) NULL,
   InputMask varchar(MAX) NULL,
   OutputMask varchar(MAX) NULL,
	CONSTRAINT PK_Sherlock_MatchOutput_ID PRIMARY KEY CLUSTERED (ID ASC)
)
CREATE INDEX IDX_FIPS ON Radar.dbo.Sherlock_MatchOutput (FIPS ASC)
-------------------------------------------------------
RETURN
ALTER TABLE Radar.dbo.Sherlock_MatchOutputFiltered DROP CONSTRAINT PK_Sherlock_MatchOutput_ID
DROP TABLE Radar.dbo.Sherlock_MatchOutputFiltered
CREATE TABLE Radar.dbo.Sherlock_MatchOutputFiltered(
	ID int IDENTITY (1,1) NOT NULL,
	FIPS char(5) NULL,
	MatchVal varchar(MAX) NULL,
	BKCol int NULL,
	DPID varchar(450) NULL,
	BKRule int NULL,
	BKHseNum varchar(MAX) NULL,
	BKStrName varchar(MAX) NULL,
      BKUnitNum varchar(MAX) NULL,
	CLCol int NULL,
	SAPID varchar(450) NULL,
	CLRule int NULL,
	CLHseNum varchar(MAX) NULL,
	CLStrName varchar(MAX) NULL,
      CLUnitNum varchar(MAX) NULL,      
	BKZip varchar(MAX) NULL,
	CLZip varchar(MAX) NULL,
   InputMask varchar(MAX) NULL,
   OutputMask varchar(MAX) NULL,
	CONSTRAINT PK_Sherlock_MatchOutputFiltered_ID PRIMARY KEY CLUSTERED (ID ASC)
)
CREATE INDEX IDX_FIPS ON Radar.dbo.Sherlock_MatchOutputFiltered (FIPS ASC)



/*ooooooooo.             oooo                .             
`888'   `Y8b            `888              .o8             
 888      888  .ooooo.   888   .ooooo.  .o888oo  .ooooo.  
 888      888 d88' `88b  888  d88' `88b   888   d88' `88b 
 888      888 888ooo888  888  888ooo888   888   888ooo888 
 888     d88' 888    .o  888  888    .o   888 . 888    .o 
o888bood8P'   `Y8bod8P' o888o `Y8bod8P'   "888" `Y8bod8*/

RETURN
TRUNCATE TABLE Radar.dbo.Sherlock_BKaggregate
TRUNCATE TABLE Radar.dbo.Sherlock_CLaggregate

RETURN
TRUNCATE TABLE Radar.dbo.Sherlock_MatchOutput
TRUNCATE TABLE Radar.dbo.Sherlock_MatchOutputFiltered

 /*oooooo..o                          .                               
d8P'    `Y8                        .o8                               
Y88bo.      oooo    ooo  .oooo.o .o888oo  .ooooo.  ooo. .oo.  .oo.   
 `"Y8888o.   `88.  .8'  d88(  "8   888   d88' `88b `888P"Y88bP"Y88b  
     `"Y88b   `88..8'   `"Y88b.    888   888ooo888  888   888   888  
oo     .d8P    `888'    o.  )88b   888 . 888    .o  888   888   888  
8""88888P'      .8'     8""888P'   "888" `Y8bod8P' o888or o888o o888o 
            .o..P'                                                   
            `Y8*/

SELECT  p.spid,
        p.status,
        p.hostname,
        p.loginame,
        p.cpu,
        r.start_time,
        r.command,
        p.program_name,
        text 
FROM    sys.dm_exec_requests AS r,
        master.dbo.sysprocesses AS p 
        CROSS APPLY sys.dm_exec_sql_text(p.sql_handle)
WHERE   1=1
AND     p.status NOT IN ('sleeping', 'background') 
AND     r.session_id = p.spid
AND     program_name = 'go-mssqldb'



-------------------------------------
/*
Hi Guys, Here is the SQL script to generate the unique DPID + SPAID pairs
The output table will be generated in DBWork in this format
dbwork.dbo.Staviean_Sherlock_MatchOutputFiltered_Pair_<Fips code>
Step 1: Set the FIPS code below
Step 2: It will check if output tabke already exists, it is does, it will drop the table
Step 3: Execute select statement then insert to output table
Step 4: Generate report
*/

DECLARE @fips char(5) = '21111' -- Set the ZIP code here
-------------------------------------
DECLARE @pairtable varchar(100)
SET @pairtable = 'dbwork.dbo.Staviean_Sherlock_MatchOutputFiltered_Pair_' + @fips
DECLARE @droptable varchar(500)
SET @droptable = 'IF OBJECT_ID(''' + @pairtable + ''') IS NOT NULL DROP TABLE ' + @pairtable
EXEC (@droptable)
DECLARE @sql varchar(500)
SET @sql = 'SELECT DPID, SAPID INTO '+ @pairtable + ' 
            FROM Radar.dbo.Sherlock_MatchOutputFiltered 
            WHERE MatchVal <> ''nomatch'' 
            AND FIPS = ' + @fips + ' 
            GROUP BY DPID, SAPID'
EXEC (@sql)
DECLARE @createbkidx varchar(500)
DECLARE @createclidx varchar(500)
SET @createbkidx = 'CREATE INDEX IDX_DPID ON ' + @pairtable +' (DPID ASC)'
SET @createclidx = 'CREATE INDEX IDX_SAPID ON ' + @pairtable +' (SAPID ASC)'
EXEC (@createbkidx)
EXEC (@createclidx)
-------------------------------------
-- BK Report
DECLARE @bkreport varchar(500)
SET @bkreport = ' SELECT * 
                  FROM Radar.dbo.Sherlock_BKaggregate AS BKAgg 
                  LEFT JOIN ' + @pairtable + ' AS pair 
                        ON BKAgg.DPID = pair.DPID 
                  WHERE pair.DPID IS NULL 
                  AND FIPS = ' + @fips
EXEC (@bkreport)
-------------------------------------
-- CL Report
DECLARE @clreport varchar(500)
SET @clreport = ' SELECT * 
                  FROM Radar.dbo.Sherlock_CLaggregate AS CLAgg 
                  LEFT JOIN ' + @pairtable + ' AS pair 
                        ON CLAgg.SA_PROPERTY_ID = pair.SAPID 
                  WHERE pair.SAPID IS NULL 
                  AND FIPS = ' + @fips
EXEC (@clreport)
