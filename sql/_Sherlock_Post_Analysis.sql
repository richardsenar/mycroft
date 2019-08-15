
--       .o.                             oooo                        o8o           
--      .888.                            `888                        `"'           
--     .8"888.     ooo. .oo.    .oooo.    888  oooo    ooo  .oooo.o oooo   .oooo.o 
--    .8' `888.    `888P"Y88b  `P  )88b   888   `88.  .8'  d88(  "8 `888  d88(  "8 
--   .88ooo8888.    888   888   .oP"888   888    `88..8'   `"Y88b.   888  `"Y88b.  
--  .8'     `888.   888   888  d8(  888   888     `888'    o.  )88b  888  o.  )88b 
-- o88o     o8888o o888o o888o `Y888""8o o888o     .8'     8""888P' o888o 8""888P' 
--                                             .o..P'                              
--                                             `Y8P'                               

DECLARE @FIPS char(5) = '13315'
------------------------------------------------------
SELECT	mo.BKCol AS BK_ColID
			,mo.CLCol AS CL_ColID
			,mo.BKRule AS BK_Rule
			,mo.CLRule AS CL_RuleID
			,COUNT(*) AS TOTAL
FROM		Radar.dbo.Sherlock_MatchOutputFiltered as mo WITH(NOLOCK)
WHERE		mo.FIPS = @FIPS
-- AND		mo.MatchVal <> 'nomatch' -- MATCHES
AND			mo.MatchVal = 'nomatch' -- NO MATCHES
AND			mo.BKRule <> -1 
AND			mo.CLRule <> -1 
GROUP BY	mo.BKCol, mo.CLCol, mo.BKRule, mo.CLRule
ORDER BY	mo.CLRule DESC
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
FROM		Radar.dbo.Sherlock_MatchOutputFiltered as mo WITH(NOLOCK)
			LEFT JOIN Radar.dbo.Sherlock_BKaggregate AS bkagg
				ON mo.DPID = bkagg.DPID
			LEFT JOIN Radar.dbo.Sherlock_CLaggregate AS clagg
				ON mo.SAPID = clagg.SA_PROPERTY_ID
WHERE		bkagg.FIPS = @FIPS
-- AND			mo.MatchVal <> 'nomatch' -- MATCHES
AND			mo.MatchVal = 'nomatch' -- NO MATCHES
AND			mo.BKRule <> -1 
AND			mo.CLRule <> -1 
-- AND 			mo.BKCol = 1
-- AND 			mo.CLCol = 1
-- AND 			mo.BKRule = 1
-- AND 			mo.CLRule = 1
ORDER BY	mo.CLRule DESC




--------------------------------------
-- For DT845
--------------------------------------
SELECT [MatchVal]
      ,[FIPS]
      ,[BK_DPID]
      ,[CL_SAPID]
      ,[BKColNum]
      ,[CLColNum]
      ,[BKRuleNum]
      ,[CLRuleNum]
      ,[InputMask]
      ,[OutputMask]
      ,[BK_APN]
      --,[BK_OLDAPN]
      --,[BK_TaxAcctNum]
      ,[CL_PN_Prime]
      ,[CL_PN_Prev]
      ,[CL_PN_Alt]
      ,[CL_PN_AcctNum]
      ,[CL_PN_Ref]
      ,[PROP_HOUSE_NBR]
      ,[SA_SITE_HOUSE_NBR]
      ,[PROP_STREET_NAME]
      ,[SA_SITE_STREET_NAME]
      ,[PROP_UNIT_NBR]
      ,[SA_SITE_UNIT_VAL]
      ,[PROP_ZIP]
      ,[SA_SITE_ZIP]
      ,[bkJurKey]
      ,[clJurKey]
FROM [Radar].[dbo].[Sherlock_MatchOutputFiltered_DT845_Raw]
WHERE [BKRuleNum] IN (5)
AND ([PROP_HOUSE_NBR] = '' OR [SA_SITE_HOUSE_NBR] = '')
ORDER BY [CL_SAPID] DESC


SELECT [MatchVal]
      ,[FIPS]
      ,[RePropertyID]
      ,[CL_SAPID]
      ,[ColNum]
      ,[CLColNum]
      ,[RuleNum]
      ,[CLRuleNum]
      ,[InputMask]
      ,[OutputMask]
      ,[APN]
      --,[OLDAPN]
      --,[TaxAcctNum]
      ,[CL_PN_Prime]
      ,[CL_PN_Prev]
      ,[CL_PN_Alt]
      ,[CL_PN_AcctNum]
      ,[CL_PN_Ref]
      ,[PROP_HOUSE_NBR]
      ,[SA_SITE_HOUSE_NBR]
      ,[PROP_STREET_NAME]
      ,[SA_SITE_STREET_NAME]
      ,[PROP_UNIT_NBR]
      ,[SA_SITE_UNIT_VAL]
      ,[PROP_ZIP]
      ,[SA_SITE_ZIP]
      ,[bkJurKey]
      ,[clJurKey]
FROM [Radar].[dbo].[Sherlock_MatchOutputFiltered_DT845]
WHERE [RuleNum] NOT IN (1, 9000)
--ORDER BY [RuleNum] DESC
ORDER BY [CLColNum] DESC

