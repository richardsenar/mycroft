
-- oooooooooooo                                                .    o8o                                 
-- `888'     `8                                              .o8    `"'                                 
--  888         oooo    ooo  .ooooo.   .ooooo.  oo.ooooo.  .o888oo oooo   .ooooo.  ooo. .oo.    .oooo.o 
--  888oooo8     `88b..8P'  d88' `"Y8 d88' `88b  888' `88b   888   `888  d88' `88b `888P"Y88b  d88(  "8 
--  888    "       Y888'    888       888ooo888  888   888   888    888  888   888  888   888  `"Y88b.  
--  888       o  .o8"'88b   888   .o8 888    .o  888   888   888 .  888  888   888  888   888  o.  )88b 
-- o888ooooood8 o88'   888o `Y8bod8P' `Y8bod8P'  888bod8P'   "888" o888o `Y8bod8P' o888o o888o 8""888P' 
--                                               888                                                    
--                                              o888o                                                   
                                                                                                     
DECLARE @FIPS char(5) = '13315'
------------------------------------------------------
SELECT FIPS
      ,BKCol
      ,CLCol
      ,BKRule
      ,CLRule
FROM Radar.dbo.Sherlock_Exceptions
WHERE FIPS = @FIPS


/* 

INSERT INTO Radar.dbo.Sherlock_Exceptions
(FIPS,BKCol,CLCol,BKRule,CLRule)
VALUES
('13315',1,1,5,1)


UPDATE Radar.dbo.Sherlock_FIPSInput
SET Loader = NULL, Matcher = NULL, Filter = NULL, Report = NULL
WHERE FIPS IN ('13315')


UPDATE Radar.dbo.Sherlock_FIPSInput
SET Loader = NULL, Matcher = NULL, Filter = NULL, Report = NULL
WHERE FIPS IN ('13315')

*/
