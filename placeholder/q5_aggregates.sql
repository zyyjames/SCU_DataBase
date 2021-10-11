SELECT * 
FROM (SELECT CategoryName,count(*) as count,round(avg(UnitPrice),2),min(UnitPrice),max(UnitPrice),sum(UnitsOnOrder) 
      FROM Category,Product WHERE Categoryid=Category.id GROUP BY Categoryid ORDER BY Categoryid) 
WHERE count>10;