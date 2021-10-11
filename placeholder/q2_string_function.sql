SELECT DISTINCT shipName,substr(shipName,1,instr(shipName,'-')-1) 
FROM 'Order' 
WHERE shipName LIKE '%-%' ORDER BY shipName;