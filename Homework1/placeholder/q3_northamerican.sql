SELECT Id,ShipCountry,(case when ShipCountry in ('USA','Mexico','Canada') then 'NorthAmerica' else 'OtherPlace' end)
FROM 'Order' 
WHERE Id>=15445 
LIMIT 20;