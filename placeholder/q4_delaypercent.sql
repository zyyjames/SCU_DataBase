SELECT CompanyName,Round(100.*(SELECT COUNT(*) FROM 'Order' where Shipper.Id='Order'.ShipVia and ShippedDate>RequiredDate)/(SELECT COUNT(*) FROM 'Order' where Shipper.Id='Order'.ShipVia),2) as delaypercent
FROM Shipper
Order BY delaypercent desc;