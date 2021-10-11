SELECT ProductName,CompanyName,ContactName
FROM (SELECT * FROM 'Order' 
               LEFT JOIN OrderDetail ON 'Order'.Id=OrderDetail.OrderId 
               LEFT JOIN Customer ON CustomerId=Customer.Id 
               LEFT JOIN Product ON ProductId=Product.Id 
               WHERE Discontinued = 1 
               ORDER BY OrderDate) 
GROUP BY ProductId 
ORDER BY ProductName;