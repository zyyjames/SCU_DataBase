SELECT GROUP_CONCAT(ProductName)
FROM 'Order'
	LEFT JOIN OrderDetail ON 'Order'.Id=OrderDetail.OrderId
	LEFT JOIN Product ON OrderDetail.ProductId=Product.Id
	LEFT JOIN Customer ON 'Order'.CustomerId=Customer.Id
WHERE Customer.CompanyName= 'Queen Cozinha' AND 'Order'.OrderDate LIKE '2014-12-25%';
