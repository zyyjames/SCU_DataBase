SELECT CompanyName,CustomerId,ROUND(expenditure,2)
FROM
   (SELECT IFNULL(CompanyName,'MISSING_NAME' ) AS CompanyName,NTILE(4) OVER(ORDER BY expenditure ASC) AS groups,CustomerId,expenditure
	FROM(SELECT * ,SUM(UnitPrice*Quantity) AS expenditure FROM 'Order'
		LEFT JOIN Customer ON 'Order'.CustomerId=Customer.Id
		LEFT JOIN OrderDetail ON 'Order'.Id=OrderDetail.OrderId
		GROUP BY 'Order'.CustomerId))
WHERE groups=1
ORDER BY expenditure;