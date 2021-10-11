SELECT RegionDescription,FirstName,LastName,BirthDate
FROM(SELECT * FROM Employee
	LEFT JOIN EmployeeTerritory ON Employee.Id=EmployeeTerritory.employeeId
	LEFT JOIN Territory ON EmployeeTerritory.TerritoryId=Territory.Id
	LEFT JOIN Region ON Territory.RegionId=Region.Id
	ORDER BY Employee.BirthDate DESC)
GROUP BY RegionDescription
ORDER BY RegionId;