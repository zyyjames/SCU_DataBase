#include <iostream>
#include <vector>
#include <memory>
#include "p0_starter.h"
#include <stdio.h>
void DisplayMat(const scudb::RowMatrix<int>& Mat)
{
    for(int r=0;r<Mat.GetRowCount();r++)
    {
        for(int c=0;c<Mat.GetColumnCount();c++)
            printf("%2d  ",Mat.GetElement(r,c));
        printf("\n");
    }
    printf("\n");
}
int main()
{
	auto MatA = scudb::RowMatrix<int>(3, 3);
	auto MatB = scudb::RowMatrix<int>(3, 3);
	auto MatC = scudb::RowMatrix<int>(3, 3);
	std::vector<int>TempA(9, 1);
	std::vector<int>TempB(9, 2);
	std::vector<int>TempC(9, 3);
	MatA.FillFrom(TempA);
	MatB.FillFrom(TempB);
	MatC.FillFrom(TempC);
    std::cout << "A:" << std::endl;
	DisplayMat(MatA);
	std::cout << "B:" << std::endl;
	DisplayMat(MatB);
	std::cout << "C:" << std::endl;
	DisplayMat(MatC);
	std::unique_ptr<scudb::RowMatrix<int>> PlusRes = scudb::RowMatrixOperations<int>::Add(&MatA, &MatB);
	std::cout << "A+B:" << std::endl;
	DisplayMat(*PlusRes);

	std::unique_ptr<scudb::RowMatrix<int>> MultiplyRes = scudb::RowMatrixOperations<int>::Multiply(&MatA, &MatB);
	std::cout << "A*B:" << std::endl;
	DisplayMat(*MultiplyRes);

	std::unique_ptr<scudb::RowMatrix<int>> GEMMRes = scudb::RowMatrixOperations<int>::GEMM(&MatA, &MatB,&MatC);
	std::cout << "A*B+C:" << std::endl;
	DisplayMat(*GEMMRes);
	return 0;
}
