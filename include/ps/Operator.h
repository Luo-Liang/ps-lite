#pragma once
#include "phub.h"
#include "OperatorContext.h"
typedef int OperatorHandle;
using namespace std;
enum OperatorType
{
	DataMovement,
	CollectiveAlgorithm,
	DataManipulation
};
class IOperator
{
public:
	 virtual OperatorHandle RunAsync(OperatorContext context) = 0;
	 string Name;
	 OperatorType Type;
};