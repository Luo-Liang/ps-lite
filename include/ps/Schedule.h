#pragma once
#include "Operator.h"
#include "OperatorContext.h"
//a schedule is simply a list of operators and 
//a list of contexts.
class Schedule
{
public:
	vector<std::tuple<IOperator, OperatorContext>> Steps;
	//????
};