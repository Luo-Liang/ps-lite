#pragma once
#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>

class PHubAggregator :IOperator
{
	shared_ptr<PHub> pPhub = NULL;
	PLinkKey key;
	size_t keySize;
	shared_ptr<OperatorContext> opContext;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	virtual OperationStatus Run() override;
};