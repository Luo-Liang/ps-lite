#pragma once
#include <ps/Operator.h>
#include <ps/phub.h>
class NAGTTOptimizer;


class PHubOptimizerOp : IOperator
{
public:
	shared_ptr<NAGTTOptimizer> opt;
	shared_ptr<PHub> pHub;
	size_t numAggregated = -1;
	//PHubAllocator* allocator
	PLinkKey targetKey;
	float* weight = NULL;
	float* grad = NULL;
	size_t len = 0;
	PLinkKey key;
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	virtual OperationStatus Run() override;
};