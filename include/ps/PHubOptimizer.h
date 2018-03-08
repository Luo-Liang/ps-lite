#pragma once
#include <ps/Operator.h>
class NAGTTOptimizer;


class PHubOptimizer : IOperator
{
public:
	shared_ptr<NAGTTOptimizer> opt;
	shared_ptr<PHub> pHub;
	size_t numAggregated = -1;
	//PHubAllocator* allocator;
	shared_ptr<vector<KeyDesc>> pKeyDescs;
	PLinkKey targetKey;
	float* weight;
	float* grad;
	size_t len;
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	virtual OperationStatus Run() override;
};