#pragma once
#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>

class PHubAggregator :IOperator
{
	shared_ptr<PHub> pPhub = NULL;
	size_t keySize;
	shared_ptr<PHubOperatorContext> opContext;
	shared_ptr<TTAggregator> aggregator;
	vector<float*> sources;
	//there is a single destination because it is the mergebuffer.
	float* dest;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	virtual OperationStatus Run() override;
};