#pragma once
#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
using namespace std;

class PHubBroadcastOp : IOperator
{
	bool isReceiver = true;
	shared_ptr<PHub> pPhub = NULL;
	PLinkKey key;
	size_t keySize;
	shared_ptr<PHubOperatorContext> opContext;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	virtual OperationStatus Run() override;
};