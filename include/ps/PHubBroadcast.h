#pragma once
#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
using namespace std;

class PHubBroadcast : IOperator
{
	bool isReceiver = true;
	shared_ptr<PHub> pPhub = NULL;
	PLinkKey key;
	size_t keySize;
	shared_ptr<OperatorContext> opContext;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override;
	void SetReciever(bool receiver);
	virtual OperationStatus Run() override;
};