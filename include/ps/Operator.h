#pragma once
#include "phub.h"
#include "OperatorContext.h"
#include <gloo/algorithm.h>
#include <gloo/allreduce_halving_doubling.h>

typedef int OperatorHandle;
using namespace std;
enum OperatorType
{
	PHubDataMovement,
	//this gives the operator context's additional context a gloo context.
	GlooCollectiveAlgorithm,
	PHubDataManipulation
};
class IOperator
{
public:
	 virtual OperatorHandle RunAsync(OperatorContext* context) = 0;
	 string Name;
	 OperatorType Type;
	 int Sequence = -1;
	 bool Participate = false;
	 virtual std::string GetUniqueName()
	 {
		 return std::to_string(Sequence) + ":" + Name;
	 }
};

template <class T>
class GlooAllReduceHalvingAndDoubling : IOperator
{
	shared_ptr<gloo::AllreduceHalvingDoubling<T>> reducer;
public:
	virtual OperatorHandle RunAsync(OperatorContext* context) override
	{
		var pctx = LocallyAvailableOperatorContext<T>*(context);
		
		reducer = make_shared<gloo::AllreduceHalvingDoubling<T>>((gloo::rendezvous::Context*)context->additionalContext.get(), p  , );
	}
};