#pragma once
#include "phub.h"
#include "OperatorContext.h"
#include <gloo/algorithm.h>
#include <gloo/allreduce_halving_doubling.h>

template <class T>
class PHubOptimizer;
class PHubBroadcast;

enum OperationStatus
{
	Finished,
	QueuedForExecution
};
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
	virtual OperationStatus Run() = 0;
	string Name;
	OperatorType Type;
	int Sequence = -1;
	bool Participate = false;
	bool Initialized = false;
	virtual std::string GetUniqueName()
	{
		return std::to_string(Sequence) + ":" + Name;
	}
	virtual void Initialize(shared_ptr<OperatorContext> context) = 0;
};

template <class T>
class GlooAlgorithms : IOperator
{
	shared_ptr<gloo::Algorithm> reducer = NULL;
public:
	virtual void Initialize(OperatorContext* context) override
	{
		LocallyAvailableOperatorContext<T>* pctx = (LocallyAvailableOperatorContext<T>*)(context);
		CHECK(pctx->typeCode == OperatorContext::OperatorContextTypeCode::LocallyAvailable);
		//check each inpuit lens is identical.
		CHECK(pctx->inputAddrs.size() > 0);
		CHECK(pctx->inputAddrs.size() == pctx.inputLens.size());
		//CHECK(std::adjacent_find(pctx->inputLens.begin(), pctx->inputLens.end(), std::not_equal_to<int>()) == pctx->inputLens.end()) << "lens of elements are different!";
	}

	virtual OperationStatus Run() override
	{
		CHECK(Initialized);
		reducer->run();
	}
};

template <class T>
class GlooHalvingAndDoubling : public GlooAlgorithms<T>
{
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		GlooAlgorithms<T>::Initialize(context);
		LocallyAvailableOperatorContext<T>* pctx = (LocallyAvailableOperatorContext<T>*)(context);
		reducer = make_shared<gloo::AllreduceHalvingDoubling<T>>((gloo::Context*)pctx->additionalContext, pctx->inputAddrs, pctx->inputLens.at(0));
	}
};

template <class T>
class GlooRingChunked : public GlooAlgorithms<T>
{
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		GlooAlgorithms<T>::Initialize(context);
		LocallyAvailableOperatorContext<T>* pctx = (LocallyAvailableOperatorContext<T>*)(context);
		reducer = make_shared<gloo::AllreduceRingChunked<T>>((gloo::Context*)pctx->additionalContext, pctx->inputAddrs, pctx->inputLens.at(0));
	}
};
 
