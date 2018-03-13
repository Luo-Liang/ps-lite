#pragma once
#include "phub.h"
#include "OperatorContext.h"
#include <gloo/algorithm.h>
#include <gloo/allreduce_halving_doubling.h>

class PHubOptimizer;
class PHubBroadcast;

enum OperationStatus
{
	Untouched,
	Finished,
	QueuedForExecution,
	//skipped executions are the ones belonging to other nodes.
	//they are in my schedule so we can lazily evaluate dependencies.
	Skipped
};
using namespace std;
enum OperatorType
{
	UNSET,
	PHubBroadcast,
	PHubAggregator,
	//PHubGather,
	PHubOptimizer,
	//this gives the operator context's additional context a gloo context.
	GlooCollectiveAlgorithm,
};
class IOperator
{
public:
	virtual OperationStatus Run() = 0;
	string Name;
	OperatorType Type = UNSET;
	int Sequence = -1;
	bool Participate = false;
	bool Initialized = false;
	virtual std::string GetUniqueName()
	{
		return std::to_string(Sequence) + ":" + Name;
	}
	virtual void Initialize(shared_ptr<OperatorContext> context) = 0;
};

class GlooAlgorithms : IOperator
{
	shared_ptr<gloo::Algorithm> reducer = NULL;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		Type = OperatorType::GlooCollectiveAlgorithm;
		auto pctx = dynamic_pointer_cast<GlooContext>(context);
		//CHECK(pctx->typeCode == OperatorContext::OperatorContextTypeCode::LocallyAvailable);
		//check each inpuit lens is identical.
		//nothing needs to be done.
		//CHECK(std::adjacent_find(pctx->inputLens.begin(), pctx->inputLens.end(), std::not_equal_to<int>()) == pctx->inputLens.end()) << "lens of elements are different!";
	}

	virtual OperationStatus Run() override
	{
		reducer->run();
	}
};

template <class T>
class GlooHalvingAndDoubling : public GlooAlgorithms
{
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		GlooAlgorithms<T>::Initialize(context);
		var pctx = dynamic_pointer_cast<GlooContext>(context);
		reducer = make_shared<gloo::AllreduceHalvingDoubling<T>>((gloo::Context*)pctx->additionalContext, pctx->inputAddrs, pctx->inputLens.at(0));
	}
};

template <class T>
class GlooRingChunked : public GlooAlgorithms
{
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		GlooAlgorithms<T>::Initialize(context);
		LocallyAvailableOperatorContext<T>* pctx = (LocallyAvailableOperatorContext<T>*)(context);
		reducer = make_shared<gloo::AllreduceRingChunked<T>>((gloo::Context*)pctx->additionalContext, pctx->inputAddrs, pctx->inputLens.at(0));
	}
};

