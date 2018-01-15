#pragma once
#include "Operator.h"
#include "OperatorContext.h"
#include "internal/ext.h"
#include <memory>
#include "phub.h"
//a schedule is simply a graph of operators and its nodes
class ScheduleNode
{
public:
	shared_ptr<OperatorContext> pContext;
	shared_ptr<IOperator> pOperator;
	vector<NodeId> RunOn;
	double EstimatedCost; //? for a local operator the cost is 0.
	vector<shared_ptr<ScheduleNode>> Upstream;
	vector<shared_ptr<ScheduleNode>> Downstream;
	uint ID = 0;
	ScheduleNode(shared_ptr<OperatorContext> s_pContext,
		shared_ptr<IOperator> s_pOperator)
	{
		pContext = s_pContext;
		pOperator = s_pOperator;
	}
	void* Annotation;
};
class Schedule
{
private:
	bool comparePtrToNode(shared_ptr<ScheduleNode> a, shared_ptr<ScheduleNode> b) { return (a->ID < b->ID); }

public:
	static Schedule DebugGenerateGlobalSchedule(std::string desc)
	{
		Schedule s;
		if (desc == "collectivescross2racks")
		{
			//assuming 6 machines.
			//1 2 3,  4 5 6 on different racks.
			//the schedule uses collectives to synchronize 1 2 3, then 4 5 6, then 1 and 4, then 
			vector<BufferHandle> n1InOut = { ToBufferHandle(0,0) };
			auto n1Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n1InOut, n1InOut);
			auto n1Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n1 = make_shared<ScheduleNode>(n1Ctx, n1Opt);
			n1->Annotation = (void*)3;
			n1->RunOn = { 1,2,3 };
			s.Components.push_back(n1);

			vector<BufferHandle> n2InOut = { ToBufferHandle(0,0) };
			auto n2Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n2InOut, n2InOut);
			auto n2Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n2 = make_shared<ScheduleNode>(n2Ctx, n2Opt);
			n2->Annotation = (void*)3;
			n2->RunOn = { 3,4,5 };
			s.Components.push_back(n2);

			//we now sycnhronize p0 and p3
			vector<BufferHandle> n3InOut = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			//vector<BufferHandle> n3out = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			auto n3Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n3InOut, n3InOut);
			auto n3Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n3 = make_shared<ScheduleNode>(n3Ctx, n3Opt);
			n3->Annotation = (void*)6;
			n3->RunOn = { 1,4 };
			s.Components.push_back(n3);

			//we now perform an optimization step on p0 and p3
			vector<BufferHandle> n4InOut = { ToBufferHandle(0,0) };
			auto n4Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n4InOut, n4InOut);
			auto n4Opt = make_shared<PHubOptimizer>();
			auto n4 = make_shared<ScheduleNode>(n4Ctx, n4Opt);
			n4.Annotation = NULL;
			n4Opt->numAggregated = (size_t)n3->Annotation;
			n4.RunOn = { 1,4 };
			s.Components.push_back(n4);


			//now broadcast back to p1 p2
			vector<BufferHandle> n5In = { ToBufferHandle(0,0) };
			vector<BufferHandle> n5Out = { ToBufferHandle(1,0), ToBufferHandle(2,0) };
			auto n5Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n5In, n5Out);
			auto n5Opt = make_shared<PHubBroadcast>();
			auto n5 = make_shared<ScheduleNode>(n5Ctx, n5Opt);
			n5.Annotation = NULL;
			n5.RunOn = { 1,2,3 };

			//now broadcast back to p5 p6
			vector<BufferHandle> n6In = { ToBufferHandle(4,0) };
			vector<BufferHandle> n6Out = { ToBufferHandle(5,0), ToBufferHandle(6,0) };
			auto n6Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n6In, n6Out);
			auto n6Opt = make_shared<PHubBroadCast>();
			auto n6 = make_shared<ScheduleNode>(n6Ctx, n6Opt);
			n6.Annotation = NULL;
			n6.RunOn = { 4,5,6 };

			//we are done.
			//set up dependency.

			n1->Downstream.push_back(n3);
			n2->Downstream.push_back(n3);
			n3->Upstream = { n2,n1 };
			n3->Downstream.push_back(n4);
			n4->Upstream.push(n3);
			n4->Downstream = { n5,n6 };
			n5->Upstream.push_back(n4);
			n6->Upstream.push_back(n5);


		}
		else if (desc == "hybridcross2racks")
		{
		}
		return s;
	}
	//a schedule is how you want to express this?
	//should be able to represent as a global schedule and local schedule (a series of API calls)
	//A list of components that can be scheduled immediately.
	vector<shared_ptr<ScheduleNode>> Components;

	//returns a topoligically sorted schedule that is targeted at NodeId
	shared_ptr<Schedule> Filter(NodeId currentID)
	{
		std::sort(Components.begin(), Components.end(), comparePtrToNode);
		var ret = make_shared<Schedule>();
		for (var ptr : Components)
		{
			if (std::find(ptr->RunOn.begin(), ptr->RunOn.end(), currentID) != ptr->RunOn.end())
			{
				ret->Components.push_back(ptr);
			}
		}
		return ret;
	}
};



