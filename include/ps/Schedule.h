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
			vector<BufferHandle> n1in = { ToBufferHandle(0,0),ToBufferHandle(1,0),ToBufferHandle(2,0) };
			vector<BufferHandle> n1out = { ToBufferHandle(0,0),ToBufferHandle(1,0),ToBufferHandle(2,0) };
			auto n1Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n1in, n1out);
			auto n1Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n1 = make_shared<ScheduleNode>(n1Ctx, n1Opt);
			s.Components.push_back(n1);

			vector<BufferHandle> n2in = { ToBufferHandle(3,0),ToBufferHandle(4,0),ToBufferHandle(5,0) };
			vector<BufferHandle> n2out = { ToBufferHandle(3,0),ToBufferHandle(4,0),ToBufferHandle(5,0) };
			auto n2Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n2in, n1out);
			auto n2Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n2 = make_shared<ScheduleNode>(n2Ctx, n2Opt);
			s.Components.push_back(n2);

			//we now sycnhronize p0 and p3
			vector<BufferHandle> n3in = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			vector<BufferHandle> n3out = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			auto n3Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n3in, n3out);
			auto n3Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n3 = make_shared<ScheduleNode>(n3Ctx, n3Opt);
			s.Components.push_back(n3);

			//we now perform an optimization step on p0 and p3

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



