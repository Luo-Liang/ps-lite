#pragma once
#include "Operator.h"
#include "OperatorContext.h"
#include "internal/ext.h"
#include <memory>
#include "phub.h"
//a schedule is simply a graph of operators and its nodes
class Schedule;

class ScheduleNode
{
	friend class Schedule;
private:
	int Level = -1;
public:
	shared_ptr<Schedule> Schedule;
	shared_ptr<OperatorContext> pContext;
	shared_ptr<IOperator> pOperator;
	NodeId RunOn;
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
			//1 2 3 4,  5 6 7 8 on different racks.
			//the schedule uses collectives to synchronize 1 2 3 4, then 5 6 7 8, then 1 and 5, then 

			//first step, collective within a single rack.
			//these run on 4 nodes.
			for (int i = 1; i <= 4; i++)
			{
				vector<PLinkKey> n1InOut = { 0 };
				auto n1Ctx = make_shared<GlooContext<float>>(n1InOut, n1InOut, i - 1, 4);
				auto n1Opt = make_shared<GlooHalvingAndDoubling<float>>();
				auto n1 = make_shared<ScheduleNode>(n1Ctx, n1Opt);
				//needs fully expanded nodes. do not abbreviate.
				n1->RunOn = i;
				s.Components.push_back(n1);
			}

			//these run on other 4 nodes, 5-8
			for (int i = 1; i <= 4; i++)
			{
				vector<PLinkKey> n2InOut = { 0 };
				auto n2Ctx = make_shared<GlooContext<float>>(n2InOut, n2InOut, i - 1, 4);
				auto n2Opt = make_shared<GlooHalvingAndDoubling<float>>();
				auto n2 = make_shared<ScheduleNode>(n2Ctx, n2Opt);
				n2->RunOn = i + 4;
				s.Components.push_back(n2);
			}

			//we now sycnhronize 1
			vector<PLinkKey> n3InOut1 = { 0 };
			//vector<BufferHandle> n3out = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			auto n31Ctx = make_shared<GlooContext<float>>(n3InOut1, n3InOut1, 0, 2);
			auto n31Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n31 = make_shared<ScheduleNode>(n31Ctx, n31Opt);
			n31->Annotation = (void*)8;
			n31->RunOn = 1;
			s.Components.push_back(n31);

			//we now sycnhronize 5.
			vector<PLinkKey> n3InOut2 = { 0 };
			//vector<BufferHandle> n3out = { ToBufferHandle(0,0),ToBufferHandle(3,0) };
			auto n32Ctx = make_shared<GlooContext<float>>(n3InOut2, n3InOut2, 1, 2);
			auto n32Opt = make_shared<GlooHalvingAndDoubling<float>>();
			auto n32 = make_shared<ScheduleNode>(n32Ctx, n32Opt);
			n32->Annotation = (void*)8;
			n32->RunOn = 5;
			s.Components.push_back(n32);

			//we now perform an optimization step on p0 and p3
			vector<BufferHandle> n4InOut = { 0 };
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
	vector<shared_ptr<ScheduleNode>> Filter(NodeId currentID)
	{
		//first, get root.
		vector<shared_ptr<ScheduleNode>> results;
		queue<shared_ptr<ScheduleNode>> reachiability;
		for (var& component : Components)
		{
			if (component->Upstream.size() == 0)
			{
				component->Level = 0;
				reachiability.push(component);
				if (component->RunOn == currentID)
				{
					results.push_back(component);
				}
			}
		}

		while (reachiability.size() != 0)
		{
			var curr = reachiability.front();
			reachiability.pop();
			//make sure only one path to a certain node from root, aka forests.
			for (shared_ptr<ScheduleNode> down : curr->Downstream)
			{
				CHECK(down->Level == -1);
				down->Level = curr->Level + 1;
				if (down->RunOn == currentID)
				{
					results.push_back(down);
				}
				reachiability.push(down);
			}
		}

		return results;
	}
};



