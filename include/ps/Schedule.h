#pragma once
#include "Operator.h"
#include "OperatorContext.h"
#include "internal/ext.h"
#include <memory>
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

};
class Schedule
{
private:
	bool comparePtrToNode(shared_ptr<ScheduleNode> a, shared_ptr<ScheduleNode> b) { return (a->ID < b->ID); }

public:
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



