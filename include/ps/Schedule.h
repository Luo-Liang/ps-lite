#pragma once
#include "Operator.h"
#include "OperatorContext.h"
#include "internal/ext.h"
#include <memory>
#include "PLink.h"
#include <unordered_set>
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
	//double EstimatedCost; //? for a local operator the cost is 0.
	//cost estimationi s moved to PLinkSim
	vector<shared_ptr<ScheduleNode>> Upstream;
	vector<shared_ptr<ScheduleNode>> Downstream;
	uint ID = 0;
	int UnresolvedDependencies;
	bool Finished;
	ScheduleNode(shared_ptr<OperatorContext> s_pContext,
		shared_ptr<IOperator> s_pOperator, NodeId runsOn)
	{
		pContext = s_pContext;
		pOperator = s_pOperator;
		RunOn = runsOn;
	}
	void* Annotation;
};
class Schedule
{
private:
	bool comparePtrToNode(shared_ptr<ScheduleNode> a, shared_ptr<ScheduleNode> b) { return (a->ID < b->ID); }
	bool VerifySubschedule(vector<shared_ptr<ScheduleNode>> roots)
	{
		//first, verify there is only one connected component for each node
	}
	void ResetDependency()
	{
		for (var& component : Components)
		{
			component->UnresolvedDependencies = component->Downstream.size();
		}
	}
	bool verifyGlobalSchedule(string& ResultStr)
	{
		//basically run a simulated execution and make sure every piece of data 
		//is aggregated and optimized at the end for each node.

		unordered_map<NodeId, int> counter;
		unordered_map<tuple<NodeId, NodeId>, int> viewOn;
		//each machine must have a root.
		//send and receive has an implicit dependency edge that is not reflected in the 
		//dependency graph		var roots = Roots();
		const int UNSET = -1;
		std::queue<shared_ptr<ScheduleNode>> ReadyQ;
		var roots = Roots();
		for (var pSN : roots)
		{
			counter[pSN->RunOn] = 1;
			//everyone's view on other people is uninitialized.
			for (var other : roots)
			{
				if (other->RunOn != pSN->RunOn)
				{
					viewOn[tuple<NodeId, NodeId>(pSN->RunOn, other->RunOn)] = UNSET;
				}
			}
			ReadyQ.push(pSN);
		}
		//everyone's counter starts with 0.
		int optimizationCounts = 0;
		unordered_map<OpID, int> opCounter;
		while (ReadyQ.size() != 0)
		{
			var current = ReadyQ.front();
			ReadyQ.pop();
			bool canFinish = true;
			switch (current->pOperator->Type)
			{
				case OperatorType::PHubOptimizer:
				{
					if (optimizationCounts != 0)
					{
						//optimized too many times.
						ResultStr = CxxxxStringFormat("Too many optimization counts. NODE=%d", current->RunOn);
						return false;
					}
					else if (counter.at(current->RunOn) != counter.size())
					{
						ResultStr = CxxxxStringFormat("Current counter is incorrect for optimization. CURRENT=%d, COUNTER=%d, EXPECTED=%d",
							current->RunOn,
							counter.at(current->RunOn),
							counter.size());
						return false;
					}
					optimizationCounts = 1;
				}
				case OperatorType::PHubAggregator:
				{
					var pContext = dynamic_pointer_cast<PHubOperatorContext>(current->pContext);
					var me = current->RunOn;
					for (var& remote : pContext->From)
					{
						//var remote = pContext->From;
						//my view on remote:
						var cntr = viewOn.at(tuple<NodeId, NodeId>(me, remote));
						if (cntr == UNSET)
						{
							ResultStr = CxxxxStringFormat("Remote has not initialized the current viewOn. CURRENT=%d, CNTR=%d", current->RunOn, cntr);
							return false;
						}
						counter.at(current->RunOn) += cntr;
					}
					//add to me.
				}
				case OperatorType::PHubBroadcast:
				{
					//if i am a reciever, my counter is overwritten.
					var pContext = dynamic_pointer_cast<PHubOperatorContext>(current->pContext);
					var isReceiver = find(pContext->To.begin(),
						pContext->To.end(),
						current->RunOn) != pContext->To.end();

					var sender = pContext->From.at(0);
					if (isReceiver)
					{
						if (opCounter[pContext->OPID] == 0)
						{
							//pop, but needs to revisit again.
							canFinish = false;
						}
						else
						{
							//upate view on, not the actual merge buffer --- that's the job of aggregate
							viewOn.at(tuple<NodeId, NodeId>(current->RunOn, sender)) = counter.at(sender);
						}
					}
					else
					{
						opCounter[pContext->OPID]++;
					}
					//if i am a sender, not really need to do anything, except for incrementing counter.
				}
				case OperatorType::GlooCollectiveAlgorithm:
				{
					//who are the particpants?
					//the result is the sum of everyone's counter.
					var pContext = dynamic_pointer_cast<GlooContext>(current->pContext);
					if (opCounter[pContext->OPID] == pContext->From.size())
					{
						for (var id : pContext->From)
						{
							if (id != current->RunOn)
							{
								//only update mine. 
								//there are many such nodes that will update others' values.
								counter.at(current->RunOn) += counter.at(id);
							}
						}
					}
					else
					{
						opCounter[pContext->OPID]++;
						canFinish = false;
					}
				}
				default:
				{
					CHECK(false) << "cannot verifiy ";
				}
			}
			current->Finished = true;
			//what are possible next operators?
			//my children, that are ready, should be queued.
			if (canFinish)
			{
				for (shared_ptr<ScheduleNode> pSN : current->Downstream)
				{
					CHECK(pSN->Finished == false);
					pSN->UnresolvedDependencies -= 1;
					if (pSN->UnresolvedDependencies == 0)
					{
						//queue it!
						ReadyQ.push(pSN);
					}
				}
			}
			else
			{
				//revisit this node.
				ReadyQ.push(current);
			}
		}

		//at the end, make sure everyone has counter = -1 * count.
		for (var& pair : counter)
		{
			if (pair.second != counter.size() || optimizationCounts != 1)
			{
				ResultStr = CxxxxStringFormat("Found incorrect final value. CURRENT=%d, VALUE=%d, EXPECTED=%d", pair.first, pair.second, counter.size());
				return false;
			}
		}
		return true;//check done.
	}


public:
	static Schedule DebugGenerateGlobalSchedule(std::string desc);
	//a schedule is how you want to express this?
	//should be able to represent as a global schedule and local schedule (a series of API calls)
	//A list of components that can be scheduled immediately.
	vector<shared_ptr<ScheduleNode>> Components;

	vector<shared_ptr<ScheduleNode>> Roots()
	{
		vector<shared_ptr<ScheduleNode>> results;
		auto it = std::copy_if(Components.begin(), Components.end(), std::back_inserter(results), [](shared_ptr<ScheduleNode> pSN) {return pSN->UpStream.size() == 0 });
		return results;
	}

	//returns a topoligically sorted schedule that is targeted at NodeId
	//returns the root node.
	//returns the roots of ONE contiguous subgraph that belongs to currentID.
	//the connection will be severed
	vector<shared_ptr<ScheduleNode>> TrimTo(NodeId currentID)
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

		return results;
	}

	bool VerifyGlobalSchedule(string& ResultStr)
	{
		var result = verifyGlobalSchedule(ResultStr);
		ResetDependency();
		return result;
	}


};



