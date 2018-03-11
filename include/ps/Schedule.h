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
	double EstimatedCost; //? for a local operator the cost is 0.
	vector<shared_ptr<ScheduleNode>> Upstream;
	vector<shared_ptr<ScheduleNode>> Downstream;
	uint ID = 0;
	int UnresolvedDependencies;
	bool Finished;
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
	bool VerifySubschedule(vector<shared_ptr<ScheduleNode>> roots)
	{
		//first, verify there is only one connected component for each node
	}

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
				auto n1Ctx = make_shared<GlooContext>(n1InOut, n1InOut, i - 1, 4);
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

			//we now perform an optimization step on p1 and p5
			vector<BufferHandle> n41InOut = { 0 };
			auto n41Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n41InOut, n41InOut);
			auto n41Opt = make_shared<PHubOptimizer>();
			auto n41 = make_shared<ScheduleNode>(n41Ctx, n41Opt);
			n41->Annotation = NULL;
			n41Opt->numAggregated = (size_t)n31->Annotation;
			n41.RunOn = 1;
			s.Components.push_back(n41);

			//perform optimization step on p5.
			vector<BufferHandle> n42InOut = { 0 };
			auto n42Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n42InOut, n42InOut);
			auto n42Opt = make_shared<PHubOptimizer>();
			auto n42 = make_shared<ScheduleNode>(n42Ctx, n42Opt);
			n42->Annotation = NULL;
			n42Opt->numAggregated = (size_t)n31->Annotation;
			n42.RunOn = 1;
			s.Components.push_back(n42);


			//now broadcast back 2-4, 6-8 from 1 and 5
			for (int i = 1; i <= 5; i++)
			{
				vector<BufferHandle> n51InOut = { 0 };
				auto n51Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n51InOut, n51InOut);
				auto n51Opt = make_shared<PHubBroadcast>();
				n51Opt->SetReciever(i != 1);
				auto n51 = make_shared<ScheduleNode>(n51Ctx, n51Opt);
				n51->Annotation = NULL;
				n51->RunOn = i;
				s.Components.push_back(n51);
			}

			//now broadcast back to p5 p6
			for (int i = 5; i <= 8; i++)
			{
				vector<BufferHandle> n52InOut = { 0 };
				auto n52Ctx = make_shared<LocallyAvailableOperatorContext<float>>(n52InOut, n52InOut);
				auto n52Opt = make_shared<PHubBroadcast>();
				n52Opt->SetReciever(i != 5);
				auto n52 = make_shared<ScheduleNode>(n52Ctx, n52Opt);
				n52->Annotation = NULL;
				n52->RunOn = i;
				s.Components.push_back(n52);
			}
			//we are done.
			//set up dependency.
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

		while (reachiability.size() != 0)
		{
			var curr = reachiability.front();
			reachiability.pop();
			//make sure only one path to a certain node from root, aka forests.
			vector<shared_ptr<ScheduleNode>> toTrim;
			for (shared_ptr<ScheduleNode> down : curr->Downstream)
			{
				//safe. previous level must have finsihed before next level start.
				down->Level = curr->Level + 1;
				if (down->RunOn != currentID)
				{
					//this whole subtree is removed.
					//this is not necessary if we're dealing with implicit dependency induced from collectives.
					toTrim.push_back(down);
					CHECK(false) << "what is this";
				}
				reachiability.push(down);
			}
		}
		//roots are returned.
		return results;
	}


	bool VerifyGlobalSchedule(string& ResultStr)
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
		while (ReadyQ.size() != 0)
		{
			var current = ReadyQ.front();
			ReadyQ.pop();
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
					var remote = pContext->From;
					//my view on remote:
					var cntr = viewOn.at(tuple<NodeId, NodeId>(me, remote));
					if (cntr == UNSET)
					{
						ResultStr = CxxxxStringFormat("Remote has not initialized the current viewOn. CURRENT=%d, CNTR=%d", current->RunOn, cntr);
						return false;
					}
					counter.at(current->RunOn) += cntr;
					//add to me.
				}
				case OperatorType::PHubBroadcast:
				{
					//if i am a reciever, my counter is overwritten.
					var pContext = dynamic_pointer_cast<PHubOperatorContext>(current->pContext);
					var isReceiver = pContext->To == current->RunOn;
					var sender = pContext->From;
					if (isReceiver)
					{
						//upate view on, not the actual merge buffer --- that's the job of aggregate
						viewOn.at(tuple<NodeId, NodeId>(pContext->To, pContext->From)) = counter.at(sender);
					}
					//if i am a sender, not really need to do anything
				}
				case OperatorType::GlooCollectiveAlgorithm:
				{
					//who are the particpants?
					//the result is the sum of everyone's counter.
					var pContext = dynamic_pointer_cast<GlooContext>(current->pContext);
					for (var id : pContext->Peers)
					{
						if (id != current->RunOn)
						{
							//only update mine. 
							//there are many such nodes that will update others' values.
							counter.at(current->RunOn) += counter.at(id);
						}
					}
				}
			}
			current->Finished = true;
			//what are possible next operators?
			//my children, that are ready, should be queued.
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


};



