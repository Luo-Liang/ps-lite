#pragma once
#include <string.h>
#include <ps/Schedule.h>
#include <vector>
#include <ps/PLink.h>
#include <ps/phubb>
class ScheduleBuilder
{
	PLinkKey key;
public:
	vector<shared_ptr<ScheduleNode>> CreateCollectives(vector<NodeId>& participants)
	{
		vector<shared_ptr<ScheduleNode>> results;
		for (Cntr rank = 0; rank < participants.size(); rank++)
		{
			var ops = make_shared<GlooHalvingAndDoubling<float>>();
			var ctx = make_shared<GlooContext>(participants, participants, rank, participants.size(), key);
			results.push_back(make_shared<ScheduleNode>(ctx, ops, participants.at(rank)));
		}
		return results;
	}

	void IntelligentZip(vector<shared_ptr<ScheduleNode>>& upstream, vector<shared_ptr<ScheduleNode>>& downstream)
	{
		CHECK(upstream.size() > 0);
		CHECK(downstream.size() > 0);
		//the operators must be homogeneous
		//CHECK(upstream.size() == downstream.size());
		//check that every node is the same operator in upstream.
		//check that every node is the same operator in downstream.
		for (Cntr i = 0; i < upstream.size(); i++)
		{
			CHECK(upstream.at(i)->pOperator->Type == upstream.at(0).pOperator->Type);
			CHECK(downstream.at(i)->pOperator->Type == downstream.at(0).pOperator->Type);
		}

		//now assign upstream and downstreams accordingly, using RunAt and from, tos.
		//only connecting send/recv using from tos, others should all be using RunAt.

		var upstreamType = upstream.at(0)->pOperator->Type;
		//i am a sender?
		var upstreamSender = find(upstream.at(0)->pContext->From.begin(),
			upstream.at(0)->pContext->From.end(),
			upstream.at(0)->RunOn) != upstream.at(0)->pContext->From.end();
		if (upstreamType == OperatorType::PHubBroadcastOp && upstreamSender)
		{
			//i can infer downstream type to be either recv nodes or other arbitrary nodes.
			var ctx0 = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(0)->pContext);
			//check everyone is send or is all receive.
			//var isSender0 = find(ctx0->From.begin(), ctx0->From.end(), upstream.at(0)->RunOn) != ctx0->From.end();
			for (Cntr i = 0; i < upstream.size(); i++)
			{
				var ctx = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(i)->pContext);
				var isSender = find(ctx->From.begin(), ctx->From.end(), upstream.at(i)->RunOn) != ctx->From.end();
				CHECK(isSender == true);
			}
			//how i zip downstream dependes on whether upstreams are senders.
				//upstream are broadcast senders.
				//downstreams must be broadcast receivers.
				//check that.
			CHECK(downstream.at(0)->pOperator->Type == OperatorType::PHubBroadcastOp);
			var dctx0 = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(downstream.at(0)->pContext);
			for (Cntr i = 0; i < downstream.size(); i++)
			{
				var ctx = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(downstream.at(i)->pContext);
				var disRecvr = find(ctx->From.begin(), ctx->From.end(), downstream.at(i)->RunOn) == ctx->From.end();
				CHECK(disRecvr);
			}
			//stitch each upstream to a downstream.
			//if multiple broadcast happening simultaneously, 
			//make multiple stitches.
			//unordered_set<shared_ptr<ScheduleNode>> usedDstream;
			for (Cntr i = 0; i < upstream.size(); i++)
			{
				var& current = upstream.at(i);
				for (var& child : current->Downstream)
				{
					//CHECK(usedDstream.find(child) == usedDstream.end());
					//first find what i am connected to.
					if (find(downstream.begin(), downstream.end(), child->RunOn) != downstream.end())
					{
						//check child is not assigned to anyone.
						CHECK(child->Upstream.size() == 0);
						child->Upstream.push_back(current);
						current->Downstream.push_back(child);
					}
					//others dont care
				}
			}
		}
		else
		{
			//i am receievr, so my downstream is solely determined by the runon --- whoever has the same RunOn with me in downstream,
			//i get to connect with them.
			for (Cntr i = 0; i < upstream.size(); i++)
			{
				var& current = upstream.at(i);
				for (var& child : current->Downstream)
				{
					if (child->RunOn == current->RunOn)
					{
						child->Upstream.push_back(current);
						current->Downstream.push_back(child);
					}
				}
			}
		}
	}
	vector<shared_ptr<ScheduleNode>> CreatePHubBroadcast(NodeId from, vector<NodeId>& To)
	{
		vector<shared_ptr<ScheduleNode>> results;
		var fromNodeOps = make_shared<PHubBroadcastOp>();
		vector<NodeId> fromVec = { from };
		var fromNodeCtx = make_shared<PHubOperatorContext>(fromVec, To, key);
		var fromNode = make_shared<ScheduleNode>(fromNodeCtx, fromNodeOps, from);
		for (Cntr i = 0; i < To.size(); i++)
		{
			var childRunOn = To.at(i);
			var childOps = make_shared<PHubBroadcastOp>();
			var childCtx = make_shared<PHubOperatorContext>(from, To, key);
			var childNode = make_shared<ScheduleNode>(childCtx, childOps, childRunOn);
			childNode->Upstream.push_back(fromNode);
			fromNode->Downstream.push_back(childNode);
			results.push_back(childNode);
		}
		return results;
	}
};