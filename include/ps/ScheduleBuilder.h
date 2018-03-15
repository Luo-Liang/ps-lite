#pragma once
#include <string.h>
#include <ps/Schedule.h>
#include <vector>
#include <ps/PLink.h>
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
			var ctx = make_shared<GlooContext>(participants, participants, rank, participants.size());
			results.push_back(make_shared<ScheduleNode>(ctx, ops, participants.at(rank)));
		}
		return results;
	}

	void IntelligentZip(vector<ScheduleNode>& upstream, vector<ScheduleNode>& downstream)
	{
		CHECK(upstream.size() > 0);
		CHECK(downstream.size() > 0);
		//the operators must be homogeneous
		//CHECK(upstream.size() == downstream.size());
		//check that every node is the same operator in upstream.
		//check that every node is the same operator in downstream.
		for (Cntr i = 0; i < upstream.size(); i++)
		{
			CHECK(upstream.at(i).pOperator->Type == upstream.at(0).pOperator->Type);
			CHECK(downstream.at(i).pOperator->Type == downstream.at(0).pOperator->Type);
		}

		//now assign upstream and downstreams accordingly, using RunAt and from, tos.
		//only connecting send/recv using from tos, others should all be using RunAt.

		var upstreamType = upstream.at(0).pOperator->Type;
		switch (upstreamType)
		{
			case OperatorType::PHubBroadcast:
			{
				//i can infer downstream type to be either recv nodes or other arbitrary nodes.
				var ctx0 = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(0).pContext);
				//check everyone is send or is all receive.
				var isSender0 = find(ctx0->From.begin(), ctx0->From.end(), upstream.at(0).RunOn) != ctx0->From.end();
				for (Cntr i = 0; i < upstream.size(); i++)
				{
					var ctx = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(i).pContext);
					var isSender = find(ctx->From.begin(), ctx->From.end(), upstream.at(i).RunOn) != ctx->From.end();
					CHECK(isSender == isSender0);
				}
				//how i zip downstream dependes on whether upstreams are senders.
				if (isSender0)
				{
					//upstream are broadcast senders.
					//downstreams must be broadcast receivers.
					//check that.
					var dctx0 = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(downstream.at(0).pContext);
					for (Cntr i = 0; i < downstream.size(); i++)
					{
						var ctx = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(downstream.at(i).pContext);
						var disRecvr = find(ctx->From.begin(), ctx->From.end(), downstream.at(i)->RunOn) == ctx->From.end();
					}
				}
				else
				{

				}
			}
		}
	}
}
};