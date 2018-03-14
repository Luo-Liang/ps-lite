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
		//the operators must be homogeneous
		CHECK(upstream.size() == downstream.size());
		//check that every node is the same operator in upstream.
		//check that every node is the same operator in downstream.
		for (Cntr i = 0; i < upstream.size(); i++)
		{
			CHECK(upstream.at(i).pOperator->Type == upstream.at(0).pOperator->Type);
			CHECK(downstream.at(i).pOperator->Type == downstream.at(0).pOperator->Type);
		}

		//now assign upstream and downstreams accordingly, using RunAt and from, tos.
		//only connecting send/recv using from tos, others should all be using RunAt.
		for (Cntr i = 0; i < upstream.size(); i++)
		{
			var upstreamType = upstream.at(0).pOperator->Type;
			//i can infer downstream type to be either recv nodes or other arbitrary nodes.
			switch (upstreamType)
			{
				case OperatorType::PHubBroadcast:
				{
					//check everyone is send or is all receive.
					for (Cntr i = 0; i < upstream.size(); i++)
					{
						var ctx = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(i).pContext);
						var ctx0 = (shared_ptr<PHubOperatorContext>)dynamic_pointer_cast<PHubOperatorContext>(upstream.at(0).pContext);

						ctx->
					}
				}
			}
		}
	}
};