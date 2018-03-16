#include <ps/ScheduleBuilder.h>
ScheduleBuilder::ScheduleBuilder(PLinkKey k)
{
	key = k;
}
vector<shared_ptr<ScheduleNode>> ScheduleBuilder::CreateCollectives(vector<NodeId>& participants)
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
vector<shared_ptr<ScheduleNode>> ScheduleBuilder::IntelligentZip(vector<shared_ptr<ScheduleNode>>& upstream, vector<shared_ptr<ScheduleNode>>& downstream)
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
	if (upstreamType == OperatorType::PHubBroadcast && upstreamSender)
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
		CHECK(downstream.at(0)->pOperator->Type == OperatorType::PHubBroadcast);
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
		//if no downstream run on, we don not connect.
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
	//you should return a list of leaf nodes.
	vector<shared_ptr<ScheduleNode>> results;
	for (var& node : upstream)
	{
		if (node->Downstream.size() == 0)
		{
			results.push_back(node);
		}
	}
	for (var& node : downstream)
	{
		if (node->Downstream.size() == 0)
		{
			results.push_back(node);
		}
	}
	return results;
}

vector<shared_ptr<ScheduleNode>> ScheduleBuilder::CreatePHubBroadcast(NodeId from, vector<NodeId>& To)
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
	//use results.at(0).UpStream to retrieve the sender node.
	return results;
}

vector<shared_ptr<ScheduleNode>> ScheduleBuilder::CreatePHubAggregatorInferredFromPHubBroadcastRecv(vector<shared_ptr<ScheduleNode>> nodes, vector<NodeId>& runOns)
{
	vector<shared_ptr<ScheduleNode>> results;
	for (var& node : nodes)
	{
		if (node->pOperator->Type == OperatorType::PHubBroadcast)
		{
			var isRecv = find(node->pContext->To.begin(),
				node->pContext->To.end(),
				node->RunOn) != node->pContext->To.end();
			CHECK(node->pContext->From.size() == 1);
			if (runOns.size() != 0 && find(runOns.begin(), runOns.end(), node->RunOn) == runOns.end())
			{
				//dont build on this.
				continue;
			}
			if (isRecv)
			{
				//build an aggregation node, targeted directly at that.
				//from and tos are all myself.
				vector<NodeId> In = { node->pContext->From.at(0) };
				vector<NodeId> Out = { node->RunOn };
				var ctx = make_shared<PHubOperatorContext>(In, Out, node->pContext->Key);
				var ops = make_shared<PHubOptimizerOp>();
				var child = make_shared<ScheduleNode>(ctx, ops, node->RunOn);
				child->Upstream.push_back(node);
				node->Downstream.push_back(child);
				results.push_back(child);
			}
		}
	}

	//return the leaf nodes.
	for (var& node : nodes)
	{
		if (node->Downstream.size() == 0)
		{
			results.push_back(node);
		}
	}
	return results;
}

vector<shared_ptr<ScheduleNode>> ScheduleBuilder::CreatePHubOptimizerInferredFromAggOrCollectives(vector<shared_ptr<ScheduleNode>> leaves, vector<NodeId>& runOns)
{
	vector<shared_ptr<ScheduleNode>> results;
	for (var& node : leaves)
	{
		if (node->pOperator->Type == OperatorType::PHubAggregator || node->pOperator->Type == OperatorType::GlooCollectiveAlgorithm)
		{
			if (runOns.size() != 0 && find(runOns.begin(), runOns.end(), node->RunOn) == runOns.end())
			{
				//dont build on this.
				continue;
			}
			//build an aggregation node, targeted directly at that.
			//from and tos are all myself.
			vector<NodeId> In = { node->pContext->From.at(0) };
			vector<NodeId> Out = { node->RunOn };
			var ctx = make_shared<PHubOperatorContext>(In, Out, node->pContext->Key);
			var ops = make_shared<PHubOptimizerOp>();
			var child = make_shared<ScheduleNode>(ctx, ops, node->RunOn);
			child->Upstream.push_back(node);
			node->Downstream.push_back(child);
			results.push_back(child);
		}
	}

	//return the leaf nodes.
	for (var& node : leaves)
	{
		if (node->Downstream.size() == 0)
		{
			results.push_back(node);
		}
	}
	return results;
}

vector<shared_ptr<ScheduleNode>> ScheduleBuilder::CreatePHubBroadcastInferredFromOptimizer(vector<shared_ptr<ScheduleNode>> nodes)
{
	shared_ptr<ScheduleNode> optNode = NULL;
	vector<NodeId> to;
	for (var node : nodes)
	{
		if (node->pOperator->Type == OperatorType::PHubOptimizer)
		{
			CHECK(optNode == NULL);
			optNode = node;
		}
		if (find(to.begin(), to.end(), node->RunOn) == to.end())
		{
			to.push_back(node->RunOn);
		}
	}
	CHECK(optNode != NULL);
	//for whatever other nodes, 
	var results = CreatePHubBroadcast(optNode->RunOn, to);
}
