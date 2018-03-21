#include <ps/internal/PLinkSim.h>
#include <set>

static unordered_map <tuple<DevId, DevId>, shared_ptr<vector<shared_ptr<Link>>>> routeCache;
static shared_ptr<vector<shared_ptr<Link>>> GetPath(DevId src, DevId dest, unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>>& routeMap)
{
	var key = tuple<DevId, DevId>(src, dest);
	if (routeCache.find(key) != routeCache.end())
	{
		return routeCache.at(key);
	}
	//now figure out the new data
	DevId current = src;
	var result = make_shared<vector<shared_ptr<Link>>>();
	while (dest != current)
	{
		var nextStep = routeMap.at(tuple<DevId, DevId, DevId>(src, dest, current));
		result->push_back(nextStep);
		if (nextStep->EP1->DID == current)
		{
			current = nextStep->EP2->DID;
		}
		else
		{
			current = nextStep->EP1->DID;
		}
	}
	routeCache.at(key) = result;
}

static uint GetLinkBottleneck(DevId src, DevId dest, unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>>& routeMap)
{
	var key = tuple<DevId, DevId>(src, dest);
	var path = GetPath(src, dest, routeMap);
	uint BW = UINT_MAX;
	for (Cntr i = 0; i < path->size(); i++)
	{
		var origBW = path->at(i)->Bandwidth;
		var eBW = origBW / path->at(i)->PendingEvents.size();
		if (BW > eBW)
		{
			BW = eBW;
		}
	}
	return BW;
}

EventId PLinkTransferEvent::Ticketer;

double PLinkSim::SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
	unordered_map<PLinkKey, shared_ptr<Schedule>>& schedules,
	unordered_map<PLinkKey, double>& ready2GoTime,
	Environment& env)
{
	//now i need to set up a timeline.
	//timeline 
	//what are the known events?
	//we know at least the start of each schedule.
	unordered_map<EventId, shared_ptr<PLinkTransferEvent>> timelineEvents;
	//you want to start with small elements.
	//priority_queue<shared_ptr<PLinkTransferEvent>, vector<shared_ptr<PLinkTransferEvent>>, PLinkTransferEventComparer> timeline(PLinkTransferEventComparer(true));
	set<tuple<double, EventId>> timeline;
	//find start of each schedule.
	//this is kind of like a K way merge.

	//unordered_map<PLinkKey, queue<shared_ptr<ScheduleNode>>> ways;
	for (var& schedule : schedules)
	{
		var roots = schedule.second->Roots();
		for (var& root : roots)
		{
			//create events, but only events that are transfer related should I care.
			var key = schedule.first;
			var readyTime = ready2GoTime.at(key);
			//var eType = 
			var node = root;
			var pendingTransfer = 0;
			shared_ptr<Link> lnk = NULL;
			var size = keySizes.at(key);
			if (root->pOperator->Type == OperatorType::PHubBroadcast)
			{
				pendingTransfer = keySizes.at(key);
				//which link am i going?
				var from = root->pContext->From.at(0);
				for (Cntr i = 0; i < root->pContext->To.size(); i++)
				{
					//associated links?
					var affectedLinks = GetPath(from, root->pContext->To.at(i), env.RouteMap);
					//var projectedEnd = readyTime + size / GetLinkBottleneck(from, root->pContext->To.at(i), env.RouteMap);
					var eventBegNode = make_shared<PLinkTransferEvent>(PLinkEventType::START, node, readyTime, pendingTransfer, affectedLinks);
					timelineEvents[eventBegNode->EID] = eventBegNode;
					timeline.insert(PLinkTimeLineElement(readyTime, eventBegNode->EID));
					//dont queue events to affected links yet.
				}
			}
			else if (root->pOperator->Type == OperatorType::GlooCollectiveAlgorithm)
			{
				//how do we model collectives?
				//requires many steps.
				//ban collectives for now.
				CHECK(false);
			}
			else
			{
				//finish instantly
				//no affected link, just need an eventEndNode.
				//this allows timeline to progress directly to the current dependants.
				var eventEndNode = make_shared<PLinkTransferEvent>(PLinkEventType::END, node, readyTime, 0, NULL);
				timelineEvents[eventEndNode->EID] = eventEndNode;
				timeline.insert(PLinkTimeLineElement(readyTime, eventEndNode->EID));
			}
		}
	}

	//has setup initialb state. now try to execute timeline.
	//each step, 
	unordered_map<OpID, int> opCounter;
	double lastTime = 0;
	while (timeline.size() != 0)
	{
		var& curr = *timeline.begin();
		var currTime = std::get<0>(curr);
		timeline.erase(curr);
		var key = timelineEvents.at(std::get<1>(curr))->RelevantNode->pContext->Key;
		var elapsedTime = currTime - lastTime;
		if (curr->EventType == PLinkEventType::START)
		{
			//needs to set up the initial phase of the algorithm.
			//must be a Send node.
			//because it is a start.
			if (curr->RelevantNode->pOperator->Type == OperatorType::PHubBroadcast)
			{
				//this to affected links.
				for (Cntr i = 0; i < curr->AssignedLinks->size(); i++)
				{
					curr->AssignedLinks->at(i)->PendingEvents.push_back(curr);

				}
				//remember, whenever a link share is updated, update the progress of each affected event
				//produce an underestimate finish time.
				var from = curr->RelevantNode->pContext->From.at(0);
				var to = curr->RelevantNode->RunOn;
				//promote.
				double sz = keySizes.at(key);
				var estimatedEnd = currTime + sz / GetLinkBottleneck(from, to, env.RouteMap);
				//queue an end node.
				var endNode = make_shared<PLinkTransferEvent>(PLinkEventType::END, curr->RelevantNode, estimatedEnd, curr->PendingTransfer, curr->AssignedLinks);
				timeline.push(endNode);
			}
			else if (curr->RelevantNode->pOperator->Type == OperatorType::GlooCollectiveAlgorithm)
			{
				//ban collectives for now.
				CHECK(false);
			}
			else
			{
				CHECK(false);
			}
		}
		else
		{
			//note that we always simulate maximized concurrency of each schedule.
			if (curr->RelevantNode->pOperator->Type == OperatorType::PHubBroadcast)
			{
				//note that due to pesudo links, we dont need to worry about depdenency b/w send and recv.
				var timeDiff = currTime - lastTime;
				//can I finish you now?

			}
		}
		lastTime = currTime;
	}
}