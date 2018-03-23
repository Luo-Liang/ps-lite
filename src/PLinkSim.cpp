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
static uint GetLinkBottleneck(DevId src,
	DevId dest,
	double now,
	unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>>& routeMap,
	unordered_map<EventId, shared_ptr<PLinkTransferEvent>>& timelineEvents)
{
	var key = tuple<DevId, DevId>(src, dest);
	var path = GetPath(src, dest, routeMap);
	uint BW = UINT_MAX;
	for (Cntr i = 0; i < path->size(); i++)
	{
		var origBW = path->at(i)->Bandwidth;
		var eBW = path->at(i)->GetEffectiveLinkSpeed(now, timelineEvents);
		if (BW > eBW)
		{
			BW = eBW;
		}
	}
	return BW;
}
//for a ghost task, AdjustETA does nothing.
static void AdjustETA(shared_ptr<PLinkTransferEvent> task, set<PLinkTimeLineElement>& timeline, double now)
{
	if (now >= task->StartTime)
	{
		var eta = task->PendingTransfer / task->TransferBandwidth;
		timeline.erase(PLinkTimeLineElement(task->TimeStamp, task->EID));
		task->TimeStamp = now + eta;
		timeline.insert(PLinkTimeLineElement(task->TimeStamp, task->EID));
	}
}

static void QueueChildren(vector<shared_ptr<ScheduleNode>>& candidates,
	set<PLinkTimeLineElement>& timeline,
	unordered_map<EventId, shared_ptr<PLinkTransferEvent>>& timelineEvents,
	size_t keySize,
	double now,
	double rdyTime,
	unordered_map<OpID, uint>& opCounter,
	Environment& env)
{
	for (var child : candidates)
	{
		bool isRoot = true;
		if (child->Upstream.size() != 0)
		{
			//signifies this is due to my parent tasks have finsihed execution.
			child->UnresolvedDependencies--;
			isRoot = false;
		}
		if (child->UnresolvedDependencies == 0)
		{
			var root = child;
			//create events, but only events that are transfer related should I care.
			var readyTime = isRoot ? rdyTime : now;
			//var eType = 
			var node = root;
			var pendingTransfer = 0;
			shared_ptr<Link> lnk = NULL;
			var size = keySize;
			if (root->pOperator->Type == OperatorType::PHubBroadcast)
			{
				pendingTransfer = size;
				//which link am i going?
				var from = root->pContext->From.at(0);
				var runON = root->RunOn;
				var isSender = from == runON;
				if (isSender)
				{
					opCounter[root->ID] = root->pContext->To.size();
					unordered_set<EventId> flags;
					for (Cntr i = 0; i < root->pContext->To.size(); i++)
					{
						//associated links?
						var affectedLinks = GetPath(from, root->pContext->To.at(i), env.RouteMap);
						//first progress existing tasks.
						for (var link = 0; link < affectedLinks->size(); link++)
						{
							for (var taskID : affectedLinks->at(link)->PendingEvents)
							{
								if (flags.find(taskID) != flags.end()) continue;
								flags.insert(taskID);
								var task = timelineEvents.at(taskID);
								//ghost tasks are not updated.
								//ghost tasks are tasks that havent actually started execution.
								task->UpdateProgress(now);
								AdjustETA(task, timeline, now);
							}
						}
						//then update links.
						//calculate my effective BW.
						//first create my node.
						var eventBegNode = make_shared<PLinkTransferEvent>(PLinkEventType::END,
							node,
							UINT_MAX,
							readyTime,
							pendingTransfer,
							affectedLinks,
							UINT_MAX,
							from,
							root->pContext->To.at(i));
						//add this to affected links.
						for (var link = 0; link < affectedLinks->size(); link++)
						{
							affectedLinks->at(link)->PendingEvents.insert(eventBegNode->EID);
						}
						var eBW = GetLinkBottleneck(from, root->pContext->To.at(i), now, env.RouteMap, timelineEvents);
						eventBegNode->TransferBandwidth = eBW;
						//now reset everyone's effective BW.
						//flags.clear();
						//the current node will be in the middle of update.
						for (var link = 0; link < affectedLinks->size(); link++)
						{
							for (var taskID : affectedLinks->at(link)->PendingEvents)
							{
								var task = timelineEvents.at(taskID);
								var eBW = affectedLinks->at(taskID)->GetEffectiveLinkSpeed(now, timelineEvents);
								if (task->TransferBandwidth > eBW)
								{
									task->TransferBandwidth = eBW;
									AdjustETA(task, timeline, now);
								}
							}
						}
						timelineEvents[eventBegNode->EID] = eventBegNode;
						//timeline.insert(PLinkTimeLineElement(eventBegNode->TimeStamp, eventBegNode->EID));
						//dont queue events to affected links yet.
					}
				}//end of if a sender 
				else
				{
					opCounter[root->ID];
					//a broadcast recv.
					//has no effect on link or other task's finish time.
					var eventEndNode = make_shared<PLinkTransferEvent>(PLinkEventType::END,
						node,
						readyTime,
						readyTime,
						0,
						NULL,
						INVALID_DEV_ID,
						INVALID_DEV_ID);
					timelineEvents[eventEndNode->EID] = eventEndNode;
					timeline.insert(PLinkTimeLineElement(readyTime, eventEndNode->EID));
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
				var eventEndNode = make_shared<PLinkTransferEvent>(PLinkEventType::END,
					node,
					readyTime,
					readyTime,
					0,
					NULL,
					INVALID_DEV_ID,
					INVALID_DEV_ID);
				timelineEvents[eventEndNode->EID] = eventEndNode;
				timeline.insert(PLinkTimeLineElement(readyTime, eventEndNode->EID));
			}
		}
	}
}



EventId PLinkTransferEvent::Ticketer;
shared_ptr<vector<shared_ptr<Link>>> PLinkTransferEvent::EMPTY_LINK;

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
	unordered_map<OpID, uint> opCounter;
	//unordered_map<PLinkKey, queue<shared_ptr<ScheduleNode>>> ways;
	for (var& schedule : schedules)
	{
		var roots = schedule.second->Roots();
		QueueChildren(roots,
			timeline,
			timelineEvents,
			keySizes.at(schedule.first),
			0,
			ready2GoTime.at(schedule.first),
			opCounter, env);
	}

	//has setup initialb state. now try to execute timeline.
	//each step, 
	unordered_map<uint, int> opCounter;
	unordered_set<EventId> flags;
	while (timeline.size() != 0)
	{
		flags.clear();
		PLinkTimeLineElement currElement = *timeline.begin();
		var currTime = std::get<0>(currElement);
		var currID = std::get<1>(currElement);
		var curr = timelineEvents.at(currID);
		timeline.erase(currElement);
		var key = curr->RelevantNode->pContext->Key;
		//var elapsedTime = currTime - lastTime;

	  //note that we always simulate maximized concurrency of each schedule.
		if (curr->RelevantNode->pOperator->Type == OperatorType::PHubBroadcast)
		{
			//note that due to pseudo links, we dont need to worry about depdenency b/w send and recv.
			//can I really finish you now?
			curr->UpdateProgress(currTime);
			//remember, each broadcast is modeled as individual transfer to all destinations.
			//I can finish this event, but not that operator, unless all transfers are done.
			CHECK(curr->PendingTransfer >= 0);
			//get rid of this from timeline.
			//timeline.erase(currElement);
			//remove this from schedule queue.
			if (curr->PendingTransfer > 0)
			{
				//reschedule it.
				//create a new estimate.
				AdjustETA(curr, timeline, currTime);
				//var estimatedDone = currTime + curr->PendingTransfer / curr->TransferBandwidth;
				//timeline.insert(PLinkTimeLineElement(estimatedDone, currID));
			}
			else
			{
				//is 0.
				//finish this event.
				timelineEvents.erase(currID);
				//free up some link resource that current transfer is using.
				for (var link = 0; link < curr->AssignedLinks->size(); link++)
				{
					//what are the affected tasks?
					for (var taskIDs : curr->AssignedLinks->at(link)->PendingEvents)
					{
						//progress these tasks.
						var& task = timelineEvents.at(taskIDs);
						if (flags.find(task->EID) != flags.end() || task->EventType == PLinkEventType::START)
						{
							//nothing affects start events, 
							//compute task progress only once.
							continue;
						}
						flags.insert(task->EID);
						//make a progress.
						task->UpdateProgress(currTime);
						//the link's bandwidth is going up.
						//unfortunately, the link speed bottleneck can be multiple segments. we must re-probe.
						//remove this task from link.
					}
					curr->AssignedLinks->at(link)->PendingEvents.erase(curr->EID);
				}
				//next, compute each affected task's effective speed, and requeue them.
				flags.clear();
				for (var link = 0; link < curr->AssignedLinks->size(); link++)
				{
					//what are the affected tasks?
					for (var taskIDs : curr->AssignedLinks->at(link)->PendingEvents)
					{
						var& task = timelineEvents.at(taskIDs);
						if (flags.find(task->EID) != flags.end() || task->EventType == PLinkEventType::START)
						{
							//nothing affects start events, 
							//compute task progress only once.
							continue;
						}
						flags.insert(task->EID);
						var eBW = GetLinkBottleneck(task->From, task->To, currTime, env.RouteMap, timelineEvents);
						task->TransferBandwidth = eBW;
						AdjustETA(task, timeline, currTime);
					}
					//curr->AssignedLinks->at(link)->PendingEvents.erase(curr->EID);
				}
				//now test to see if this operator is finished
				opCounter[curr->RelevantNode->ID]++;
				if (opCounter[curr->RelevantNode->ID] == curr->RelevantNode->pContext->To.size())
				{
					//i have finsihed this broadcast op.
					//if i am a sender, my receivers will all finish instantly (maybe 
					//you want to add a certain delay).

					//otherwise, they may be other type of ops.
					QueueChildren(curr->RelevantNode->Downstream, timeline, timelineEvents, keySizes.at(key), currTime, currTime, opCounter, env);
				}
			}
		}
		else
		{
			//other operator. finish instantly.
			//there is no need for updating any sort of link.
			QueueChildren(curr->RelevantNode->Downstream, timeline, timelineEvents, keySizes.at(key), currTime, currTime, opCounter, env);
		}
		//lastTime = currTime;
	}
}