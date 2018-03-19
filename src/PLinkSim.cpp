#include <ps/internal/PLinkSim.h>
#include <queue>

static unordered_map <tuple<DevId, DevId>, shared_ptr<vector<shared_ptr<Link>>>> routeCache;
static unordered_map<tuple<DevId, DevId>, size_t> bottleneckCache;
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
	uint eBw = UINT_MAX;
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
		eBw = eBw > nextStep->Bandwidth ? nextStep->Bandwidth : eBw;
	}
	bottleneckCache.at(key) = eBw;
	routeCache.at(key) = result;
}

static uint GetLinkBottleneck(DevId src, DevId dest, unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>>& routeMap)
{
	var key = tuple<DevId, DevId>(src, dest);
	if (bottleneckCache.find(key) == bottleneckCache.end())
	{
		GetPath(src, dest, routeMap);
	}
	return bottleneckCache.at(key);
}

double PLinkSim::SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
	unordered_map<PLinkKey, shared_ptr<Schedule>>& schedules,
	unordered_map<PLinkKey, double>& ready2GoTime,
	Environment& env)
{
	//now i need to set up a timeline.
	double currentTimeInSec = 0;
	//timeline 
	//what are the known events?
	//we know at least the start of each schedule.
	vector<shared_ptr<PLinkTransferEvent>> pendingEvents;
	//you want to start with small elements.
	priority_queue<shared_ptr<PLinkTransferEvent>, vector<PLinkTransferEvent>, PLinkTransferEventComparer> timeline(PLinkTransferEventComparer(true));

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
					var projectedEnd = readyTime + size / GetLinkBottleneck(from, root->pContext->To.at(i), env.RouteMap);
					var eventEndNode = make_shared<PLinkTransferEvent>(PLinkEventType::END, node, readyTime, pendingTransfer, affectedLinks);
					//queue a 

				}
			}
			else if (root->pOperator->Type == OperatorType::GlooCollectiveAlgorithm)
			{

			}
			else
			{
				//finish instantly
			}

		}
	}


}