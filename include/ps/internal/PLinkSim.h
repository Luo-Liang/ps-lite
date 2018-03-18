#pragma once
#include <memory>
#include <vector>
#include <ps/PLink.h>
using namespace std;
typedef uint DevId;
class Device
{
	//each of the link has one side that is me.
	vector<shared_ptr<Link>> Links;
	DevId DID;
	NodeId NID;
};

///This is what goes into the event driven queue.
struct PLinkTransferEvent
{
	shared_ptr<ScheduleNode> RelevantNode;
	PLinkEventType EventType;
	double TimeStamp;
	double PendingTransfer;
	shared_ptr<Link> AssignedLink;
};

class PLinkTransferEventComparer
{
	//just compare timestamp.
	bool Reverse;
public:
	PLinkTransferEventComparer(bool reverse)
	{
		Reverse = reverse;
	}
	bool operator() (shared_ptr<PLinkTransferEvent> pE1, shared_ptr<PLinkTransferEvent> pE2)
	{
		if (Reverse)
		{
			return pE1->TimeStamp > pE2->TimeStamp;
		}
		else
		{
			return pE1->TimeStamp < pE2->TimeStamp;
		}
	}
};

class Link
{
	uint64_t Bandwidth; //Bps
	//two endpoints.
	shared_ptr<Device> EP1;
	shared_ptr<Device> EP2;
	//effective bandwidth is Bandwidth, averaged by number of pending events.
	vector<shared_ptr<PLinkTransferEvent>> PendingEvents;
};

class Environment
{
	vector<shared_ptr<Device>> Network;
	//we also need a global route table.
	//source destination current device -> Link
	unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>> RouteMap;
};

enum PLinkEventType
{
	START,
	END
};



class PLinkSim
{
	double SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
		unordered_map<PLinkKey, shared_ptr<Schedule>>& schedules,
		unordered_map<PLinkKey, double>& ready2GoTime,
		Environment& env);
};