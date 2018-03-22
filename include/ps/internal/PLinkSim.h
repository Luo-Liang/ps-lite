#pragma once
#include <memory>
#include <vector>
#include <ps/PLink.h>
using namespace std;
typedef uint DevId;
typedef uint EventId;
typedef tuple<double, EventId> PLinkTimeLineElement;
class Link;
class Device
{
	//each of the link has one side that is me.
public:
	vector<shared_ptr<Link>> Links;
	DevId DID;
	NodeId NID;
};

///This is what goes into the event driven queue.
struct PLinkTransferEvent
{
	static EventId Ticketer;
	shared_ptr<ScheduleNode> RelevantNode;
	PLinkEventType EventType;
	double TimeStamp;
	double PendingTransfer;
	double TransferBandwidth;//this is effectively the bottleneck link given a global route table
	shared_ptr<vector<shared_ptr<Link>>> AssignedLinks;
	EventId EID;
	PLinkTransferEvent(PLinkEventType type, shared_ptr<ScheduleNode> node, double timeStamp, double pendingTransfer, shared_ptr < vector<shared_ptr<Link>>> link, double bw)
	{
		RelevantNode = node;
		EventType = type;
		TimeStamp = timeStamp;
		PendingTransfer = pendingTransfer;
		AssignedLinks = link;
		//for (shared_ptr<Link> assignedLink : *AssignedLinks)
		//{
		//	assignedLink->PendingEvents.push_back(this);
		//}
		EID = Ticketer++;
		TransferBandwidth = bw;
	}
};

class Link
{
public:
	size_t Bandwidth; //Bps
	//two endpoints.
	shared_ptr<Device> EP1;
	shared_ptr<Device> EP2;
	//effective bandwidth is Bandwidth, averaged by number of pending events.
	vector<shared_ptr<PLinkTransferEvent>> PendingEvents;
};

class Environment
{
public:
	vector<shared_ptr<Device>> Network;
	//we also need a global route table.
	//source destination current device -> Link
	unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>> RouteMap;
	Environment(vector<shared_ptr<Device>>& networkSetup, unordered_map<tuple<DevId, DevId, DevId>, shared_ptr<Link>>& routeMap)
	{
		Network = networkSetup;
		RouteMap = routeMap;
	}
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