#pragma once
#include <memory>
#include <vector>
#include <ps/PLink.h>
using namespace std;
typedef uint DevId;
#define INVALID_DEV_ID UINT_MAX
#define LINK_TRANSFER_DELAY 0.01
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
	static shared_ptr < vector<shared_ptr<Link>>> EMPTY_LINK;
	shared_ptr<ScheduleNode> RelevantNode;
	PLinkEventType EventType;
	double TimeStamp;
	double PendingTransfer;
	double TransferBandwidth;//this is effectively the bottleneck link given a global route table
	double LastTouch;
	double StartTime;
	shared_ptr<vector<shared_ptr<Link>>> AssignedLinks;
	EventId EID;
	DevId From;
	DevId To;
	PLinkTransferEvent(PLinkEventType type,
		shared_ptr<ScheduleNode> node,
		double timeStamp,
		double startTime,
		double pendingTransfer,
		shared_ptr < vector<shared_ptr<Link>>> link,
		double bw,
		DevId from,
		DevId to)
	{
		RelevantNode = node;
		EventType = type;
		TimeStamp = timeStamp;
		PendingTransfer = pendingTransfer;
		if (link != NULL)
		{
			AssignedLinks = link;
		}
		else
		{
			AssignedLinks = EMPTY_LINK;
		}
		//for (shared_ptr<Link> assignedLink : *AssignedLinks)
		//{
		//	assignedLink->PendingEvents.push_back(this);
		//}
		EID = Ticketer++;
		TransferBandwidth = bw;
		From = from;
		To = to;
		StartTime = LastTouch = startTime;
	}
	void UpdateProgress(double now)
	{
		if (now > LastTouch)
		{
			PendingTransfer -= (now - LastTouch) * TransferBandwidth;
			LastTouch = now;
		}
	}
};

class Link
{
public:
	double Bandwidth; //Bps
	//two endpoints.
	shared_ptr<Device> EP1;
	shared_ptr<Device> EP2;
	//effective bandwidth is Bandwidth, averaged by number of pending events.
	unordered_set<EventId> PendingEvents;
	double GetEffectiveLinkSpeed(double now, unordered_map<EventId, shared_ptr<PLinkTransferEvent>>& timelineEvents)
	{
		int cntr = 0;
		for (var id : PendingEvents)
		{
			//exclude ghost tasks
			if (timelineEvents.at(id)->StartTime >= now)
			{
				cntr++;
			}
		}
		return Bandwidth / cntr;
	}
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
private:
	//removes and requeues an event element to timeline, in response to a change in effective transfer speed.
public:
	double SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
		unordered_map<PLinkKey, shared_ptr<Schedule>>& schedules,
		unordered_map<PLinkKey, double>& ready2GoTime,
		Environment& env);
};