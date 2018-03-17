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

class Link
{
	uint64_t Bandwidth; //Bps
	//two endpoints.
	shared_ptr<Device> EP1;
	shared_ptr<Device> EP2;
};

class Environment
{
	vector<shared_ptr<Device>> Network;
};

class PLinkSim
{
	double SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
		unordered_map<PLinkKey, shared_ptr<Schedule>>& schedules,
		unordered_map<PLinkKey, double>& ready2GoTime,
		Environment& env);
};