#include <ps/internal/PLinkSim.h>
#include <queue>
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
			//create events.

		}
	}


}