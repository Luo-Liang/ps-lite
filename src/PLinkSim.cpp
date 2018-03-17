#include <ps/internal/PLinkSim.h>
#include <queue>
double PLinkSim::SimulateTime(unordered_map<PLinkKey, size_t>& keySizes,
	unordered_map<PLinkKey, shared_ptr<Schedule>> schedules,
	unordered_map<PLinkKey, double> ready2GoTime,
	Environment& env)
{
	//now i need to set up a timeline.
	double currentTimeInSec = 0;
	//timeline 
	//what are the known events?
	//we know at least the start of each schedule.

}