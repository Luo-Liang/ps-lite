#include <ps/Schedule.h>
#include <ps/ScheduleBuilder.h>
Schedule Schedule::DebugGenerateGlobalSchedule(std::string desc)
{
	Schedule s;
	if (desc == "collectivescross2racks")
	{
		//assuming 6 machines.
		//1 2 3 4,  5 6 7 8 on different racks.
		//the schedule uses collectives to synchronize 1 2 3 4, then 5 6 7 8, then 1 and 5, then 

		//first step, collective within a single rack.
		//these run on 4 nodes.

		ScheduleBuilder sb(0);
		vector<NodeId> col1 = { 0,1,2,3 };
		var col1R = sb.CreateCollectives(col1);
		vector<NodeId> col2 = { 4,5,6,7 };
		var col2R = sb.CreateCollectives(col2);

		//these two have no dependence.
		//now synchronize 0 and 4.
		vector<NodeId> col1col2 = { 0,4 };
		var col1col2R = sb.CreateCollectives(col1col2);

		//now zip col1R col1col2R , and col2R col1col2R
		var col1RR = sb.IntelligentZip(col1R, col1col2R);
		var col2RR = sb.IntelligentZip(col2R, col1col2R);

		//now 0 and 4 needs to perform optimization.
		//col1RR has 0 and 4 hooked to a two-node collectives with 0 and 4.
		//now let 0 perform an optimization.
		vector<NodeId> optNode = { 0 };
		var opt1R = sb.CreatePHubOptimizerInferredFromAggOrCollectives(col1RR, optNode);

		vector<NodeId> optNode2 = { 4 };
		var opt2R = sb.CreatePHubOptimizerInferredFromAggOrCollectives(col2RR, optNode2);
		//now broadcast out from 0 to  1,2,3
		//and from 4 to 5 6 7


		
	}
	else if (desc == "hybridcross2racks")
	{
	}
	return s;
}