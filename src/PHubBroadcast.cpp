#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>

class PHubBroadcast : IOperator
{
	bool isReceiver = true;
	shared_ptr<PHub> pPhub = NULL;
	vector<vector<PLinkKey>> qpIdx2KeysIdx;
	PLinkKey key;
	size_t keySize;
	//cannot afford to create queue pairs for each broadcast opeartor.
	//reuse some of the queue pairs.
	static unordered_map<NodeId, int> node2QPNumber;
public:
	NodeId MyID;
	virtual void Initialize(OperatorContext* context) override
	{

	}

	virtual OperatorHandle Run() override
	{
		//im i the receiving side?
		//phub should optimize the broadcast routine.
		//using a central, dedicated polling from threads.
		if (isReceiver)
		{

		}
		else
		{

		}

	}
};