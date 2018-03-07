#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
using namespace std;
class PHubBroadcast : IOperator
{
	bool isReceiver = true;
	shared_ptr<PHub> pPhub = NULL;
	PLinkKey key;
	size_t keySize;
	shared_ptr<OperatorContext> opContext;
public:
	virtual void Initialize(shared_ptr<OperatorContext> context) override
	{
		pPhub = static_pointer_cast<PHub>(context->additionalContext);
		//am i a sender or a receiver
		//isReceiver = context->inputs.size() == 1;
		//target destinations in output
		opContext = context;
	}

	void SetReciever(bool receiver)
	{
		isReceiver = receiver;
	}

	virtual OperationStatus Run() override
	{
		//im i the receiving side?
		//phub should optimize the broadcast routine.
		//using a central, dedicated polling from threads.
		if (isReceiver)
		{
			//a receiver simply polls.
			var status = pPhub->TryPull(key, (NodeId)opContext->inputs[0]);
			if (status)
			{
				return OperationStatus::Finished;
			}
			else
			{
				return OperationStatus::QueuedForExecution;
			}
		}
		else
		{
			//a sender simply sends.
			for (var remote : opContext->outputs)
			{
				pPhub->Push(key, (NodeId)remote);
			}
			return OperationStatus::Finished;
		}

	}
};