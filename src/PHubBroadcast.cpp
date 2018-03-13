#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <ps/PHubBroadcast.h>


void PHubBroadcast::Initialize(shared_ptr<OperatorContext> context)
{
	Type = OperatorType::PHubBroadcast;
	pPhub = dynamic_pointer_cast<PHub>(context->additionalContext);
	opContext = dynamic_pointer_cast<PHubOperatorContext>(context);
	//am i a sender or a receiver
	//i am a receiver if i am to.

	CHECK(opContext->From.size() == 1);
	isReceiver = opContext->From.at(0) == pPhub->ID;
	//target destinations in output
	opContext = context;
}

OperationStatus PHubBroadcast::Run()
{
	//am i the receiving side?
	//phub should optimize the broadcast routine.
	//using a central, dedicated polling from threads.
	if (isReceiver)
	{
		//a receiver simply polls.
		var status = pPhub->TryPull(key, (NodeId)opContext->From.at(0));
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
		for (var remote : opContext->To)
		{
			pPhub->Push(key, (NodeId)remote);
		}
		return OperationStatus::Finished;
	}
}
