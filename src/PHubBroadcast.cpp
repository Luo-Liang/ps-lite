#include <ps/Schedule.h>
#include <ps/Operator.h>
#include <ps/phub.h>
class PHubBroadcast : IOperator
{
	bool isReceiver = true;
public:
	NodeId MyID;
	virtual void Initialize(OperatorContext* context) override
	{
		CHECK(context->typeCode == OperatorContext::General);
		//CHECK(std::adjacent_find(pctx->inputLens.begin(), pctx->inputLens.end(), std::not_equal_to<int>()) == pctx->inputLens.end()) << "lens of elements are different!";
		for (var buffer : context->inputs)
		{
			var node = NodeIdFromHandle(buffer);
			if (node == MyID)
			{
				isReceiver = false;
				break;
			}
		}
		//im i a receiver or a sender?

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