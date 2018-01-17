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
		CHECK(pPhub != NULL);
		CHECK(context->typeCode == OperatorContext::General);
		//CHECK(std::adjacent_find(pctx->inputLens.begin(), pctx->inputLens.end(), std::not_equal_to<int>()) == pctx->inputLens.end()) << "lens of elements are different!";
		//im i a receiver or a sender
		for (var buffer : context->inputs)
		{
			var node = NodeIdFromHandle(buffer);
			if (node == MyID)
			{
				isReceiver = false;
				break;
			}
		}

		//i need to determine the fastest way to finish this operator,
		//creating new QPs nad stuff.
		std::vector<NodeId> remotes = CxxxxSelect<NodeId, BufferHandle>(
			isReceiver ? context->inputs : context->outputs,
			[](BufferHandle handle)
		{
			return NodeIdFromHandle(handle);
		});

		var qpCount = remotes.size();
		var keys = CxxxxSelect<PLinkKey, BufferHandle>(
			isReceiver ? context->inputs : context->outputs,
			[](BufferHandle handle)
		{
			return KeyFromHandle(handle);
		});

		CHECK(keys.size() > 0);
		CHECK(AllEqual<PLinkKey>(keys));
		key = keys.at(0);

		//size in Bytes
		keySize = pPhub->keySizes.at(key);

		//distribute key to qps.
		qpSizes.resize(qpCount);
		qpIdx2KeysIdx.resize(qpCount);

		for (Cntr i = 0; i < keys.size(); i++)
		{
			var qp = key2QPIdx.at(i);
			qpSizes.at(i) += kSizes.at(i);
			qpIdx2KeysIdx.at(qp).push_back(i);
		}


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