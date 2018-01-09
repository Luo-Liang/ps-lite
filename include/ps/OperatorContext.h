#pragma once
#include "phub.h"
#include <vector>
class OperatorContext
{

public:
	OperatorContext(vector<BufferHandle> in,
		vector<BufferHandle> out, 
		void* additionalCtx)
	{
		inputs = in;
		outputs = out;
		additionalContext = additionalCtx;
	}
	vector<BufferHandle> inputs;
	vector<BufferHandle> outputs;
	void* additionalContext;
};