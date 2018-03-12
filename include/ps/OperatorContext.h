#pragma once
#include "phub.h"
#include <vector>

class OperatorContext
{
public:
	enum OperatorContextTypeCode
	{
		General,
		LocallyAvailable
	};
	OperatorContext(vector<NodeId>& in,
		vector<NodeId>& out, PLinkKey k) : Initialized(false)
	{
		From = in;
		To = out;
		Key = k;
	}
	bool Initialized;
	PLinkKey Key;
	vector<NodeId> From;
	vector<NodeId> To;
	shared_ptr<void> additionalContext = NULL;
	OperatorContextTypeCode typeCode = OperatorContextTypeCode::General;
};

//template <class T>
//class LocallyAvailableOperatorContext : public OperatorContext
//{
//public:
//	LocallyAvailableOperatorContext(vector<PLinkKey> in,
//		vector<PLinkKey> out)
//		: OperatorContext(in, out)
//	{
//		typeCode = OperatorContext::OperatorContextTypeCode::LocallyAvailable;
//	}
//};

class PHubOperatorContext : public OperatorContext
{
public:
	PHubOperatorContext(vector<NodeId>& in, vector<NodeId>& out, PLinkKey k) :
		OperatorContext(in, out, k)
	{

	}
};

class GlooContext : public OperatorContext
{
public:
	GlooContext(vector<NodeId>& in, vector<NodeId>& out, int rank, int size, PLinkKey k) :
		OperatorContext(in, out, k)
	{
		Rank = rank;
		Size = size;
	}
	int Rank;
	int Size;
};
