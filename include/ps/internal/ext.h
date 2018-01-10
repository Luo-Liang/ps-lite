#pragma once
#define var auto
//returns 2 complement of the index at which the element should be placed on failure.
//returns the index at which the element present on success.
template<class Iter, class T>
int CxxxxBinarySearch(Iter begin, Iter end, T val)
{
	// Finds the lower bound in at most log(last - first) + 1 comparisons
	Iter i = std::lower_bound(begin, end, val);

	if (i != end && !(val < *i))
		return i - begin; // found
	else
		return ~(end - begin); // not found
}

std::vector<std::string> CxxxxStringSplit(const std::string& s, char delimiter)
{
	std::vector<std::string> tokens;
	std::string token;
	std::istringstream tokenStream(s);
	while (std::getline(tokenStream, token, delimiter))
	{
		tokens.push_back(token);
	}
	return tokens;
}