#pragma once
#include "DpValuesPackage.h"
#include <map>
#include "DpDescription.h"
#include "DpValuesGroupingStrategyBase.h"

class DpValuesGroupingStrategyByTablesAndPackages : public DpValuesGroupingStrategyBase
{
private:
	int _packageSize;
	std::map<QString, DpDescription> dpAddressToDpDescription;
public:
	DpValuesGroupingStrategyByTablesAndPackages(int packageSize, const std::vector<DpDescription>& dpDescription);

	std::vector<DpValuesPackage> Group(const std::vector<DpValue>& args);;
};

