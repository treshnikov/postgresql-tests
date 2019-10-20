#pragma once
#include <vector>
#include "DpValuesPackage.h"

class DpValuesGroupingStrategyBase
{
public:
	virtual std::vector<DpValuesPackage> Group(const std::vector<DpValue>& args) = 0;
};

