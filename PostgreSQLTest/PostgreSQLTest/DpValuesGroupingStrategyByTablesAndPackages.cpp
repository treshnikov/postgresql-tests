#include "DpValuesGroupingStrategyByTablesAndPackages.h"

DpValuesGroupingStrategyByTablesAndPackages::DpValuesGroupingStrategyByTablesAndPackages(int packageSize,
                                                                                         const std::vector<DpDescription
                                                                                         >& dpDescription):
	_packageSize(packageSize)
{
	for (size_t i = 0; i < dpDescription.size(); i++)
	{
		dpAddressToDpDescription.insert(
			std::make_pair(dpDescription[i].Address,
			               dpDescription[i]));
	}
}

std::vector<DpValuesPackage> DpValuesGroupingStrategyByTablesAndPackages::Group(const std::vector<DpValue>& args)
{
	std::map<QString, std::vector<DpValue>> arcTableNameToDpValues;

	// group dpValues by archive table name
	for (const auto& arg : args)
	{
		auto tableName = dpAddressToDpDescription[arg.Address].ArchiveName;
		if (arcTableNameToDpValues.find(tableName) == arcTableNameToDpValues.end())
		{
			arcTableNameToDpValues[tableName] = std::vector<DpValue>();
		}

		arcTableNameToDpValues[tableName].push_back(arg);
	}

	std::vector<DpValuesPackage> result;
	for (auto iter = arcTableNameToDpValues.begin(); iter != arcTableNameToDpValues.end(); ++iter)
	{
		auto tableName = iter->first;
		DpValuesPackage currentPackage(tableName);

		for (size_t i = 0; i < arcTableNameToDpValues[tableName].size(); i++)
		{
			if (i != 0 && i % _packageSize == 0)
			{
				result.push_back(currentPackage);
				currentPackage.Values.clear();
			}

			currentPackage.Values.push_back(arcTableNameToDpValues[tableName][i]);
		}

		if (currentPackage.Values.size() != 0)
		{
			result.push_back(currentPackage);
		}
	}

	return result;
}
