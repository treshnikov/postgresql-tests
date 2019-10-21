#include <QtCore/qdatetime.h>
#include <QtCore/qvariant.h>
#include <QtCore/qrandom.h>

#include "DpDescription.h"
#include "DpValue.h"
#include "DpValuesGroupingStrategyByTablesAndPackages.h"
#include "DbWriter.h"
#include <iostream>


std::vector<DpDescription> PopulateDemoDpDescriptions(int dpCount)
{
	std::vector<DpDescription> dps;
	for (size_t i = 1; i <= dpCount; i++)
	{
		dps.push_back(DpDescription(
			"System1.temperature_" + QString::number(i).rightJustified(2, '0'),
			QString("z_arc%1").arg(i)));
	}

	return dps;
}


std::vector<DpValue>  GenerateDemoDpValues(std::vector<DpDescription>& dpsDescriptions, QDateTime& modelTime, int dpValuesForOneDp)
{
	std::vector<DpValue> values;
	for (auto& dpsDescription : dpsDescriptions)
	{
		auto ts = modelTime;
		for (size_t valueIdx = 0; valueIdx < dpValuesForOneDp; valueIdx++)
		{
			values.push_back(
				DpValue(dpsDescription.Address,
					ts,
					QRandomGenerator::global()->generate(),
					QRandomGenerator::global()->generate()));
			ts = ts.addMSecs(1);
		}
	}
	return values;
}

int main(void)
{
	// the number of datapoints to emulate, the values of each datapoint are stored in a separate table - z_arcXXX : [Timestamp, P_01, S_01]
	const auto dpCounts = 50;

	// the number of data point values ​​to generate per iteration for each datapoint 
	const auto dpValuesForOneDp = 1000;

	// package size for bulk insert
	const auto dpValuesInOneDbTransaction = 1000;
	
	// count of threads for writing datapoint values into DB
	const auto dbWriteThreadsCount = 5;

	auto dpsDescriptions = PopulateDemoDpDescriptions(dpCounts);
	DpValuesGroupingStrategyByTablesAndPackages dpValuesGroupingStratagy(dpValuesInOneDbTransaction, dpsDescriptions);
	DbWriter dbWriter(
		dpsDescriptions,
		std::make_shared<DpValuesGroupingStrategyByTablesAndPackages>(dpValuesGroupingStratagy),
		dbWriteThreadsCount);

	auto modelTime = QDateTime::currentDateTime();
	while (true)
	{
		auto values = GenerateDemoDpValues(dpsDescriptions, modelTime, dpValuesForOneDp);
		modelTime = modelTime.addMSecs(dpValuesForOneDp + 1);

		auto startTime = QDateTime::currentDateTime();
		dbWriter.Write(values);
		std::cout << 1000 * dpCounts * dpValuesForOneDp / startTime.msecsTo(QDateTime::currentDateTime()) << " param per sec" << std::endl;
	}
	system("pause");

	return 0;
}