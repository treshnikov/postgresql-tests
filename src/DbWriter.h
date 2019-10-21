#pragma once
#include <map>
#include <QtCore/qstring.h>
#include "DpDescription.h"
#include "DpValuesGroupingStrategyBase.h"
#include <QtSql/qsqldatabase.h>
#include <thread>
#include "DbProfile.h"

class DbWriter
{
private:
	DbProfile _dbProfile;
	int _threadsCount;
	std::map<QString, DpDescription> _datapointDescriptionMap;
	std::shared_ptr<DpValuesGroupingStrategyBase> _dpGrouppingStratagy;
	std::vector<QSqlDatabase> _dbPool;

	void PopulateDbConnections();

public:
	DbWriter(const DbProfile& dbProfile,
			 const std::vector<DpDescription>& dpDescription,
	         const std::shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy,
	         const int threadsCount);

	void WritePackage(DpValuesPackage& group, QSqlDatabase& db);

	void Write(const std::vector<DpValue>& dpValues);;
};

