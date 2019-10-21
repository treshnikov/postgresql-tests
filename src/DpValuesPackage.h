#pragma once
#include <QtCore/qstring.h>
#include <vector>
#include "DpValue.h"

class DpValuesPackage
{
public:
	QString ArchiveTableName;
	std::vector<DpValue> Values;

	DpValuesPackage(QString archiveTableName, std::vector<DpValue>& values);

	DpValuesPackage(QString archiveTableName);
};

