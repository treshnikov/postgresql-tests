#pragma once
#include <QtCore/qstring.h>
#include <QtCore/qdatetime.h>

class DpDescription
{
public:
	QString Address;
	QString ArchiveName;

	DpDescription();

	DpDescription(const QString& address, const QString& archiveName);

	QString GetArchiveTableName(const QDateTime& timestamp);
};

