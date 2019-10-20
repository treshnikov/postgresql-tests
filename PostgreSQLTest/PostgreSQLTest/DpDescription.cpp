#include "DpDescription.h"

DpDescription::DpDescription()
{
}

DpDescription::DpDescription(const QString& address, const QString& archiveName): Address(address),
                                                                                  ArchiveName(archiveName)
{
}

QString DpDescription::GetArchiveTableName(const QDateTime& timestamp)
{
	// todo define the logic that returns the name of the table depending on the given timestamp
	return ArchiveName;
}
