#include "DpValuesPackage.h"

DpValuesPackage::
DpValuesPackage(QString archiveTableName, std::vector<DpValue>& values): ArchiveTableName(archiveTableName),
                                                                         Values(values)
{
}

DpValuesPackage::DpValuesPackage(QString archiveTableName): ArchiveTableName(archiveTableName)
{
}
