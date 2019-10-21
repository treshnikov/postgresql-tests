#pragma once
#include <QtCore/qstring.h>

struct DbProfile
{
	QString HostName;
	QString DatabaseName;
	QString UserName;
    QString Password;

	DbProfile(QString hostName, QString databaseName, QString userName, QString password);
};

