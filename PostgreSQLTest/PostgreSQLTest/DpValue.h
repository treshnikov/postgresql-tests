#pragma once
#include <QtCore/qstring.h>
#include <QtCore/qdatetime.h>

class DpValue
{
public:
	QString Address;
	QDateTime Timestamp;
	float Value;
	float Status;

	DpValue(const QString& address, const QDateTime& timestamp, float value, float status);
};

