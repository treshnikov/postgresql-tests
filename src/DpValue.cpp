#include "DpValue.h"

DpValue::DpValue(const QString& address, const QDateTime& timestamp, float value, float status)
	: Address(address), Timestamp(timestamp), Value(value), Status(status)
{
}
