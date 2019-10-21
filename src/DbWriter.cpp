#include "DbWriter.h"
#include <iostream>
#include <QtSql/qsqlquery.h>
#include <QtSql/qsqlerror.h>

void DbWriter::PopulateDbConnections()
{
	// todo pass connection string
	for (size_t i = 0; i < _threadsCount; i++)
	{
		QSqlDatabase db = QSqlDatabase::addDatabase("QPSQL", QString::number(i));
		db.setHostName("localhost");
		db.setDatabaseName("winccoa");
		db.setUserName("postgres");
		db.setPassword("postgres");
		auto connected = db.open();

		if (!connected)
		{
			// todo
			std::cout << "Cannot connect to db" << std::endl;
		}

		_dbPool.push_back(db);
	}
}

DbWriter::DbWriter(const std::vector<DpDescription>& dpDescription,
                   const std::shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy, const int threadsCount):
	_dpGrouppingStratagy(dpGrouppingStratagy), _threadsCount(threadsCount)
{
	for (const auto& i : dpDescription)
	{
		_datapointDescriptionMap.insert(std::make_pair(i.Address, i));
	}

	PopulateDbConnections();
}

void DbWriter::WritePackage(DpValuesPackage& group, QSqlDatabase& db)
{
	QString sql;
	QSqlQuery query(db);
	const int cnt = group.Values.size();

	QStringList tss;
	QStringList values;
	QStringList statuses;

	//db.transaction();
	for (auto& dpValue : group.Values)
	{
		tss << "TIMESTAMP '" + dpValue.Timestamp.toString("dd.MM.yyyy hh:mm:ss.zzz") + "'";
		values << QString::number(dpValue.Value);
		statuses << QString::number(dpValue.Status);
	}

	// todo pass values by array variables
	sql = QString(
			"INSERT INTO \"" + group.ArchiveTableName +
			"\" (\"timestamp\", \"p_01\", \"s_01\") SELECT unnest(array[%1]), unnest(array[%2]), unnest(array[%3])")
		.arg(tss.join(", "), values.join(", "), statuses.join(", "));

	query.prepare(sql);

	const auto insertResult = query.exec();
	if (!insertResult)
	{
		// todo
		auto lastError = query.lastError().text();
	}

	//db.commit();
}

void DbWriter::Write(const std::vector<DpValue>& dpValues)
{
	std::vector<std::thread> threadPool;
	auto groupedValues = _dpGrouppingStratagy->Group(dpValues);

	for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
	{
		auto& group = groupedValues[groupIdx];

		const int poolIdx = groupIdx % _threadsCount;
		auto& db = _dbPool[poolIdx];

		threadPool.emplace_back(&DbWriter::WritePackage, this, std::ref(group), std::ref(db));

		if (poolIdx == _threadsCount - 1)
		{
			for (auto& th : threadPool)
			{
				th.join();
			}
			threadPool.clear();
		}
	}

	for (auto& th : threadPool)
	{
		th.join();
	}
	threadPool.clear();
}
