#pragma once
#include <map>
#include <QtCore/qstring.h>
#include "DpDescription.h"
#include "DpValuesGroupingStrategyBase.h"
#include <QtSql/qsqldatabase.h>
#include <iostream>
#include <QtSql/qsqlquery.h>
#include <QtSql/qsqlerror.h>
#include <thread>

class DbWriter
{
private:
	int _threadsCount;
	std::map<QString, DpDescription> _datapointDescriptionMap;
	std::shared_ptr<DpValuesGroupingStrategyBase> _dpGrouppingStratagy;
	std::vector<QSqlDatabase> dbPool;

	void PopulateDbConnections()
	{
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

			dbPool.push_back(db);
		}
	}

public:
	DbWriter(const std::vector<DpDescription>& dpDescription,
		const std::shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy,
		const int threadsCount)
		: _dpGrouppingStratagy(dpGrouppingStratagy), _threadsCount(threadsCount)
	{
		for (size_t i = 0; i < dpDescription.size(); i++)
		{
			_datapointDescriptionMap.insert(
				std::make_pair(dpDescription[i].Address,
					dpDescription[i]));
		}

		PopulateDbConnections();
	};

	void WritePackage(DpValuesPackage& group, QSqlDatabase& db)
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

	void Write(const std::vector<DpValue>& dpValues)
	{
		std::vector<std::thread> threadPool;
		auto groupedValues = _dpGrouppingStratagy->Group(dpValues);

		for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
		{
			auto& group = groupedValues[groupIdx];

			int poolIdx = groupIdx % _threadsCount;
			auto& db = dbPool[poolIdx];

			threadPool.push_back(std::thread(&DbWriter::WritePackage, this, std::ref(group), std::ref(db)));

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
	};
};

