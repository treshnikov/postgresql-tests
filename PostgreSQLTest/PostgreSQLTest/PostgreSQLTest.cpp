#include <iostream>
#include <string>

#include <QtCore/qdatetime.h>
#include <QtCore/qvariant.h>
#include <QtCore/qrandom.h>

#include <QtSql/qsql.h>
#include <QtSql/qsqlquery.h>
#include <QtSql/qsqlerror.h>
#include <thread>

using namespace std;

class DpValue
{
public:
	QString Address;
	QDateTime Timestamp;
	float Value;
	float Status;

	DpValue(const QString& address, const QDateTime& timestamp, float value, float status)
		:Address(address), Timestamp(timestamp), Value(value), Status(status)
	{
	}
};

class DpValuesPackage
{
public:
	QString ArchiveTableName;
	vector<DpValue> Values;

	DpValuesPackage(QString archiveTableName, vector<DpValue>& values)
		: ArchiveTableName(archiveTableName), Values(values)
	{}

	DpValuesPackage(QString archiveTableName)
		: ArchiveTableName(archiveTableName)
	{}
};

class DpDescription
{
public:
	QString Address;
	QString ArchiveName;

	DpDescription() {}

	DpDescription(const QString& address, const QString& archiveName)
		:Address(address), ArchiveName(archiveName)
	{
	}

	QString GetArchiveTableName(const QDateTime& timestamp)
	{
		return "values";
	}
};

class DpValuesGroupingStrategyBase
{
public:
	virtual vector<DpValuesPackage> Group(const vector<DpValue>& args) = 0;
};

class DpValuesGroupingStrategyByTablesAndPackages : public DpValuesGroupingStrategyBase
{
private:
	int _packageSize;
	map<QString, DpDescription> dpAddressToDpDescription;
public:
	DpValuesGroupingStrategyByTablesAndPackages(int packageSize, const vector<DpDescription>& dpDescription) 
		: _packageSize(packageSize) 
	{
		for (size_t i = 0; i < dpDescription.size(); i++)
		{
			dpAddressToDpDescription.insert(
				make_pair(dpDescription[i].Address,
					dpDescription[i]));
		}

	}

	vector<DpValuesPackage> Group(const vector<DpValue>& args)
	{
		map<QString, vector<DpValue>> arcTableNameToDpValues;

		// group dpValues by archive table name
		for (size_t i = 0; i < args.size(); i++)
		{
			auto tableName = dpAddressToDpDescription[args[i].Address].ArchiveName;
			if (arcTableNameToDpValues.find(tableName) == arcTableNameToDpValues.end())
			{
				arcTableNameToDpValues[tableName] = vector<DpValue>();
			}

			arcTableNameToDpValues[tableName].push_back(args[i]);
		}

		vector<DpValuesPackage> result;
		for (auto iter = arcTableNameToDpValues.begin(); iter != arcTableNameToDpValues.end(); ++iter)
		{
			auto tableName = iter->first;
			DpValuesPackage currentPackage(tableName);

			for (size_t i = 0; i < arcTableNameToDpValues[tableName].size(); i++)
			{
				currentPackage.Values.push_back(arcTableNameToDpValues[tableName][i]);
			}
			result.push_back(currentPackage);
		}

		return result;
	};
};

class DbWriter
{
private:
	map<QString, DpDescription> _datapointDescriptionMap;
	shared_ptr<DpValuesGroupingStrategyBase> _dpGrouppingStratagy;
	//QSqlDatabase _db = QSqlDatabase::addDatabase("QPSQL");
	vector<thread> threadPool;
	vector<QSqlDatabase> dbPool;

	//bool TryConnectToDb()
	//{
	//	//todo pass params via config or arguments
	//	_db.setHostName("localhost");
	//	_db.setDatabaseName("winccoa");
	QSqlDatabase& AcquireConnection()
	{
		auto maxConnections = 5;
		auto connectionNumber = 0;

		if (dbPool.size() <= connectionNumber)
		{
			//std::hash<std::thread::id> hasher;
			//auto connectionName = QString::number(hasher(this_thread::get_id()));

			QSqlDatabase db = QSqlDatabase::addDatabase("QPSQL", QString::number(connectionNumber));
			db.setHostName("localhost");
			db.setDatabaseName("winccoa");
			db.setUserName("postgres");
			db.setPassword("postgres");
			auto connected = db.open();

			if (!connected)
			{
			}

			dbPool.push_back(db);
		}

		return dbPool[connectionNumber];
	}

public:
	DbWriter(const vector<DpDescription>& dpDescription,
		const shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy)
		: _dpGrouppingStratagy(dpGrouppingStratagy)
	{
		for (size_t i = 0; i < dpDescription.size(); i++)
		{
			_datapointDescriptionMap.insert(
				make_pair(dpDescription[i].Address,
					dpDescription[i]));
		}
	};

	void WritePackage(DpValuesPackage& group)
	{
		QSqlDatabase& db = AcquireConnection();
		QString sql;
		QSqlQuery query(db);

		QVariantList tss;
		QVariantList values;
		QVariantList statuses;

		db.transaction();
		for (size_t dpIdx = 0; dpIdx < group.Values.size(); dpIdx++)
		{
			DpValue& dpValue = group.Values[dpIdx];
			tss.push_back(dpValue.Timestamp);
			values.push_back(dpValue.Value);
			statuses.push_back(dpValue.Status);
		}

		sql = "INSERT INTO \"" + group.ArchiveTableName + "\" (\"timestamp\", \"p_01\", \"s_01\") VALUES (?, ?, ?)"; 			//ON CONFLICT (\"timestamp\") DO UPDATE SET \"p_01\" = :value, \"s_01\" = :status
		query.prepare(sql);
		query.addBindValue(tss);
		query.addBindValue(values);
		query.addBindValue(statuses);

		auto insertResult = query.execBatch();
		if (!insertResult)
		{
			auto lastError = query.lastError().text();
		}

		db.commit();
		//db.close();
		//QSqlDatabase::removeDatabase(QString::number(connectionNumber));
	}

	void Write(const vector<DpValue>& dpValues)
	{
		auto groupedValues = _dpGrouppingStratagy->Group(dpValues);

		for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
		{
			DpValuesPackage& group = groupedValues[groupIdx];
			threadPool.push_back(thread(&DbWriter::WritePackage, this, ref(group)));
		}

		for (auto& th : threadPool)
		{
			th.join();
		}

		threadPool.clear();

	};
};

vector<DpDescription> PopulateDemoDpDescriptions(int dpCount)
{
	vector<DpDescription> dps;
	for (size_t i = 1; i <= dpCount; i++)
	{
		dps.push_back(DpDescription(
			"System1.temperature_" + QString::number(i).rightJustified(2, '0'),
			QString("z_arc%1").arg(i)));
	}

	return dps;
}

int main(void)
{
	int demoDpCounts = 1;
	int demoValuesPerOneDp = 5000;
	auto dpsDescriptions = PopulateDemoDpDescriptions(demoDpCounts);

	DpValuesGroupingStrategyByTablesAndPackages dpValuesGroupingStratagy(demoValuesPerOneDp, dpsDescriptions);
	
	DbWriter dbWriter(
		dpsDescriptions, 
		make_shared<DpValuesGroupingStrategyByTablesAndPackages>(dpValuesGroupingStratagy));

	auto modelTime = QDateTime::currentDateTime();
	while (true)
	{
		vector<DpValue> values;
		for (size_t dpIdx = 0; dpIdx < dpsDescriptions.size(); dpIdx++)
		{
			auto ts = modelTime;
			for (size_t valueIdx = 0; valueIdx < demoValuesPerOneDp; valueIdx++)
			{
				values.push_back(
					DpValue(dpsDescriptions[dpIdx].Address,
						ts,
						QRandomGenerator::global()->generate(),
						QRandomGenerator::global()->generate()));
				ts = ts.addMSecs(1);
			}
		}
		modelTime = modelTime.addMSecs(demoValuesPerOneDp + 1);

		auto startTime = QDateTime::currentDateTime();
		dbWriter.Write(values);
		auto durationMs = startTime.msecsTo(QDateTime::currentDateTime());
		cout << (1000 * demoDpCounts * demoValuesPerOneDp) / durationMs << " param per sec" << endl;
	}
	system("pause");

	return 0;
}