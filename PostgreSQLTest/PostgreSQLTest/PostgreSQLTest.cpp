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
		// todo define the logic that returns the name of the table depending on the given timestamp
		return ArchiveName;
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
				if (i != 0 && i % _packageSize == 0)
				{
					result.push_back(currentPackage);
					currentPackage.Values.clear();
				}

				currentPackage.Values.push_back(arcTableNameToDpValues[tableName][i]);
			}

			if (currentPackage.Values.size() != 0)
			{
				result.push_back(currentPackage);
			}
		}

		return result;
	};
};

class DbWriter
{
private:
	int _threadsCount;
	map<QString, DpDescription> _datapointDescriptionMap;
	shared_ptr<DpValuesGroupingStrategyBase> _dpGrouppingStratagy;
	vector<QSqlDatabase> dbPool;

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
			}

			dbPool.push_back(db);
		}
	}

public:
	DbWriter(const vector<DpDescription>& dpDescription,
		const shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy,
		int threadsCount)
		: _dpGrouppingStratagy(dpGrouppingStratagy), _threadsCount(threadsCount)
	{
		for (size_t i = 0; i < dpDescription.size(); i++)
		{
			_datapointDescriptionMap.insert(
				make_pair(dpDescription[i].Address,
					dpDescription[i]));
		}

		PopulateDbConnections();
	};

	void WritePackage(DpValuesPackage& group, QSqlDatabase& db)
	{
		QString sql;
		QSqlQuery query(db);

		QVariantList tss;
		QVariantList values;
		QVariantList statuses;

		//db.transaction();
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
			// todo
			auto lastError = query.lastError().text();
		}

		//db.commit();
	}

	void Write(const vector<DpValue>& dpValues)
	{
		vector<thread> threadPool;
		auto groupedValues = _dpGrouppingStratagy->Group(dpValues);

		for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
		{
			DpValuesPackage& group = groupedValues[groupIdx];
			
			int poolIdx = groupIdx % _threadsCount;
			QSqlDatabase& db = dbPool[poolIdx];
			
			threadPool.push_back(thread(&DbWriter::WritePackage, this, ref(group), ref(db)));

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


vector<DpValue>  GenerateDemoDpValues(vector<DpDescription>& dpsDescriptions, QDateTime& modelTime, int dpValuesForOneDp)
{
	vector<DpValue> values;
	for (size_t dpIdx = 0; dpIdx < dpsDescriptions.size(); dpIdx++)
	{
		auto ts = modelTime;
		for (size_t valueIdx = 0; valueIdx < dpValuesForOneDp; valueIdx++)
		{
			values.push_back(
				DpValue(dpsDescriptions[dpIdx].Address,
					ts,
					QRandomGenerator::global()->generate(),
					QRandomGenerator::global()->generate()));
			ts = ts.addMSecs(1);
		}
	}
	return values;
}

int main(void)
{
	// the number of datapoints to emulate, the values of each datapoint are stored in a separate table - z_arcXXX : [Timestamp, P_01, S_01]
	int dpCounts = 10;

	// the number of data point values ​​to generate per iteration for each datapoint 
	int dpValuesForOneDp = 2000;

	// package size for bulk insert
	int dpValuesInOneDbTransaction = 2000;
	
	// count of threads for writing datapoint values into DB
	int dbWriteThreadsCount = 6;

	auto dpsDescriptions = PopulateDemoDpDescriptions(dpCounts);
	DpValuesGroupingStrategyByTablesAndPackages dpValuesGroupingStratagy(dpValuesInOneDbTransaction, dpsDescriptions);
	DbWriter dbWriter(
		dpsDescriptions, 
		make_shared<DpValuesGroupingStrategyByTablesAndPackages>(dpValuesGroupingStratagy),
		dbWriteThreadsCount);

	auto modelTime = QDateTime::currentDateTime();
	while (true)
	{
		auto values = GenerateDemoDpValues(dpsDescriptions, modelTime, dpValuesForOneDp);
		modelTime = modelTime.addMSecs(dpValuesForOneDp + 1);

		auto startTime = QDateTime::currentDateTime();
		dbWriter.Write(values);
		cout << 1000 * dpCounts * dpValuesForOneDp / startTime.msecsTo(QDateTime::currentDateTime()) << " param per sec" << endl;
	}
	system("pause");

	return 0;
}