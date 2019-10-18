#include <iostream>
#include <string>

#include <QtCore/qdatetime.h>
#include <QtCore/qvariant.h>
#include <QtCore/qrandom.h>

#include <QtSql/qsql.h>
#include <QtSql/qsqlquery.h>
#include <QtSql/qsqlerror.h>

using namespace std;

class DatapointValue
{
public:
	QString Address;
	QDateTime Timestamp;
	float Value;
	float Status;

	DatapointValue(const QString& address, const QDateTime& timestamp, float value, float status)
		:Address(address), Timestamp(timestamp), Value(value), Status(status)
	{
	}
};

class DatapointDescription
{
public:
	QString Address;
	QString ArchiveName;
	QString ValueColumnName;
	QString StatusColumnName;

	DatapointDescription() {}

	DatapointDescription(const QString& address, const QString& archiveName, const QString& valueColumnName, const QString& statusColumnName)
		:Address(address), ArchiveName(archiveName), ValueColumnName(valueColumnName), StatusColumnName(statusColumnName)
	{
	}

	QString GetArchiveTableName(const QDateTime& timestamp)
	{
		return "values";
	}
};

class DatapointValuesGrouppingStrategyBase
{
public:
	virtual vector<vector<DatapointValue>> Group(const vector<DatapointValue>& args) = 0;
};

class DatapointValuesGrouppingStrategyByValuesNumber : public DatapointValuesGrouppingStrategyBase
{
private:
	int _packageSize;
public:
	DatapointValuesGrouppingStrategyByValuesNumber(int packageSize) : _packageSize(packageSize) {
	}

	vector<vector<DatapointValue>> Group(const vector<DatapointValue>& args)
	{
		vector<vector<DatapointValue>> result;
		vector<DatapointValue> currentPackage;

		for (size_t i = 0; i < args.size(); i++)
		{
			currentPackage.push_back(args[i]);

			if ((i + 1) % _packageSize == 0)
			{
				result.push_back(currentPackage);
				currentPackage.clear();
			}
		}

		if (currentPackage.size() > 0)
		{
			result.push_back(currentPackage);
		}

		return result;
	};

};

class DbWriter
{
private:
	map<QString, DatapointDescription> _datapointDescriptionMap;
	shared_ptr<DatapointValuesGrouppingStrategyBase> _dpGrouppingStratagy;
	QSqlDatabase _db = QSqlDatabase::addDatabase("QPSQL");

	bool TryConnectToDb()
	{
		_db.setHostName("localhost");
		_db.setDatabaseName("winccoa");
		_db.setUserName("postgres");
		_db.setPassword("postgres");
		return _db.open();
	}

	void PopulateTimestamps(const std::vector<DatapointValue>& datapointValues, QSqlQuery& insertQuery)
	{
		QString sql;
		vector<QDateTime> dts;
		for (size_t i = 0; i < datapointValues.size(); i++)
		{
			if (find(dts.begin(), dts.end(), datapointValues[i].Timestamp) == dts.end())
			{
				dts.push_back(datapointValues[i].Timestamp);
			}
		}
		_db.transaction();
		for (size_t i = 0; i < dts.size(); i++)
		{
			sql = "INSERT INTO \"values\" (\"Timestamp\") VALUES (:ts) ON CONFLICT DO NOTHING";
			insertQuery.bindValue(":ts", dts[i]);
			auto insertResult = insertQuery.exec();
			if (!insertResult)
			{
				auto lastError = insertQuery.lastError().text();
			}
		}
		_db.commit();
	}

public:
	DbWriter(const vector<DatapointDescription>& datapointDescription,
		const shared_ptr<DatapointValuesGrouppingStrategyBase>& dpGrouppingStratagy)
	{
		for (size_t i = 0; i < datapointDescription.size(); i++)
		{
			_datapointDescriptionMap.insert(
				make_pair(datapointDescription[i].Address,
					datapointDescription[i]));
		}
		_dpGrouppingStratagy = dpGrouppingStratagy;
	};

	void Write(const vector<DatapointValue>& datapointValues)
	{
		if (!TryConnectToDb())
		{
			//todo
			throw - 1;
		}

		// group values by some criterias to write them a few threads
		// the criterias can be related to the archive name or timestamps or even values count in one package
		auto groupedValues = _dpGrouppingStratagy->Group(datapointValues);

		// perform each group in a separate thread
		// ...
		int idx = 0;
		int total = datapointValues.size();
		QString sql;
		QSqlQuery insertQuery;

		//		PopulateTimestamps(datapointValues, insertQuery);

		for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
		{
			_db.transaction();
			for (size_t dpIdx = 0; dpIdx < groupedValues[groupIdx].size(); dpIdx++)
			{
				auto dpValue = groupedValues[groupIdx][dpIdx];
				auto dpDesc = _datapointDescriptionMap[dpValue.Address];

				sql = "INSERT INTO \"" + dpDesc.ArchiveName + "\" (\"Timestamp\", \"" + dpDesc.ValueColumnName + "\", \"" + dpDesc.StatusColumnName + "\") " +
					+" VALUES (:ts, :value, :status) ON CONFLICT (\"Timestamp\") DO UPDATE SET \"" + dpDesc.ValueColumnName + "\" = :value, \"" + dpDesc.StatusColumnName + "\" = :status";

				//sql = "UPDATE \"" + dpDesc.ArchiveName + "\" SET \"" + dpDesc.ValueColumnName + "\" = :value, " +
				//	"\"" + dpDesc.StatusColumnName + "\" = :status " +
				//	"WHERE \"Timestamp\" = :ts";

				insertQuery.prepare(sql);
				insertQuery.bindValue(":ts", dpValue.Timestamp);
				insertQuery.bindValue(":value", dpValue.Value);
				insertQuery.bindValue(":status", dpValue.Status);

				auto insertResult = insertQuery.exec();
				if (!insertResult)
				{
					auto lastError = insertQuery.lastError().text();
				}

				idx++;
			}
			_db.commit();
			cout << idx << " / " << total << endl;
		}
	};
};

int main(void)
{
	int demoDpCounts = 100;
	int demoValuesPerOneDp = 100;
	DatapointValuesGrouppingStrategyByValuesNumber dpValuesGroupingStratagy(1000);

	// preparing datapoints description for testing
	vector<DatapointDescription> dps;
	for (size_t i = 1; i <= demoDpCounts; i++)
	{
		dps.push_back(DatapointDescription(
			"System1.temperature_" + QString::number(i).rightJustified(2, '0'),
			"values",
			"P_" + QString::number(i).rightJustified(2, '0'),
			"S_" + QString::number(i).rightJustified(2, '0')));
	}

	// init a new DbWriter instsnce
	DbWriter dbWriter(dps, make_shared<DatapointValuesGrouppingStrategyByValuesNumber>(dpValuesGroupingStratagy));

	while (true)
	{

		// generate demo dp values - 10 000 values for each datapoint
		vector<DatapointValue> values;
		auto now = QDateTime::currentDateTime();
		for (size_t dpIdx = 0; dpIdx < dps.size(); dpIdx++)
		{
			auto ts = now;
			for (size_t valueIdx = 0; valueIdx < demoValuesPerOneDp; valueIdx++)
			{
				values.push_back(DatapointValue(dps[dpIdx].Address, ts, QRandomGenerator::global()->generate(), QRandomGenerator::global()->generate()));
				ts = ts.addMSecs(1);
			}
		}

		auto startTime = QDateTime::currentDateTime();
		dbWriter.Write(values);
		auto durationMs = now.msecsTo(QDateTime::currentDateTime());
		cout << durationMs << " ms" << endl;
		cout << (1000 * demoDpCounts * demoValuesPerOneDp) / durationMs << " param per sec" << endl;
	}
	system("pause");

	return 0;
}