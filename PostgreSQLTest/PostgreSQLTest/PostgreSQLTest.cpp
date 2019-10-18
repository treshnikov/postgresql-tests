#include <iostream>
#include <string>

#include <QtCore/qdatetime.h>
#include <QtCore/qvariant.h>
#include <QtCore/qrandom.h>

#include <QtSql/qsql.h>
#include <QtSql/qsqlquery.h>
#include <QtSql/qsqlerror.h>

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
	virtual vector<vector<DpValue>> Group(const vector<DpValue>& args) = 0;
};

class DpValuesGroupingStrategyByValuesNumber : public DpValuesGroupingStrategyBase
{
private:
	int _packageSize;
public:
	DpValuesGroupingStrategyByValuesNumber(int packageSize) : _packageSize(packageSize) {
	}

	vector<vector<DpValue>> Group(const vector<DpValue>& args)
	{
		vector<vector<DpValue>> result;
		vector<DpValue> currentPackage;

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
	map<QString, DpDescription> _datapointDescriptionMap;
	shared_ptr<DpValuesGroupingStrategyBase> _dpGrouppingStratagy;
	QSqlDatabase _db = QSqlDatabase::addDatabase("QPSQL");

	bool TryConnectToDb()
	{
		_db.setHostName("localhost");
		_db.setDatabaseName("winccoa");
		_db.setUserName("postgres");
		_db.setPassword("postgres");
		return _db.open();
	}

public:
	DbWriter(const vector<DpDescription>& dpDescription,
		const shared_ptr<DpValuesGroupingStrategyBase>& dpGrouppingStratagy)
	{
		for (size_t i = 0; i < dpDescription.size(); i++)
		{
			_datapointDescriptionMap.insert(
				make_pair(dpDescription[i].Address,
					dpDescription[i]));
		}
		_dpGrouppingStratagy = dpGrouppingStratagy;
	};

	void Write(const vector<DpValue>& dpValues)
	{
		if (!TryConnectToDb())
		{
			//todo
			throw - 1;
		}

		auto groupedValues = _dpGrouppingStratagy->Group(dpValues);

		// todo perform each group in a separate thread
		int idx = 0;
		int total = dpValues.size();
		QString sql;
		QSqlQuery insertQuery;

		// PopulateTimestamps(datapointValues, insertQuery);

		for (size_t groupIdx = 0; groupIdx < groupedValues.size(); groupIdx++)
		{
			_db.transaction();
			for (size_t dpIdx = 0; dpIdx < groupedValues[groupIdx].size(); dpIdx++)
			{
				auto dpValue = groupedValues[groupIdx][dpIdx];
				auto dpDesc = _datapointDescriptionMap[dpValue.Address];

				sql = "INSERT INTO \"" + dpDesc.ArchiveName + "\" (\"timestamp\", \"p_01\", \"s_01\") " +
					+" VALUES (:ts, :value, :status)";
				//ON CONFLICT (\"timestamp\") DO UPDATE SET \"p_01\" = :value, \"s_01\" = :status

				insertQuery.prepare(sql);
				insertQuery.bindValue(":ts", dpValue.Timestamp);
				insertQuery.bindValue(":value", dpValue.Value);
				insertQuery.bindValue(":status", dpValue.Status);

				auto insertResult = insertQuery.exec();
				if (!insertResult)
				{
					auto lastError = insertQuery.lastError().text();
					//cout << lastError.toStdString();
				}

				idx++;
			}
			_db.commit();
			//cout << idx << " / " << total << endl;
		}
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
	int demoDpCounts = 100;
	int demoValuesPerOneDp = 100;
	
	DpValuesGroupingStrategyByValuesNumber dpValuesGroupingStratagy(demoValuesPerOneDp);
	
	auto dpsDescriptions = PopulateDemoDpDescriptions(demoDpCounts);
	DbWriter dbWriter(dpsDescriptions, 
					  make_shared<DpValuesGroupingStrategyByValuesNumber>(dpValuesGroupingStratagy));

	while (true)
	{
		// generate demo dp values
		vector<DpValue> values;
		auto now = QDateTime::currentDateTime();
		for (size_t dpIdx = 0; dpIdx < dpsDescriptions.size(); dpIdx++)
		{
			auto ts = now;
			for (size_t valueIdx = 0; valueIdx < demoValuesPerOneDp; valueIdx++)
			{
				values.push_back(DpValue(dpsDescriptions[dpIdx].Address, ts, 
					QRandomGenerator::global()->generate(), 
					QRandomGenerator::global()->generate()));
				ts = ts.addMSecs(1);
			}
		}

		auto startTime = QDateTime::currentDateTime();
		dbWriter.Write(values);
		auto durationMs = now.msecsTo(QDateTime::currentDateTime());
		cout << (1000 * demoDpCounts * demoValuesPerOneDp) / durationMs << " param per sec" << endl;
	}
	system("pause");

	return 0;
}