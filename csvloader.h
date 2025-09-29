#pragma once
#include <QObject>
#include <QString>
#include <QThread>
#include <memory>
#include <vector>

struct ParsedResult {
  QStringList headers;
  size_t rows;
  size_t cols;
  // shared_ptr to heap vector to avoid expensive copies in signal/slot
  std::shared_ptr<std::vector<float>> data;
};

class CsvLoader : public QObject {
  Q_OBJECT
 public:
  explicit CsvLoader(const QString &filePath, QObject *parent = nullptr);
  ~CsvLoader();

  // start parsing in a worker thread
  void start();

 signals:
  void progress(int percent);
  void finished(std::shared_ptr<ParsedResult> result);
  void failed(const QString &err);

 private:
  QString m_filePath;
  QThread *m_thread;

 public slots:
  void process();  // actual work
};
