#pragma once
#include <QMainWindow>
#include <memory>

class QPushButton;
class QProgressBar;
class QTableView;
class CsvModel;
struct ParsedResult;

class MainWindow : public QMainWindow {
  Q_OBJECT
 public:
  MainWindow(QWidget *parent = nullptr);
  ~MainWindow();

 private slots:
  void onSelectFile();
  void onProgress(int percent);
  void onLoadFailed(const QString &err);
  void onLoadFinished(std::shared_ptr<ParsedResult> result);

 private:
  QPushButton *m_btnSelect;
  QProgressBar *m_progress;
  QTableView *m_tableView;
  CsvModel *m_model;
};
