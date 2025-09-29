#pragma once
#include <QAbstractTableModel>
#include <QStringList>
#include <memory>
#include <vector>

class CsvModel : public QAbstractTableModel {
  Q_OBJECT
 public:
  // dataVec is shared_ptr to avoid copy; we'll keep it const
  CsvModel(const QStringList &headers, size_t rows, size_t cols,
           std::shared_ptr<std::vector<float>> dataVec,
           QObject *parent = nullptr);

  int rowCount(const QModelIndex &parent = QModelIndex()) const override;
  int columnCount(const QModelIndex &parent = QModelIndex()) const override;
  QVariant data(const QModelIndex &index,
                int role = Qt::DisplayRole) const override;
  QVariant headerData(int section, Qt::Orientation orientation,
                      int role = Qt::DisplayRole) const override;

 private:
  QStringList m_headers;
  size_t m_rows;
  size_t m_cols;
  std::shared_ptr<std::vector<float>> m_data;
};
